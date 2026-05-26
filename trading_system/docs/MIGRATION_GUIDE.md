# Trading System — Migration Guide

This guide documents how to move the `trading_system` service from the current scaffold/demo state into a reproducible database-backed runtime. It is intentionally conservative: paper trading is the default, live trading remains gated, and migrations should be validated outside production before any capital-bearing workflow is enabled.

## Current migration state

The current repository has the important building blocks in place:

- `alembic/env.py` exists and wires Alembic to `storage.postgres.models.Base.metadata`.
- `DATABASE_URL` is read from the environment at migration/runtime startup.
- Core SQLAlchemy models exist for portfolios, sleeves, strategy allocations/configs/runs, orders, fills, capital buckets, approvals, audit events, alerts, incidents, exchange state, and market data feed health.
- The API lifespan initializes the database and seeds default portfolios when `DATABASE_URL` is configured.
- Docker and deployment assets exist for local/dev and production-style container runs.

The remaining migration risk is making sure the committed Alembic revision history exactly matches the models and that deploy operators have a repeatable upgrade/rollback process.

## Migration safety policy

1. **Paper first**: keep `TRADING_MODE=PAPER` during migration validation.
2. **Live disabled by default**: keep `LIVE_TRADING_ENABLED=false` until schema, reconciliation, and smoke tests pass.
3. **Approvals required**: keep `REQUIRE_APPROVALS=true` for any workflow that can affect capital.
4. **No schema changes during open live execution**: pause workers, drain queues, and cancel/settle open orders before applying production schema migrations.
5. **Back up first**: every production migration starts with a logical database backup and a rollback decision point.

## Environment matrix

| Environment | Purpose | Database | Trading mode | Live gate | Migration rule |
|---|---|---|---|---|---|
| Local dev | Fast iteration and model edits | Local Postgres from compose | `PAPER` | `false` | Autogenerate allowed, manual review required |
| CI | Regression gate | Ephemeral service DB or SQLite-compatible subset if supported | `PAPER` | `false` | `alembic upgrade head` must run cleanly |
| Staging | Full deployment rehearsal | Production-like Postgres | `PAPER` or `SHADOW` | `false` | Apply exact committed revisions only |
| Production | Capital-bearing service | Managed Postgres | `PAPER`, `CANARY`, or approved live mode | explicit only | Apply exact committed revisions only after backup and smoke checks |

## Baseline workflow

Run from the repository root unless noted.

```bash
cd trading_system
cp .env.example .env
# Edit DATABASE_URL and keep TRADING_MODE=PAPER / LIVE_TRADING_ENABLED=false.
```

Install dependencies:

```bash
python -m pip install --upgrade pip
pip install -e .[dev]
```

Start local dependencies:

```bash
docker compose up -d postgres redis
```

Validate Alembic wiring:

```bash
alembic current
alembic heads
```

If no schema revision exists yet, create the reviewed baseline migration:

```bash
alembic revision --autogenerate -m "baseline_core_schema"
```

Before committing the generated file, inspect it carefully for:

- all expected tables from `storage/postgres/models.py`;
- primary keys, foreign keys, indexes, nullable settings, and numeric precision;
- no accidental drops of existing tables;
- no environment-specific data baked into the revision.

Apply the migration locally:

```bash
alembic upgrade head
```

Then run the local quality gate:

```bash
make ci
```

## Existing database upgrade workflow

Use this for staging or production-like databases that already contain data.

1. Record the current revision:
   ```bash
   alembic current
   alembic heads
   ```
2. Back up the database:
   ```bash
   pg_dump "$DATABASE_URL" > backups/trading_$(date +%Y%m%d_%H%M%S).sql
   ```
3. Disable capital-bearing runtime actions:
   ```bash
   export TRADING_MODE=PAPER
   export LIVE_TRADING_ENABLED=false
   export REQUIRE_APPROVALS=true
   ```
4. Stop workers and leave the API in maintenance/degraded mode if needed.
5. Apply migrations:
   ```bash
   alembic upgrade head
   ```
6. Run smoke checks:
   ```bash
   python -m apps.worker.main --dry-run  # when the worker supports dry-run mode
   pytest -q tests/integration/test_ops_api.py
   curl -fsS http://localhost:8000/health
   curl -fsS http://localhost:8000/ready
   ```
7. Re-enable only the intended runtime mode after reconciliation and operator approval.

## Rollback workflow

Alembic downgrade support must be verified per revision before production use. A safe rollback strategy is:

1. Stop workers and capital-bearing execution.
2. Decide whether data rollback is required or whether a forward-fix migration is safer.
3. If downgrade is supported and tested:
   ```bash
   alembic downgrade -1
   ```
4. If data corruption or destructive migration occurred, restore from backup into a clean database and point the service back to the restored DB.
5. Re-run smoke tests and reconciliation before resuming any live mode.

## Data migration conventions

- Keep schema migrations and data backfills separate when possible.
- Backfills should be idempotent and restartable.
- Large backfills should run outside request handling and emit audit events.
- Never backfill secrets, raw API keys, private keys, or wallet material into relational tables.
- Use explicit revision names that describe the domain change, for example `add_order_lifecycle_fields` instead of `update_models`.

## Validation checklist

Before a migration is merged:

- [ ] `alembic current` and `alembic heads` are understood.
- [ ] Generated revision is reviewed manually.
- [ ] `alembic upgrade head` succeeds on a fresh DB.
- [ ] Upgrade succeeds on a DB seeded with representative portfolios, strategies, orders, fills, approvals, alerts, and audit events.
- [ ] Downgrade or forward-fix plan is documented.
- [ ] `make ci` passes.
- [ ] `/health`, `/ready`, and `/metrics` are checked after startup.
- [ ] `TRADING_MODE`, `LIVE_TRADING_ENABLED`, and `REQUIRE_APPROVALS` are verified before runtime resume.

## Near-term migration TODOs

- Commit a reviewed baseline Alembic revision if `alembic/versions/` only contains package markers.
- Add CI service containers for Postgres and Redis, then run `alembic upgrade head` in CI.
- Add a seed-data smoke test that exercises portfolio, strategy, order, fill, approval, audit, alert, incident, exchange state, and market data feed models.
- Add a documented production restore rehearsal using a local Postgres container.
- Add migration-specific runbooks under `docs/runbooks/` once staging/production targets are chosen.
