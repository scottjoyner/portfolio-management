<<<<<<< HEAD
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
=======
# Migration Guide — Trading System Database Schema

## Overview

This document describes the database schema migrations for the portfolio management trading system.

**Status**: ✅ Baseline migration committed and validated

## Baseline Revision (`0001_initial.py`)

The initial schema was generated from SQLAlchemy models and covers 15 core tables:

### Core Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `portfolios` | Portfolio state and metrics | id, name, objective, nav, available_capital, locked_capital, realized_pnl, unrealized_pnl, liquidity_score, capital_efficiency, created_at, updated_at |
| `portfolio_sleeves` | Sleeve allocations per portfolio | id, portfolio_id (FK), name, weight |
| `strategy_configs` | Strategy metadata and flags | strategy_id, strategy_type, status, paper_mode, live_supported, replay_supported, backtest_supported, risk_mode_hint, capital_bucket, enabled, config_json, created_at, updated_at |
| `strategy_runs` | Strategy execution runs | task_id, strategy_id (FK), status, mode, queued_at, started_at, completed_at, error_message |
| `orders` | Order lifecycle tracking | id, order_id (unique), preview_id, strategy_id (FK), portfolio_id (FK), sleeve_id, product_id, side, size, remaining_size, price, notional, order_type, status, maker_taker_expectation, queue_age_s, risk_mode, reduce_only, created_at, updated_at |
| `strategy_allocations` | Capital allocation per strategy | id, portfolio_id (FK), strategy_id, weight |
| `fills` | Fill tracking from orders | id, fill_id (unique), order_id (FK), product_id, side, price, fee_basis_points, quantity_executed, commission_amount |
| `audit_logs` | Audit trail for operations | id, portfolio_id (FK), actor_id, event_type, event_data, created_at |
| `alerts` | Alert notifications | id, portfolio_id (FK), strategy_id, severity, message, resolved, created_at |
| `incidents` | Incident tracking | id, severity, status, description, affected_entity, remediation_notes, created_at, resolved_at |
| `market_data_snapshots` | Market data caching | id, product_id, quote_type, bid_price, ask_price, last_price, volume_24h, high_24h, low_24h, open_24h, timestamp |
| `market_book_snapshots` | Order book L2 snapshots | id, product_id, quote_type, sequence, bids (JSON), asks (JSON), timestamp |
| `exchange_state` | Coinbase API state tracking | id, account_id (FK), exchange_state (JSON), sync_timestamp, trust_score |
| `market_data_feed_health` | Feed health monitoring | id, product_id, last_heartbeat, latency_ms, error_count, status |
| `order_book_levels` | Aggregated order book levels | id, product_id, price_level, side, quantity, total_quantity |

## Upgrade Procedure

### From Fresh Database

```bash
cd trading_system

# Create fresh database
psql -f <(cat << 'EOF'
DROP DATABASE IF EXISTS trading_system_test;
CREATE DATABASE trading_system_test;
EOF
)

# Run migrations
alembic upgrade head

# Verify current head
alembic current
```

### From Existing Database (Without Migrations)

If you have an existing database without Alembic metadata:

```bash
cd trading_system

# Option 1: Generate migration from existing schema
alembic revision --autogenerate -m "migration_from_scratch"

# Review generated migration, then apply
alembic upgrade head

# Or use create_all as last resort (not recommended for production)
python3 -c "from storage.postgres.models import Base; Base.metadata.create_all(engine)"
```

## Rollback Procedure

To rollback to previous revision:

```bash
cd trading_system
alembic downgrade -1  # Rollback one revision
alembic downgrade 0  # Rollback all revisions (empty database)
```

## CI Migration Checks

Add to `.github/workflows/ci.yml`:

```yaml
- name: Run migrations on fresh DB
  run: |
    psql -c "DROP DATABASE IF EXISTS trading_system_ci; CREATE DATABASE trading_system_ci;"
    alembic -c trading_system/alembic.ini upgrade head -p trading_system_ci
    
- name: Verify migration head
  run: alembic current --env-file=trading_system/.env
  
- name: Migration smoke test
  run: pytest tests/migrations/test_smoke.py -q
```

## Adding New Migrations

When adding new tables or modifying schema:

```bash
cd trading_system
alembic revision --autogenerate -m "description"

# Review generated migration in alembic/versions/YYYY_*.py
# Manually verify column types, constraints, and foreign keys match SQLAlchemy models
# If models changed but not reflected, edit migration manually or use autogenerate again
alembic upgrade head  # Apply migration locally before committing
```

## Production Deployment Checklist

- [ ] Review migration script for idempotency (can run multiple times)
- [ ] Test rollback procedure works as expected
- [ ] Document new columns and their purposes
- [ ] Add migration smoke tests
- [ ] Commit migration to repository
- [ ] Update `MIGRATION_GUIDE.md` with new table/column documentation

## Environment Variables

Set in production `.env`:

```bash
DATABASE_URL=postgresql://user:password@host:5432/trading_system
ALEMBIC_MIGRATION_ENVIRONMENT=production
```

## Monitoring & Alerts

Add to monitoring dashboard:

- `alembic current` on startup (verify migration is at head)
- Alert if `alembic current` != expected head
- Log migration duration on first apply after deploy
>>>>>>> b5e23b51 (Added falcon updates)
