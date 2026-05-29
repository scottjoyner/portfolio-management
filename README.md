# Prediction-Market Arbitrage / Portfolio Management System

## Production readiness status

**Not ready for production or live trading.**

This repository is currently a safety-first mock/paper scaffold plus an expanding portfolio-management planning baseline. Live trading must remain disabled until the UI, API, persistence, strategy lifecycle, backtesting, approvals, reconciliation, observability, and deployment controls are complete.

See:

- `TODO.md` for the prioritized P0/P1/P2/P3 backlog.
- `docs/PRODUCTION_READINESS_REVIEW_2026_05_29.md` for the deployment review.
- `docs/ARCHITECTURE.md` for the target operator workflow and service-boundary decisions.
- `docs/P0_UI_API_IMPLEMENTATION.md` for the first operator UI/API implementation slice.
- `docs/P0_DURABLE_STATE_AND_MIGRATIONS.md` for durable local state and migration groundwork.
- `docs/P0_POSTGRES_STORE_AND_MIGRATIONS.md` for Postgres store and migration runner setup.

## Safety warning

Paper trading is the default. Live trading is disabled by default and requires explicit configuration plus runtime confirmation. Do not connect real-money credentials or execute live orders until the release gates are implemented and certified.

## Quickstart: mock/paper scaffold

```bash
pnpm install
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm api
```

Then open:

```text
http://localhost:3000/
```

CLI smoke commands:

```bash
pnpm cli doctor --mode mock
pnpm cli discover --mode mock
pnpm cli match:propose --mode mock
pnpm cli arb:scan --mode mock
pnpm cli arb:paper --mode mock
```

## Local runtime state

The operator API uses local durable state by default:

```text
data/operator-state.json
```

Override with:

```bash
OPERATOR_STATE_PATH=/path/to/operator-state.json pnpm api
```

Disable file persistence with:

```bash
OPERATOR_STATE_DISABLED=true pnpm api
```

## Postgres migration preview

Preview migration SQL:

```bash
pnpm migrations:dry-run
```

Apply migrations with the PostgreSQL client installed:

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb pnpm migrations:up
```

Run the API in Postgres mode after migrations and runtime dependencies are available:

```bash
OPERATOR_STORE=postgres DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb pnpm api
```

## Current capability snapshot

| Capability | Current status |
|---|---|
| Mock CLI | Partial deterministic demo responses. |
| Node API | Operator API skeleton with health, readiness, strategy, backtest, approval, audit, metrics, and kill-switch routes. |
| Web UI | Static operator console for mock/paper workflows. |
| Durable local state | File-backed runtime state for local/dev continuity. |
| SQL migrations | Baseline schema, validation, dry-run, and psql runner added. |
| Postgres repository | Adapter scaffold and fake-client tests added; live runtime requires `pg` dependency and integration testing. |
| Strategy lifecycle | Basic create/list flow only; validation, versioning, and lifecycle controls remain incomplete. |
| Backtesting | Deterministic demo simulation only; not a realistic certification engine. |
| Plaid/account data | Mock/scaffold responses only. |
| Live trading | Blocked by design and not certified. |

## Live trading checklist

Live trading remains blocked until all of these are true:

1. `PAPER_TRADING=false`
2. `LIVE_TRADING=true`
3. `REQUIRE_MANUAL_APPROVAL=true`
4. Market pair or strategy status is approved.
5. Compliance gate passed.
6. Risk checks approved.
7. Runtime confirmation supplied.
8. Database-backed reconciliation is healthy.
9. Kill switch is tested from UI/API/worker paths.
10. CI and staging certification pass without waived failures.

## Known risks

- Cross-venue execution is non-atomic.
- Partial fills can create temporary unhedged exposure.
- Similar wording markets may still resolve differently.
- Strategy and backtest modules are not yet fully wired into production-grade lifecycle workflows.
- Account, Plaid, broker, exchange, and onchain integrations are not production-certified.
