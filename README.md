# Prediction-Market Arbitrage / Portfolio Management System

## Production readiness status

**Usable for P0/P1 mock/paper operator evaluation. Not ready for production or live trading.**

The current implementation provides a browser operator console, durable local state, Postgres migration scaffolding, strategy templates, deterministic backtest reports, approval decisions, and paper-execution lifecycle controls. Live trading remains blocked until P2 hardening, connector certification, reconciliation, observability, and deployment controls are complete.

See:

- `docs/P0_P1_ACCEPTANCE_CHECKLIST.md` for the P0/P1 completion map.
- `docs/API_CONTRACT_P0_P1.md` for the current API contract.
- `docs/OPERATOR_RUNBOOK_P0_P1.md` for local operator usage.
- `TODO.md` for the broader P0/P1/P2/P3 backlog.
- `docs/PRODUCTION_READINESS_REVIEW_2026_05_29.md` for the deployment review.
- `docs/ARCHITECTURE.md` for the target operator workflow and service-boundary decisions.
- `docs/P0_UI_API_IMPLEMENTATION.md` for the first operator UI/API slice.
- `docs/P0_DURABLE_STATE_AND_MIGRATIONS.md` for durable local state.
- `docs/P0_POSTGRES_STORE_AND_MIGRATIONS.md` for Postgres store and migration setup.

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

## Optional operator auth

Local mock mode does not require auth by default. To require a bearer token:

```bash
OPERATOR_AUTH_REQUIRED=true \
OPERATOR_AUTH_TOKEN=dev-secret \
pnpm api
```

Then use:

```http
Authorization: Bearer dev-secret
```

`MODE=live` also requires auth, although live execution remains blocked.

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
| Node API | P0/P1 operator API with auth guard, request IDs, health, readiness, account, instrument, strategy, backtest, approval, paper, risk, audit, and metrics routes. |
| Web UI | Static operator console plus dynamic P1 panels for accounts, instruments, templates, approvals, and paper execution. |
| Durable local state | File-backed runtime state for local/dev continuity. |
| SQL migrations | Core and P1 product-layer schemas, validation, dry-run, and psql runner added. |
| Postgres repository | P1 adapter scaffold with fake-client tests; runtime requires `pg` dependency and integration testing. |
| Strategy lifecycle | Template creation, validation, cloning/versioning, status updates, backtest evidence, and approval path. |
| Backtesting | Deterministic strategy-version report scaffold with fees/slippage assumptions, metrics, equity curve, and trade log. |
| Plaid/account data | Paper/sandbox account ledger scaffold only. |
| Paper execution | Approved-strategy paper session lifecycle with stop and kill-switch controls. |
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
- Deterministic backtesting is not production-grade market replay.
- Paper execution is a lifecycle scaffold, not a full signal/order/fill engine yet.
- Account, Plaid, broker, exchange, and onchain integrations are not production-certified.
