# P0/P1 Acceptance Checklist

This checklist tracks the long-running implementation branch for P0/P1 completion. Live trading remains blocked throughout this phase.

## P0 — Blockers

### P0.1 — Operator UI

Status: **implemented for mock/paper workflows; production UI hardening remains P2**

Implemented:

- Static operator dashboard served by the API.
- Dynamic P1 panels for accounts, instruments, templates, and paper execution.
- Strategy, backtest, approval, risk, and audit views.
- Approval buttons and paper start/stop controls.
- Kill-switch controls.

Validation:

- `scripts/build-web.mjs` verifies required UI assets and P1 endpoint references.

### P0.2 — Real API routes

Status: **implemented for P0/P1 mock/paper product surface**

Implemented:

- Health, readiness, metrics, and summary routes.
- Accounts, instruments, strategy templates.
- Strategy lifecycle routes.
- Backtest run/report routes.
- Approval request/decision routes.
- Paper execution lifecycle routes.
- Positions, audit, kill switch.
- Auth guard and request IDs for operator routes.
- API contract documentation.

Validation:

- `tests/operator-api.test.mjs`
- `tests/operator-p1-api.test.mjs`
- `tests/operator-auth.test.mjs`

### P0.3 — Migrations and repositories

Status: **implemented for local/file and Postgres scaffold; row-level Postgres hardening remains P2**

Implemented:

- File-backed durable local state.
- Postgres store scaffold.
- P1 Postgres product-layer persistence via versioned operator flags.
- Migration runner.
- Migration validator.
- Migration 001: core operator tables.
- Migration 002: accounts, instruments, templates, and paper executions.

Validation:

- `tests/operator-store.test.mjs`
- `tests/postgres-operator-store.test.mjs`
- `tests/postgres-p1-store.test.mjs`
- `pnpm migrations:validate`
- `pnpm migrations:dry-run`

### P0.4 — Strategy lifecycle management

Status: **implemented for template creation, validation, cloning, status, approval path**

Implemented:

- Strategy templates.
- Parameter schema validation.
- Strategy creation from templates.
- Free-form strategy creation.
- Strategy version cloning.
- Lifecycle status update.
- Backtest evidence and approval path.

Validation:

- `tests/operator-p1-api.test.mjs`

### P0.5 — Backtesting

Status: **implemented as deterministic certification scaffold; realistic market replay remains P2**

Implemented:

- Strategy-version-aware deterministic backtest route.
- Fee/slippage assumptions.
- Metrics summary.
- Equity curve.
- Trade log.
- Report artifact metadata.
- UI cards and report endpoint.

Remaining beyond P1:

- Real historical market data replay.
- Latency/partial-fill modeling.
- Market impact model.
- Walk-forward validation.

### P0.6 — Keep real-money execution disabled

Status: **implemented**

Implemented:

- `/api/execution/live/*` remains blocked.
- Live mode remains uncertified.
- Operator readiness remains fail-closed.
- Kill switch stops paper sessions.
- Auth required in live mode.

Validation:

- `tests/operator-api.test.mjs`
- `tests/operator-auth.test.mjs`

### P0.7 — Clean deployment hazards

Status: **implemented for generated/runtime state; continued audit needed before production**

Implemented:

- Runtime data ignored.
- State stored under `data/operator-state.json` by default.
- Migrations/scripts are source controlled.

### P0.8 — Strict CI/build gates

Status: **partially implemented; full hosted CI remains P2**

Implemented:

- Build validates Node scaffold, web assets, and migrations.
- Tests cover API, auth, storage, Postgres mapping, and P1 flows.

Remaining beyond P1:

- Hosted CI enforcement.
- Dependency audit.
- Secret scanning.
- Python lint/type/test gate.

## P1 — Usable from UI

### P1.1 — Accounts and portfolio ledger

Status: **implemented as paper/sandbox ledger scaffold**

Implemented:

- Account records.
- NAV/cash display.
- UI account cards.

Remaining beyond P1:

- Real Plaid token exchange.
- Encrypted token storage.
- Holdings/transaction sync.
- Reconciliation engine.

### P1.2 — Instrument master

Status: **implemented as normalized instrument registry scaffold**

Implemented:

- Instrument records.
- Active/inactive status.
- Strategy validation against instrument registry.
- UI instrument table.

### P1.3 — Strategy templates and parameter UI

Status: **implemented for default templates and validation**

Implemented:

- Template registry.
- Parameter schemas.
- Default values.
- API validation.
- UI template cards.
- Create-from-template action.

### P1.4 — Backtest reports

Status: **implemented as deterministic report cards and report endpoint**

Implemented:

- Metrics.
- Assumptions.
- Trades.
- Equity curve.
- Report metadata.

Remaining beyond P1:

- Comparison view.
- Export formats.
- Charting.

### P1.5 — Approval workflow

Status: **implemented**

Implemented:

- Approval requests.
- Requires backtest evidence when requested through P1 route.
- Approve/reject decisions.
- Audit trail.
- UI approval buttons.

### P1.6 — Paper/shadow execution

Status: **implemented as lifecycle scaffold**

Implemented:

- Start paper session for approved strategy.
- Stop paper session.
- Stop all paper sessions.
- Kill switch stops running paper sessions.
- UI paper execution panel.

Remaining beyond P1:

- Full signal/order/fill lifecycle.
- Position P&L from simulated fills.
- Reconciliation state machine.

## Final release stance

P0/P1 completion makes the product usable for **mock/paper operator workflow evaluation**. It does not certify production or live trading.
