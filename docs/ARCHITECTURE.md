# Architecture

## Current architecture status

The repository currently contains two partially connected system layers:

1. **Node/TypeScript prediction-market arbitrage scaffold** under `apps/` and `packages/`.
2. **Python portfolio-management and trading-system scaffold** under `trading_system/`.

This split is workable, but it must be made explicit before production. Today, the service boundary is not clear enough for deployment.

## Intended product architecture

```text
apps/web-ui
  Operator dashboard
  - accounts and connectors
  - strategy registry
  - strategy configuration
  - backtest runner and reports
  - approval workflow
  - paper/shadow execution status
  - portfolio positions and risk
  - audit trail
  - system health and kill switch

apps/api
  Product API
  - auth/session
  - readiness and health
  - accounts and portfolio ledger
  - instruments and market data
  - strategies
  - backtests
  - approvals and certification
  - execution controls
  - audit logs

trading_system/
  Python domain services
  - plaid/account ingestion
  - portfolio ledger
  - instrument master
  - strategy registry
  - backtesting
  - research and hypothesis generation
  - fair-market-price evaluation
  - approval workflow
  - onchain ingestion and safety scoring

packages/
  Node domain modules
  - prediction-market arbitrage math
  - market matching
  - execution safety gates
  - venue adapter contracts
  - storage/reconciliation primitives
```

## Production rule

The UI must be the primary operating surface. CLI commands are useful for local diagnostics and automation, but they are not a substitute for a complete operator workflow.

## Required service boundary decision

Before adding more features, choose one of these paths:

### Option A — Python primary API

Use FastAPI as the main product API for portfolio-management, strategies, backtesting, approvals, and onchain services. Keep Node prediction-market logic as either a separate worker/service or gradually port core calculations into Python.

### Option B — Node primary API

Use the existing Node `apps/api` as the product API and call Python domain services over RPC/HTTP/queue boundaries.

### Option C — Two-service architecture

Run Node and Python as separate services, but define:

- ownership boundaries
- API contracts
- shared database policy
- event contracts
- auth model
- deployment topology
- observability and incident ownership

Do not deploy until this decision is made and documented.

## Core production flows

### Strategy setup flow

1. User opens UI.
2. User creates or imports strategy definition.
3. API validates schema, parameters, allowed instruments, and risk limits.
4. Strategy version is persisted.
5. Strategy appears in draft state.

### Backtest flow

1. User selects strategy version, data range, instruments, capital, and execution assumptions.
2. API validates the request.
3. Backtest worker runs deterministic simulation.
4. Results are persisted with immutable artifact IDs.
5. UI displays metrics, equity curve, drawdown, trade log, and assumptions.

### Approval flow

1. User submits strategy/backtest evidence for approval.
2. Approval workflow scores risk and routes the request.
3. Decision is persisted with reviewer, timestamp, evidence, and reason.
4. Approved strategies can move to paper/shadow incubation.

### Paper/shadow flow

1. Approved strategy emits signals.
2. Execution service creates order previews.
3. Paper executor simulates fills.
4. Reconciliation verifies expected positions and balances.
5. UI shows status, P&L, risk, and incidents.

### Production/live flow

Live execution must remain disabled until all lower environments pass certification. Live mode requires explicit approval, credentials, reconciliation, kill-switch coverage, and incident runbooks.

## Non-negotiable production controls

- Auth and role-based permissions.
- Strict config validation.
- Central kill switch.
- Immutable audit log.
- DB-backed reconciliation.
- Secrets management.
- CI gates for tests, typing, linting, migrations, and secret scanning.
- Health/readiness endpoints that verify dependencies.
- Rollback and restore procedures.
