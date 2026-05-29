# TODO — Production Readiness Backlog

This repository is **not production-ready**. Do not use it for live trading or real-money execution until the items below are implemented, tested, reviewed, and formally approved.

## P0 — Blockers

### P0.1 — Build the operator UI
- [ ] Create a browser UI for accounts, strategies, backtests, approvals, positions/risk, audit logs, health, and settings.
- [ ] Make the UI the primary workflow; CLI remains developer/admin only.
- [ ] Add visible pause/stop controls and audit logging.

### P0.2 — Replace API stubs with real routes
- [ ] Implement typed API routes for accounts, instruments, strategies, backtests, approvals, paper mode, positions, risk, audit, health, and readiness.
- [ ] Add authentication, request validation, request IDs, and structured error responses.
- [ ] Add OpenAPI documentation and route tests.

### P0.3 — Add database migrations and repositories
- [ ] Define the canonical schema for accounts, instruments, strategies, backtest runs, approvals, orders, fills, positions, balances, and audit logs.
- [ ] Add migrations and integration tests against a fresh Postgres container.
- [ ] Add seed data for local development.

### P0.4 — Complete strategy lifecycle management
- [ ] Define a strategy schema with versioning, parameters, risk limits, and lifecycle status.
- [ ] Persist strategy definitions and changelogs.
- [ ] Add UI/API flows for create, edit, clone, archive, validate, and approve.

### P0.5 — Replace prototype backtesting
- [ ] Execute registered strategy versions instead of hardcoded demo logic.
- [ ] Add realistic fees, spread, slippage, latency, partial fills, and market-impact assumptions.
- [ ] Persist immutable backtest artifacts and expose metrics/trade logs in the UI.

### P0.6 — Keep real-money execution disabled
- [ ] Fail closed unless all certification, approval, reconciliation, and risk checks pass.
- [ ] Add CI tests proving unsafe execution paths are blocked.
- [ ] Add a single shared kill switch across UI, API, workers, and adapters.

### P0.7 — Clean deployment hazards
- [ ] Remove local absolute paths from executable tests and scripts.
- [ ] Move host-specific examples to docs/examples.
- [ ] Ensure generated files, caches, secrets, and local env files are ignored.

### P0.8 — Make CI strict
- [ ] Add Node and Python lint/type/test/build gates.
- [ ] Remove permissive failure bypasses from CI.
- [ ] Add migration validation, dependency audit, and secret scanning.

## P1 — Make the product usable from the UI

### P1.1 — Accounts and portfolio ledger
- [ ] Implement sandbox account linking, encrypted token storage, refresh, revocation, holdings, transactions, and reconciliation.
- [ ] Add consolidated NAV, cash, exposure, P&L, and refresh status in the UI.

### P1.2 — Instrument master and market data
- [ ] Add normalized instrument registry and data-quality checks.
- [ ] Make all strategies and backtests select instruments from the same registry.

### P1.3 — Strategy templates and parameter UI
- [ ] Add common strategy templates and generate UI forms from parameter schemas.
- [ ] Validate parameter ranges and incompatible settings.

### P1.4 — Backtest reports
- [ ] Add equity curve, drawdown, trade table, metrics summary, parameter snapshot, and export options.
- [ ] Add comparison view across strategy versions and parameter sets.

### P1.5 — Approval and certification workflow
- [ ] Persist approval requests and decisions.
- [ ] Require backtest evidence before strategy promotion.
- [ ] Add audit trail in the UI.

### P1.6 — Paper/shadow execution
- [ ] Implement signal-to-order-preview-to-paper-fill lifecycle.
- [ ] Show paper positions, fills, P&L, and reconciliation status in the UI.

## P2 — Production hardening

### P2.1 — Adapter interfaces
- [ ] Define contract tests for adapters before enabling any real connector.

### P2.2 — Prediction-market venue integration
- [ ] Complete read-only market discovery and matching first.
- [ ] Keep order submission disabled until certification is complete.

### P2.3 — Onchain runtime hardening
- [ ] Add checkpoints, reorg handling, rate limits, retries, and ABI-driven parsing.

### P2.4 — Observability and runbooks
- [ ] Add metrics, logs, alerts, dashboards, deployment, rollback, backup, restore, and incident-response runbooks.

### P2.5 — Security review
- [ ] Add auth, RBAC, CORS/CSRF policy, audit logging, secret management, and dependency scanning.

## P3 — Advanced research

### P3.1 — Fair-market-price evaluation
- [ ] Replace placeholder price bands with tested models and provenance.

### P3.2 — Hypothesis generation
- [ ] Add research queue and require human approval before promotion.

### P3.3 — Portfolio optimization
- [ ] Add allocation constraints, risk budgets, scenario analysis, and stress tests.
