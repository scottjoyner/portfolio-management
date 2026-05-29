# TODO — Production Readiness Backlog

This repository is **usable for P0/P1 mock/paper operator workflow evaluation**, but it is **not production-ready** and must not be used for live trading or real-money execution until P2 certification is complete.

## P0 — Blockers

### P0.1 — Build the operator UI
- [x] Create a browser UI for accounts, strategies, backtests, approvals, positions/risk, audit logs, health, and settings-equivalent controls.
- [x] Make the UI the primary workflow; CLI remains developer/admin only.
- [x] Add visible pause/stop controls and audit logging.

### P0.2 — Replace API stubs with real routes
- [x] Implement API routes for accounts, instruments, strategies, backtests, approvals, paper mode, positions, risk, audit, health, and readiness.
- [x] Add auth guard, request IDs, request validation, and structured error responses for the P0/P1 operator surface.
- [x] Add API contract documentation and route tests.

### P0.3 — Add database migrations and repositories
- [x] Define schema baseline for accounts, instruments, strategies, backtest runs, approvals, paper executions, positions, and audit logs.
- [x] Add migrations and dependency-free Postgres fake-client tests.
- [x] Add seed data for local development.
- [ ] P2: Add live integration tests against a fresh Postgres container.
- [ ] P2: Replace P1 JSON-flag product persistence with row-level Postgres repository operations.

### P0.4 — Complete strategy lifecycle management
- [x] Define a strategy schema with versioning, parameters, risk limits, and lifecycle status.
- [x] Persist strategy definitions and changelogs/audit events.
- [x] Add UI/API flows for create, clone, archive/status, validate, and approve.

### P0.5 — Replace prototype backtesting
- [x] Execute registered strategy versions in the P1 deterministic certification scaffold.
- [x] Add fee/slippage assumptions and report artifacts.
- [x] Persist backtest artifacts and expose metrics/trade logs in the UI/API.
- [ ] P2: Replace deterministic scaffold with historical market replay, latency, spreads, partial fills, market impact, and walk-forward validation.

### P0.6 — Keep real-money execution disabled
- [x] Fail closed unless certification/approval/risk checks pass.
- [x] Add tests proving unsafe live execution paths are blocked.
- [x] Add a shared kill-switch control that stops paper executions and is visible from UI/API.

### P0.7 — Clean deployment hazards
- [x] Ignore generated local runtime state.
- [x] Move operational usage into docs/runbooks.
- [x] Keep live credentials out of source and default runtime.

### P0.8 — Make CI/build strict
- [x] Add Node test/build gates for the operator scaffold.
- [x] Add migration validation.
- [x] Add API contract validation.
- [x] Add UI asset validation.
- [ ] P2: Add hosted CI enforcement, dependency audit, Python gates, and secret scanning.

## P1 — Make the product usable from the UI

### P1.1 — Accounts and portfolio ledger
- [x] Add paper/sandbox account ledger scaffold.
- [x] Add consolidated NAV, cash, exposure-ready state, and refresh status fields in the UI.
- [ ] P2: Implement real Plaid sandbox token exchange, encrypted token storage, refresh, revocation, holdings, transactions, and reconciliation.

### P1.2 — Instrument master and market data
- [x] Add normalized instrument registry and basic data-quality/status fields.
- [x] Make strategies and backtests select/validate instruments from the same registry.
- [ ] P2: Add real market data adapters, snapshots, historical bars, and data-quality scoring.

### P1.3 — Strategy templates and parameter UI
- [x] Add common strategy templates and parameter schemas.
- [x] Validate parameter ranges and incompatible settings.
- [x] Add UI create-from-template flow.

### P1.4 — Backtest reports
- [x] Add metrics summary, assumptions, equity curve data, and trade table data.
- [x] Add report endpoint and UI cards.
- [ ] P2: Add comparison view across strategy versions and parameter sets.
- [ ] P2: Add exports/charts.

### P1.5 — Approval and certification workflow
- [x] Persist approval requests and decisions.
- [x] Require backtest evidence before strategy promotion on P1 approval routes.
- [x] Add audit trail in the UI/API.

### P1.6 — Paper/shadow execution
- [x] Implement approved-strategy paper session lifecycle.
- [x] Show paper execution status in the UI.
- [x] Add stop controls and kill-switch interaction.
- [ ] P2: Implement full signal-to-order-preview-to-paper-fill lifecycle, simulated fills, P&L, and reconciliation.

## P2 — Production hardening

### P2.1 — Adapter interfaces
- [ ] Define contract tests for broker, exchange, Plaid, market-data, and onchain adapters before enabling any real connector.

### P2.2 — Prediction-market venue integration
- [ ] Complete read-only market discovery and matching first.
- [ ] Keep order submission disabled until certification is complete.

### P2.3 — Onchain runtime hardening
- [ ] Add checkpoints, reorg handling, rate limits, retries, and ABI-driven parsing.

### P2.4 — Observability and runbooks
- [ ] Add metrics, logs, alerts, dashboards, deployment, rollback, backup, restore, and incident-response runbooks.

### P2.5 — Security review
- [ ] Add full RBAC, CORS/CSRF policy, immutable audit logging, secret management, dependency scanning, and penetration review.

## P3 — Advanced research

### P3.1 — Fair-market-price evaluation
- [ ] Replace placeholder price bands with tested models and provenance.

### P3.2 — Hypothesis generation
- [ ] Add research queue and require human approval before promotion.

### P3.3 — Portfolio optimization
- [ ] Add allocation constraints, risk budgets, scenario analysis, and stress tests.
