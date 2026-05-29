# Production Readiness Review — 2026-05-29

## Decision

This repository is **not ready for production deployment**.

It is currently best treated as a mock/paper scaffold and planning baseline. The codebase contains useful safety concepts and early domain modules, but the full product workflow is not implemented end to end.

## Main reason

The system is not yet usable from the UI. The repository does not currently include a complete operator dashboard for account setup, strategy registration, parameter configuration, backtesting, approval, paper/shadow incubation, monitoring, and promotion.

## Current state by area

| Area | Status | Notes |
|---|---|---|
| Mock CLI | Partial | Returns deterministic JSON for a few commands. |
| API | Prototype | The Node server only exposes health, metrics, and generic route responses. |
| UI | Missing | No production operator UI is present. |
| Strategy lifecycle | Incomplete | Registry scaffolding exists, but persistence and validation are unfinished. |
| Backtesting | Prototype | The engine does not run registered strategy versions with realistic execution assumptions. |
| Plaid/account layer | Mock | Current routes return generated tokens and mock accounts. |
| Persistence | Incomplete | No canonical migration-backed schema is established across the whole system. |
| Approvals | Prototype | Useful routing logic exists, but routes and persistence are placeholders. |
| Onchain runtime | Prototype | Polling and scoring scaffolds exist but need persistence, reorg handling, retries, and production validation. |
| Deployment | Incomplete | Strict CI, release gates, production runbooks, and rollback procedures are missing. |

## What is strong

- Safety intent is clearly present.
- Mock/paper defaults are appropriate.
- The repo has useful early modules for arbitrage math, matching, safety gates, approval routing, backtesting, strategy registration, Plaid/account scaffolding, and onchain ingestion.
- Existing docs already acknowledge that live operation is not certified.

## Production blockers

### P0.1 — Missing operator UI

Build the browser UI before adding more strategy ideas. The UI must support accounts, strategies, backtests, approvals, positions, risk, audit logs, settings, and emergency controls.

### P0.2 — API stubs must become real routes

Replace stubbed routes with typed endpoints, request validation, authentication, persistence, and tests.

### P0.3 — Strategy lifecycle must be executable

A user must be able to create, validate, version, backtest, approve, and archive strategies through the UI/API.

### P0.4 — Backtesting must be realistic and reproducible

Backtests must run registered strategy versions and include fees, spreads, slippage, market impact, data-quality checks, metrics, immutable artifacts, and UI reports.

### P0.5 — Account and portfolio data must be real or clearly marked mock

Plaid/account flows must use sandbox first, persist normalized records, encrypt tokens, verify webhooks, and prevent mock balances from appearing as production data.

### P0.6 — Persistence and migrations must be canonical

Define one schema source of truth, add migrations, repositories, seed data, and integration tests.

### P0.7 — Strict release gates are required

CI must fail on lint, type, test, migration, security, and secret-scan failures. Avoid permissive `|| true` behavior in release workflows.

## Recommended next implementation order

1. UI shell and API contract.
2. Database schema and migrations.
3. Strategy lifecycle.
4. Backtest engine and reports.
5. Plaid/account ledger.
6. Approval and certification workflow.
7. Paper/shadow execution.
8. Observability, deployment, rollback, and incident response.
9. Real broker, venue, and onchain adapters only after the above is stable.

## Minimum release bar

The product is not production-ready until a fresh clone can start the full stack, a user can operate the strategy/backtest workflow from the UI, all state is persisted, all safety gates are tested, and the deployment pipeline blocks unsafe releases.
