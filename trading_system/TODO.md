# Trading System — TODO Backlog

This backlog reflects the repo after the May 26, 2026 implementation/demo pass. Completed scaffold items were removed from the critical path; the remaining work focuses on making the system operationally reliable, database-migratable, observable, and safe for staged execution.

## P0 — Blockers before staging

### P0.1 Commit and validate baseline Alembic revision
- **Why**: `alembic/env.py` and SQLAlchemy models are present, but operators need a committed, reviewed baseline revision and upgrade proof.
- **Deliverables**:
  - committed baseline revision under `alembic/versions/`;
  - `alembic upgrade head` tested on a fresh Postgres database;
  - migration smoke test documented in `docs/MIGRATION_GUIDE.md`.
- **Acceptance criteria**:
  - fresh DB can run `alembic upgrade head` without using `Base.metadata.create_all` as a substitute;
  - CI or local script verifies current head.

### P0.2 Add database-backed integration test harness
- **Why**: Current tests validate many units/contracts, but the deployment path needs database-backed API smoke coverage.
- **Deliverables**:
  - Postgres test fixture or service-container based integration setup;
  - seed data for portfolios, strategies, orders, fills, approvals, alerts, incidents, audit events, and feed health;
  - tests for `/ready`, `/ops/*` read/write paths, and migration head state.
- **Acceptance criteria**:
  - `pytest -q tests/integration` passes against a real Postgres URL;
  - tests fail if the schema is missing or stale.

### P0.3 Remove committed runtime artifacts and local environment bleed-through
- **Why**: Runtime artifacts and local paths make repo state noisy and can hide operational drift.
- **Deliverables**:
  - stop tracking generated experiment logs unless intentionally versioned as fixtures;
  - ensure `.venv/`, build artifacts, caches, and runtime outputs are ignored;
  - move intentional demo artifacts into `tests/fixtures/` or `docs/evidence/`.
- **Acceptance criteria**:
  - `git status` stays clean after `make ci`, demo backtests, and local app startup.

## P1 — Core runtime wiring

### P1.1 Signal-to-fill end-to-end workflow
- **Why**: The system has strategy, risk, paper exchange, persistence, audit, and websocket components, but needs a single tested path joining them.
- **Deliverables**:
  - `tests/e2e/test_signal_to_fill.py`;
  - worker path: market event -> strategy signal -> risk evaluation -> paper order -> fill -> persisted order/fill -> audit event -> websocket/notification emission;
  - deterministic fixture for repeatable assertions.
- **Acceptance criteria**:
  - one command proves the paper-mode trade lifecycle end to end;
  - no live credentials are required.

### P1.2 Wire worker event publishing to websocket hub
- **Why**: WebSocket endpoints exist, but producers must publish real order/market events.
- **Deliverables**:
  - worker publishes order lifecycle events to `orders` channel;
  - paper exchange publishes fill events;
  - market data storage publishes candle/tick events to `market:{product_id}`.
- **Acceptance criteria**:
  - websocket client receives order and market updates during the e2e test.

### P1.3 Coinbase live connector staging harness
- **Why**: Coinbase REST/WebSocket clients exist, but live integration must be gated and testable without enabling production execution.
- **Deliverables**:
  - read-only account/portfolio sync command;
  - shadow-mode order preview path;
  - reconciliation report comparing local and remote state.
- **Acceptance criteria**:
  - credentials are required for live/shadow connector tests;
  - failures degrade to no-trade state;
  - `LIVE_TRADING_ENABLED=false` blocks order placement.

### P1.4 Onchain ingestion runtime
- **Why**: RPC adapters and services exist, but no runtime continuously calls them.
- **Deliverables**:
  - poller for tracked pools, token metadata, price feeds, and events;
  - persistence path for snapshots and event observations;
  - safety scoring before route approval packet generation.
- **Acceptance criteria**:
  - onchain data can be fetched in paper/shadow mode without signing transactions;
  - RPC errors are retried and surfaced in feed health.

## P2 — Production hardening

### P2.1 Rate limiting and abuse protection
- **Deliverables**:
  - token-bucket middleware backed by Redis for multi-worker deployments;
  - safe defaults for local single-process mode;
  - tests for burst and sustained limits.

### P2.2 Redis-backed pub/sub
- **Deliverables**:
  - replace or augment in-memory `PubSubHub` for multi-process deployments;
  - worker -> Redis -> API websocket fanout path;
  - fallback behavior when Redis is unavailable.

### P2.3 Deployment verification
- **Deliverables**:
  - local Docker Compose smoke script;
  - production compose validation checklist;
  - optional Kubernetes readiness/liveness checks for API and worker.

### P2.4 Secrets and key management plan
- **Deliverables**:
  - documented dev/staging/prod secret sources;
  - explicit prohibition on committing private keys or API secrets;
  - future KMS/HSM signing adapter interface for onchain wallets.

### P2.5 Operator UI/API contract hardening
- **Deliverables**:
  - versioned API contract notes;
  - response schema snapshots for `/ops/*` routes;
  - compatibility policy for frontend consumers.

## P3 — Completeness and research extensions

### P3.1 Strategy catalog quality gates
- Verify all catalog strategies have metadata, risk-tier hints, paper-mode support labels, and backtest/replay capability flags.

### P3.2 Backtesting evidence pack
- Store canonical benchmark configs, expected outputs, and result interpretation docs.

### P3.3 Onchain advanced modules
- Prioritize MEV, bridge, DEX routing, liquidity graph, and Solana research only after paper-mode CEX and Base/Ethereum shadow-mode paths are stable.

### P3.4 Documentation system
- Add `mkdocs` or a lightweight docs index once runbooks, migration guide, API map, and implementation plan stabilize.

## Done / no longer TODO

- Dockerfile and Docker Compose deployment assets.
- GitHub Actions quality workflow.
- `make ci` local quality command.
- Prometheus-format `/metrics` endpoint.
- `/ready` endpoint.
- Core SQLAlchemy model set.
- Coinbase REST/WebSocket client modules.
- Paper exchange engine.
- Risk, execution, notification, analytics, storage, and onchain module implementations at initial functional depth.
