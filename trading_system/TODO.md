# Trading System — TODO Backlog

<<<<<<< HEAD
This backlog reflects the repo after the May 26, 2026 implementation/demo pass and adds the new agentic evaluation mechanism scope: automatic position evaluation, fair-market-price estimation, strategy research/backtesting/certification, approval packets, Plaid account-data integration, equity broker adapters, and crypto/onchain execution alignment.
=======
This backlog reflects the repo after the May 26, 2026 implementation/demo pass. Completed scaffold items were removed from the critical path; the remaining work focuses on making the system operationally reliable, database-migratable, observable, and safe for staged execution.
>>>>>>> b5e23b51 (Added falcon updates)

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

<<<<<<< HEAD
## P1 — Data foundation and account aggregation

### P1.1 Plaid sandbox integration
- **Why**: The agentic evaluator needs external account, holding, security, balance, and investment-transaction data before it can evaluate the real portfolio.
- **Deliverables**:
  - Plaid Link token flow in sandbox/dev;
  - models for Plaid Item, institution, account, consent state, webhook state, holdings, securities, and investment transactions;
  - encrypted storage/handling for Plaid access tokens;
  - refresh jobs and audit events.
- **Acceptance criteria**:
  - sandbox Item can be linked, refreshed, revoked, and audited;
  - holdings and transactions map to canonical internal records;
  - revoked consent disables refresh and downstream evaluation;
  - access tokens are never logged or returned in API responses.

### P1.2 Canonical multi-account portfolio ledger
- **Deliverables**:
  - institution/account/balance snapshot/position snapshot/transaction/tax lot/corporate action/transfer models;
  - consolidated NAV, cash, exposure, unrealized P&L, realized P&L, and allocation-drift endpoint;
  - source connector and refresh timestamp on every external record.
- **Acceptance criteria**:
  - portfolio state can be rebuilt from source snapshots and ledger records;
  - stale or mismatched source data blocks automated recommendations.

### P1.3 Instrument master and security mapping
- **Deliverables**:
  - canonical `Instrument` model for equities, ETFs, options, crypto, stablecoins, LP positions, and onchain tokens;
  - mapping between Plaid securities, broker symbols, Coinbase products, and onchain token addresses;
  - quarantine workflow for unresolved instruments.
- **Acceptance criteria**:
  - every seeded holding resolves to one canonical instrument;
  - unresolved instruments cannot be traded or recommended automatically.

## P2 — Core runtime wiring

### P2.1 Signal-to-fill end-to-end workflow
=======
## P1 — Core runtime wiring

### P1.1 Signal-to-fill end-to-end workflow
>>>>>>> b5e23b51 (Added falcon updates)
- **Why**: The system has strategy, risk, paper exchange, persistence, audit, and websocket components, but needs a single tested path joining them.
- **Deliverables**:
  - `tests/e2e/test_signal_to_fill.py`;
  - worker path: market event -> strategy signal -> risk evaluation -> paper order -> fill -> persisted order/fill -> audit event -> websocket/notification emission;
  - deterministic fixture for repeatable assertions.
- **Acceptance criteria**:
  - one command proves the paper-mode trade lifecycle end to end;
  - no live credentials are required.

<<<<<<< HEAD
### P2.2 Wire worker event publishing to websocket hub
=======
### P1.2 Wire worker event publishing to websocket hub
>>>>>>> b5e23b51 (Added falcon updates)
- **Why**: WebSocket endpoints exist, but producers must publish real order/market events.
- **Deliverables**:
  - worker publishes order lifecycle events to `orders` channel;
  - paper exchange publishes fill events;
  - market data storage publishes candle/tick events to `market:{product_id}`.
- **Acceptance criteria**:
  - websocket client receives order and market updates during the e2e test.

<<<<<<< HEAD
### P2.3 Strategy lifecycle wiring
- **Deliverables**:
  - sync catalog entries into `strategy_configs`;
  - persist enable/disable/start/stop/pause/resume state;
  - prevent disabled strategies from producing runtime orders.

## P3 — Agentic valuation and position evaluation

### P3.1 Fair-market-price engine
- **Deliverables**:
  - valuation model contract with `fair_value_low`, `fair_value_mid`, `fair_value_high`, `buy_below`, `sell_above`, `hold_range`, confidence, expected holding period, invalidation triggers, max position size, and hedge/stop policy;
  - market microstructure, technical/statistical, fundamental, macro/regime, sentiment/news, and onchain valuation modules;
  - persisted valuation snapshots with source-data IDs and model versions.
- **Acceptance criteria**:
  - every recommendation includes price bands, rationale, timestamp, model version, and evidence;
  - stale data or unresolved instrument mapping blocks recommendation generation.

### P3.2 Specialist evaluation agents
- **Deliverables**:
  - Position Auditor, Market Analyst, Fundamental Analyst, Crypto/Onchain Analyst, Risk Analyst, Strategy Researcher, Backtest Critic, and Approval Drafter;
  - structured outputs stored in database;
  - disagreement/dissent handling.
- **Acceptance criteria**:
  - final recommendation includes buy/sell/hold/reduce/exit/watch, confidence, risk score, investment philosophy, holding period, evidence, and dissenting signals;
  - agents cannot submit trades or approve their own recommendations.

### P3.3 Recommendation API and audit trail
- **Deliverables**:
  - endpoints for portfolio recommendations, instrument recommendations, evaluation history, stale-data warnings, and pending approvals;
  - audit events for each recommendation change.
- **Acceptance criteria**:
  - operator can answer why every position is buy/sell/hold;
  - recommendations are reconstructable from source data and model versions.

## P4 — Strategy research, backtesting, and certification

### P4.1 Strategy hypothesis registry
- **Deliverables**:
  - immutable strategy hypotheses with philosophy, target instruments, timeframe, holding period, signal rules, exit rules, risk constraints, and expected edge;
  - deterministic config hashes;
  - links to backtest runs, paper runs, approvals, and execution attempts.
- **Acceptance criteria**:
  - no strategy can be backtested or approved without a registered hypothesis;
  - strategy version changes require new certification.

### P4.2 Backtest certification gates
- **Deliverables**:
  - historical multi-regime backtests;
  - walk-forward and out-of-sample validation;
  - transaction-cost/slippage/liquidity capacity model;
  - tail-risk, drawdown, sensitivity, benchmark, and stress replay checks;
  - Backtest Critic checks for look-ahead, survivorship, overfitting, and unrealistic fills.
- **Acceptance criteria**:
  - positive net return after fees/slippage;
  - acceptable drawdown for risk tier;
  - threshold metrics defined per strategy type;
  - fragile/overfit strategies are rejected before paper/shadow execution.

### P4.3 Paper/shadow incubation reports
- **Deliverables**:
  - runtime paper-mode incubation;
  - shadow-mode payload generation against live quotes without execution;
  - report comparing backtest assumptions to runtime fills, slippage, latency, and risk events.
- **Acceptance criteria**:
  - strategy cannot request live/canary approval until incubation completes.

## P5 — Approval pipeline

### P5.1 Strategy approval packets
- **Deliverables**:
  - strategy hypothesis and config hash;
  - investment philosophy;
  - instruments, venues, accounts touched;
  - fair-value logic and signal rules;
  - backtest/walk-forward/paper/shadow evidence;
  - expected return/risk range;
  - max allocation and capital at risk;
  - holding period and exit criteria;
  - stop-loss, hedge, kill-switch policy;
  - compliance constraints;
  - canary/live rollout plan;
  - required human approver and expiry time.
- **Acceptance criteria**:
  - approval packet cannot execute directly;
  - expiry or strategy code/config/data change requires re-evaluation.

### P5.2 Trade approval packets
- **Deliverables**:
  - account, venue, instrument, side, order type, order bounds, fair-value range;
  - expected slippage, fees, spread, liquidity, fill risk;
  - position and portfolio exposure impact;
  - holding period and exit plan;
  - source strategy approval reference.
- **Acceptance criteria**:
  - live execution checks both strategy approval and trade approval;
  - rejected trades and partial fills update audit trail.

## P6 — Execution adapters

### P6.1 Equity broker adapter interface
- **Why**: Plaid is for data aggregation; stock/ETF/options orders require broker APIs.
- **Deliverables**:
  - adapter interface for account sync, asset metadata, buying power, order preview, submit, cancel, positions, fills, activities, and reconciliation;
  - paper broker adapter;
  - first real broker adapter in read-only/shadow mode before live support.
- **Acceptance criteria**:
  - stock/ETF execution is impossible through Plaid connector code;
  - broker adapter supports preview and reconciliation before submit is enabled.

### P6.2 Crypto execution adapter alignment
- **Deliverables**:
  - Coinbase adapter aligned to strategy/trade approval contracts;
  - exchange capability matrix for products, fees, order types, limits, custody constraints, and region restrictions;
  - exchange trust degradation blocks new orders.

### P6.3 Onchain execution approval alignment
- **Deliverables**:
  - route approval packets with gas/slippage/capital-at-risk estimates;
  - allowlisted contracts/tokens and wallet spend policies;
  - signer interface for local dev key, KMS, HSM, or hardware-wallet/manual approval.
- **Acceptance criteria**:
  - no transaction can be signed from an unapproved route.

## P7 — Production hardening and UI

### P7.1 Rate limiting and abuse protection
=======
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
>>>>>>> b5e23b51 (Added falcon updates)
- **Deliverables**:
  - token-bucket middleware backed by Redis for multi-worker deployments;
  - safe defaults for local single-process mode;
  - tests for burst and sustained limits.

<<<<<<< HEAD
### P7.2 Redis-backed pub/sub
=======
### P2.2 Redis-backed pub/sub
>>>>>>> b5e23b51 (Added falcon updates)
- **Deliverables**:
  - replace or augment in-memory `PubSubHub` for multi-process deployments;
  - worker -> Redis -> API websocket fanout path;
  - fallback behavior when Redis is unavailable.

<<<<<<< HEAD
### P7.3 Deployment verification
=======
### P2.3 Deployment verification
>>>>>>> b5e23b51 (Added falcon updates)
- **Deliverables**:
  - local Docker Compose smoke script;
  - production compose validation checklist;
  - optional Kubernetes readiness/liveness checks for API and worker.

<<<<<<< HEAD
### P7.4 Secrets and key management plan
- **Deliverables**:
  - documented dev/staging/prod secret sources;
  - explicit prohibition on committing private keys, Plaid access tokens, or broker/exchange API secrets;
  - future KMS/HSM signing adapter interface for onchain wallets.

### P7.5 Operator UI/API contract hardening
- **Deliverables**:
  - recommendation dashboard;
  - pending strategy/trade approvals;
  - evidence and dissenting-agent output viewer;
  - approve/reject with comments;
  - versioned API contract notes and schema snapshots.
=======
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
>>>>>>> b5e23b51 (Added falcon updates)

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
