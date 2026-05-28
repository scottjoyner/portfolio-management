# Trading System — Implementation Plan

This plan reflects the current repository state after the May 26, 2026 implementation/demo pass and extends the trading system with an agentic evaluation mechanism for portfolio evaluation, fair-market-price estimation, strategy research, approval routing, and gated execution.

The project is no longer just a scaffold: the core service layout, Docker build assets, CI quality workflow, SQLAlchemy model set, paper exchange, Coinbase connector modules, WebSocket routes, Prometheus metrics, and broad risk/execution/onchain modules now exist. The next major direction is to turn those components into a governed investment-research and execution platform.

## Product boundary

The system has three distinct responsibilities:

1. **Observe**: ingest accounts, balances, holdings, transactions, prices, fundamentals, macro data, crypto exchange data, and onchain data.
2. **Evaluate**: automatically evaluate every current and candidate position, estimate fair buy/sell/hold levels, assign holding period, explain investment philosophy, and generate strategy hypotheses.
3. **Execute only after approval**: route profitable and robust strategies through backtesting, paper/shadow incubation, human approval, risk checks, reconciliation, and broker/exchange/onchain execution adapters.

Plaid belongs in the **observe** layer for banking, investment-account, holding, security, and transaction data. It should not be treated as the primary stock-trade execution rail. Stocks/ETFs/options need broker adapters; crypto needs centralized exchange adapters and/or onchain execution modules.

## Current state snapshot

| Area | Current state | Implementation posture |
|---|---|---|
| Packaging | `pyproject.toml` defines Python 3.12 package, dependencies, and dev extras | Present; keep dependency additions intentional |
| Local commands | `Makefile` has install, lint, typecheck, test, ci, api, worker, backtest, paper demo targets | Present; use `make ci` as local gate |
| CI | GitHub Actions runs lint, mypy, pytest, and wheel build for `trading_system/**` changes | Present; add DB service containers next |
| Docker | `Dockerfile`, local compose, and production deploy compose/systemd assets exist | Present; needs smoke validation and clean env handling |
| Database models | Core SQLAlchemy models cover portfolio, strategy, order, fill, capital, approval, audit, alert, incident, exchange state, and market feed domains | Present; extend for accounts, instruments, valuation, strategy certification, and approval packets |
| Alembic | `alembic/env.py` wires metadata and `DATABASE_URL` | Present; baseline revision workflow still needs a hard gate |
| API | Health, readiness, metrics, strategy catalog, risk, reconciliation, onchain, ops, and websocket routes exist | Present; add agentic evaluation and approval APIs |
| Ops API | Repository-backed persistence path exists for operator workflows | Present; prove restart behavior with integration tests |
| Metrics | `/metrics` exports Prometheus client output | Present; add dashboard/runbook later |
| WebSockets | `/ws/orders` and `/ws/market/{product_id}` exist with an in-memory hub | Present; producers and Redis fanout still needed |
| Paper exchange | Paper simulation exists | Present; needs signal-to-fill e2e and strategy incubation coverage |
| Coinbase | REST/WebSocket/auth/account/execution modules exist | Present; needs read-only staging harness, shadow/live gates, and approval alignment |
| Onchain | RPC, wallet, DEX, bridge, MEV, contract, safety, and route-analysis modules exist | Present; needs ingestion runtime and production key-management plan |
| Agentic evaluation | Not yet implemented | New roadmap item; see `docs/AGENTIC_EVALUATION_PLAN.md` |
| Plaid/account aggregation | Not yet implemented | New roadmap item for account/holding/transaction ingestion, not execution |
| Equity broker execution | Not yet implemented | New roadmap item; requires broker adapter separate from Plaid |
| Tests | Unit/integration/sim/performance coverage exists and recent docs report passing lint/typecheck/tests | Present; expand e2e, DB migration, valuation, backtest-certification, and approval tests |
| Database models | Core SQLAlchemy models cover portfolio, strategy, order, fill, capital, approval, audit, alert, incident, exchange state, and market feed domains | Present; migration revision history must be verified |
| Alembic | `alembic/env.py` wires metadata and `DATABASE_URL` | Present; baseline revision workflow still needs a hard gate |
| API | Health, readiness, metrics, strategy catalog, risk, reconciliation, onchain, ops, and websocket routes exist | Present; add contract versioning and DB-backed integration tests |
| Ops API | Repository-backed persistence path exists for operator workflows | Present; prove restart behavior with integration tests |
| Metrics | `/metrics` exports Prometheus client output | Present; add dashboard/runbook later |
| WebSockets | `/ws/orders` and `/ws/market/{product_id}` exist with an in-memory hub | Present; producers and Redis fanout still needed |
| Paper exchange | Paper simulation exists | Present; needs signal-to-fill e2e coverage |
| Coinbase | REST/WebSocket/auth/account/execution modules exist | Present; needs read-only staging harness and shadow/live gates |
| Onchain | RPC, wallet, DEX, bridge, MEV, contract, safety, and route-analysis modules exist | Present; needs ingestion runtime and production key-management plan |
| Tests | Unit/integration/sim/performance coverage exists and recent docs report passing lint/typecheck/tests | Present; expand e2e + DB migration coverage |

## Guiding rules

1. **Default to paper mode**: no implementation should require live trading to validate correctness.
2. **Live trading remains gated**: any live mode must require `LIVE_TRADING_ENABLED=true`, explicit credentials, approvals, and reconciliation.
3. **Separate recommendation from execution**: agents may recommend and draft approval packets, but cannot bypass approval or submit orders directly.
4. **Backtest before approval**: a strategy must be registered, backtested, stress tested, paper/shadow incubated, and approved before live/canary execution.
5. **Treat database migration as production infrastructure**: committed Alembic revisions, backup/restore proof, and CI migration checks are mandatory before staging.
6. **Keep generated artifacts out of source control** unless they are intentionally curated fixtures or evidence.
7. **Audit everything**: every position evaluation, fair-value estimate, strategy hypothesis, backtest, approval, rejection, and execution attempt must be reconstructable.

## Phase 0 — Repo hygiene and migration baseline

### 0.1 Normalize tracked artifacts

**Goal**: keep repository state reproducible after local runs.

**Work**:
- Review generated files under `artifacts/` and decide which are fixtures/evidence versus runtime output.
- Move intentional demo evidence into `docs/evidence/` or `tests/fixtures/`.
- Ensure `.gitignore` excludes `.venv/`, caches, build outputs, local `.env`, experiment logs, and runtime DB files.

**Acceptance criteria**:
- running `make ci`, demo backtests, and local startup does not create dirty tracked files;
- no virtualenv binaries or local secrets are tracked.

### 0.2 Establish baseline Alembic revision

**Goal**: make the database schema reproducible without relying on `Base.metadata.create_all` as the deployment mechanism.

**Work**:
- Run `alembic revision --autogenerate -m "baseline_core_schema"` from `trading_system` if no committed revision exists.
- Manually review table definitions, nullable flags, foreign keys, indexes, and numeric precision.
- Validate `alembic upgrade head` on a fresh Postgres container.
- Document upgrade/rollback steps in `docs/MIGRATION_GUIDE.md`.

**Acceptance criteria**:
- a fresh database can be migrated to head;
- `alembic current` reports the expected head;
- migration smoke coverage is part of local or CI checks.

### 0.3 Add DB-backed integration harness

**Goal**: prove API + repository + migrations against a real database.

**Work**:
- Add integration fixtures that create a temporary Postgres database or use a service container.
- Seed representative portfolio, strategy, order, fill, approval, alert, incident, audit, exchange state, and feed-health rows.
- Verify `/ready`, `/ops/*`, order preview/submit, fills, audit, and restart behavior.

**Acceptance criteria**:
- `pytest -q tests/integration` catches missing/stale schema;
- repository methods survive API restart/re-instantiation.

## Phase 1 — Account, portfolio, and Plaid data foundation

### 1.1 Plaid ingestion

**Goal**: connect external banks and brokerage accounts for normalized balances, holdings, securities, and investment transactions.

**Work**:
- Add Plaid Link token flow for sandbox/dev.
- Store Plaid Items, accounts, institution metadata, consent state, and webhook state.
- Pull investment holdings, securities, balances, and investment transactions.
- Map Plaid securities and account data into internal account/portfolio/instrument models.
- Add token encryption and secret-handling policy for Plaid access tokens.

**Acceptance criteria**:
- sandbox Item can be linked, refreshed, revoked, and audited;
- holdings and transactions map to canonical internal records;
- revoked consent disables refresh and downstream evaluation for that Item;
- Plaid access tokens are never logged or returned by APIs.

### 1.2 Multi-account portfolio ledger

**Goal**: create one canonical view across cash, brokerage, retirement, crypto, and onchain accounts.

**Work**:
- Add account ledger models: institution, account, balance snapshot, position snapshot, transaction, tax lot, corporate action, and transfer.
- Track cash availability separately from margin/borrowing capacity.
- Reconcile stale holdings, missing transactions, and source mismatch.

**Acceptance criteria**:
- one endpoint returns consolidated NAV, cash, exposures, unrealized P&L, realized P&L, and allocation drift;
- each external account can be traced to source connector and refresh timestamp.

### 1.3 Instrument master

**Goal**: unify stocks, ETFs, options, crypto, stablecoins, LP positions, and onchain tokens.

**Work**:
- Add canonical `Instrument` model with asset class, symbol, venue, CUSIP/ISIN where available, chain/address where applicable, currency, multiplier, and trading hours.
- Add mapping between Plaid securities, broker symbols, Coinbase products, and onchain tokens.
- Quarantine unresolved instruments from automated recommendations.

**Acceptance criteria**:
- every position resolves to a canonical instrument;
- unresolved instruments are visible to operators and blocked from automation.

## Phase 2 — End-to-end paper trading path

### 2.1 Paper-mode signal-to-fill workflow

**Goal**: demonstrate a full trading lifecycle without live credentials.

## Phase 1 — End-to-end trading path

### 1.1 Paper-mode signal-to-fill workflow

**Goal**: demonstrate a full trading lifecycle without live credentials.

**Flow**:

```text
market fixture -> strategy signal -> risk evaluation -> paper order -> simulated fill -> persistence -> audit event -> websocket/notification event
```

**Work**:
- Create `tests/e2e/test_signal_to_fill.py` with deterministic fixture data.
- Wire worker/paper exchange components so a strategy can emit an order intent, risk can approve/deny, paper exchange can fill, and repository can persist outcomes.
- Assert order, fill, audit, risk mode, and feed-health state.

**Acceptance criteria**:
- a single test proves the full paper path;
- the test requires no live credentials;
- denied risk decisions produce persisted audit evidence.

### 2.2 Worker-to-WebSocket event publishing

**Goal**: make realtime endpoints useful with actual producers.

**Work**:
- Publish order lifecycle events to `orders` channel.
- Publish market ticks/candles to `market:{product_id}` channel.
- Publish fill events from paper exchange.
- Add WebSocket integration tests that subscribe and assert received events.

**Acceptance criteria**:
- connected clients receive order and market events during paper-mode e2e test;
- disconnected clients do not break publisher flow.

### 2.3 Strategy lifecycle wiring

**Goal**: bridge strategy catalog, persisted configs, and runtime state.

**Work**:
- Sync catalog entries into `strategy_configs` at startup or via explicit command.
- Persist enable/disable/start/stop/pause/resume state.
- Ensure strategy metadata includes supported modes, risk hints, capital bucket, and backtest/replay flags.

**Acceptance criteria**:
- `/strategies/catalog` and ops strategy endpoints agree on strategy IDs and capabilities;
- disabled strategies cannot emit runtime orders.

## Phase 3 — Fair-market-price and agentic position evaluation

### 3.1 Fair-market-price engine

**Goal**: estimate fair buy/sell/hold ranges for every current and candidate position.

**Work**:
- Add valuation models for fair-value low/mid/high, buy-below, sell-above, hold range, confidence, expected holding period, invalidation conditions, max position size, and hedge/stop policy.
- Combine market microstructure, technical/statistical, fundamental, macro/regime, sentiment/news, and onchain inputs.
- Persist valuation snapshots with data-source IDs and model versions.

**Acceptance criteria**:
- every valuation output includes source inputs, timestamp, model version, and rationale;
- recommendations are blocked if market data is stale or instrument mapping is unresolved.

### 3.2 Specialist evaluation agents

**Goal**: produce auditable buy/sell/hold recommendations through specialist agents.

**Agents**:
- Position Auditor.
- Market Analyst.
- Fundamental Analyst.
- Crypto/Onchain Analyst.
- Risk Analyst.
- Strategy Researcher.
- Backtest Critic.
- Approval Drafter.

**Work**:
- Define structured outputs and persistence models for each agent.
- Surface disagreement instead of forcing false consensus.
- Link each recommendation to evidence, source data, and model/prompt version.

**Acceptance criteria**:
- every recommendation has rationale, evidence, confidence, risk score, investment philosophy, holding period, and dissenting signals;
- agents cannot submit trades or approve their own recommendations.

### 3.3 Recommendation API

**Goal**: expose current evaluation state to the operator UI and approval pipeline.

**Work**:
- Add endpoints for portfolio recommendations, instrument recommendations, evaluation history, and stale-data warnings.
- Add filters by account, asset class, philosophy, risk tier, holding period, and approval status.

**Acceptance criteria**:
- operator can answer why each position is BUY/SELL/HOLD/REDUCE/EXIT/WATCH;
- all recommendation changes create audit events.

## Phase 4 — Strategy research, backtesting, and certification

### 4.1 Strategy hypothesis registry

**Goal**: make all strategy ideas versioned, parameterized, and reviewable before testing.

**Work**:
- Add `StrategyHypothesis` records with philosophy, target instruments, timeframe, holding period, signal rules, exit rules, risk constraints, and expected edge.
- Require deterministic config hashes for each parameter set.
- Link hypotheses to backtest runs, paper runs, approval packets, and execution attempts.

**Acceptance criteria**:
- no strategy can be backtested or approved without a registered hypothesis;
- every strategy version is immutable once evaluated.

### 4.2 Backtest certification pipeline

**Goal**: reject weak or overfit strategies before paper/live routing.

**Required checks**:
- historical backtest across multiple regimes;
- walk-forward validation;
- out-of-sample validation;
- transaction-cost and slippage model;
- liquidity capacity model;
- drawdown and tail-risk tests;
- parameter sensitivity analysis;
- benchmark comparison;
- stress replay scenarios.

**Acceptance criteria**:
- strategy must show positive net return after fees/slippage;
- drawdown must be acceptable for declared risk tier;
- Sharpe/Sortino/profit-factor thresholds are defined by strategy type;
- backtest critic must detect and flag look-ahead, survivorship, overfitting, and unrealistic fill assumptions.

### 4.3 Paper/shadow incubation

**Goal**: prove strategies in runtime without risking capital.

**Work**:
- Run certified strategies in paper mode first.
- Run shadow mode against live market data and broker/exchange payload generation.
- Compare expected fills to paper fills and live quotes.
- Track drift from backtest assumptions.

**Acceptance criteria**:
- strategy cannot request live approval until incubation completes;
- incubation report includes paper P&L, slippage estimate, missed fills, latency, and risk events.

## Phase 5 — Approval pipeline

### 5.1 Strategy approval packet

**Goal**: require human approval before a strategy can move into canary/live execution.

**Packet must include**:
- strategy hypothesis and config hash;
- investment philosophy;
- instruments, venues, and accounts touched;
- fair-value logic and signal rules;
- backtest, walk-forward, and paper/shadow evidence;
- expected return/risk range;
- max allocation and capital at risk;
- holding period and exit criteria;
- stop-loss, hedge, and kill-switch policy;
- compliance/regulatory constraints;
- live/canary rollout plan;
- broker/exchange/onchain adapter to use;
- required human approver and expiry time.

**Acceptance criteria**:
- no strategy approval packet can execute directly;
- approval expiry requires re-evaluation;
- code/config/data changes invalidate prior approval unless explicitly grandfathered.

### 5.2 Trade approval packet

**Goal**: require strategy-approved trades to pass per-trade approval and risk checks.

**Packet must include**:
- account and venue;
- instrument and side;
- order type, limit/market bounds, and fair-value range;
- expected slippage, fees, spread, liquidity, and fill risk;
- position impact and portfolio exposure impact;
- holding period and exit plan;
- source strategy approval reference.

**Acceptance criteria**:
- live execution checks both strategy approval and trade approval;
- rejected trades are logged with reasons;
- partial fills and cancellations update audit trail.

## Phase 6 — Broker, crypto, and onchain execution adapters

### 6.1 Equity broker execution layer

**Goal**: execute stocks/ETFs/options through broker APIs, not Plaid.

**Work**:
- Define broker adapter interface: account sync, asset metadata, buying power, order preview, submit, cancel, positions, fills, activities, and reconciliation.
- Implement paper broker adapter first.
- Add one real broker adapter in read-only/shadow mode before live support.
- Require account suitability and asset-class permissions before enabling options, margin, or shorting.

**Acceptance criteria**:
- stock/ETF execution is impossible through Plaid connector code;
- broker adapter must support preview and reconciliation before submit is enabled.

### 6.2 Coinbase and crypto adapter alignment

**Goal**: align crypto execution with the same valuation, approval, and risk contracts.

**Work**:
- Extend Coinbase read-only and shadow-mode paths.
- Add exchange capability matrix for products, fees, order types, limits, custody constraints, and region restrictions.
- Degrade exchange trust on reconciliation mismatch.

**Acceptance criteria**:
- crypto trades use the same strategy/trade approval pipeline;
- exchange trust degradation blocks new orders.

### 6.3 Onchain execution alignment

**Goal**: keep onchain transactions approval-first and signer-safe.

**Work**:
- Generate route analysis, gas/slippage/capital-at-risk estimates, and approval packets.
- Require allowlisted contracts/tokens and wallet spend policies.
- Keep signing disabled unless all safety gates pass.
- Define signer interface for local dev key, cloud KMS, HSM, or hardware-wallet/manual approval.

**Acceptance criteria**:
- no transaction can be signed from an unapproved route;
- production refuses unsafe raw-key configuration unless explicitly overridden.

## Phase 7 — Coinbase staging path

### 7.1 Read-only Coinbase sync

**Goal**: validate credentials and remote state without placing orders.

**Work**:
- Add command/API path to fetch accounts, portfolios, products, and open orders.
- Normalize remote objects into internal models.
- Persist exchange-state snapshots and trust-score evidence.

**Acceptance criteria**:
- read-only sync works with credentials;
- missing/invalid credentials fail closed;
- no order placement path is reachable with `LIVE_TRADING_ENABLED=false`.

### 7.2 Shadow-mode order preview

**Goal**: prove live connector payload generation without sending orders.

**Work**:
- Generate Coinbase order payloads from internal order intents.
- Run risk, sizing, slippage, and approval gates.
- Produce an approval packet and audit event instead of submitting.

**Acceptance criteria**:
- operators can inspect exact would-submit payloads;
- all shadow/live attempts are audit logged;
- live submit code path requires explicit live gate and approval state.

### 7.3 Reconciliation loop

**Goal**: prevent local/remote state divergence.

**Work**:
- Poll or stream remote open orders/fills.
- Compare remote state against local orders/fills.
- Degrade trust score on unknown fills, duplicate events, stale sequence gaps, or mismatched status.

**Acceptance criteria**:
- mismatches block live execution;
- reconciliation summary is visible through API and audit log.

## Phase 8 — Onchain runtime path

### 8.1 RPC ingestion service

**Goal**: turn implemented onchain adapters into a runtime feed.

**Work**:
- Add a poller for Ethereum/Base pools, token metadata, price feeds, and swap/transfer events.
- Persist feed health and snapshots.
- Add retry/backoff and stale-data detection.

**Acceptance criteria**:
- onchain data can be fetched in paper/shadow mode without signing transactions;
- RPC failures are visible in feed-health and do not trigger execution.

## Phase 9 — Production operations

### 9.1 Redis-backed realtime and rate limiting
### 3.2 Approval-first transaction planning

**Goal**: keep onchain execution behind explicit operator approval.

**Work**:
- Generate route analysis, gas/slippage/capital-at-risk estimates, and approval packets.
- Require allowlisted contracts/tokens and wallet spend policies.
- Keep signing disabled unless all safety gates pass.

**Acceptance criteria**:
- no transaction can be signed from an unapproved route;
- approval packet includes fallback route, touched contracts/tokens, expected edge, gas, slippage, and risk state.

### 3.3 Key-management abstraction

**Goal**: avoid long-term dependence on raw private keys in environment variables.

**Work**:
- Define signer interface for local dev key, cloud KMS, HSM, or hardware-wallet/manual approval.
- Keep `EVM_PRIVATE_KEY` as development-only.
- Add startup warnings for production with unsafe key source.

**Acceptance criteria**:
- production mode refuses unsafe key configuration unless explicitly overridden;
- signer interface is testable with a mock signer.

## Phase 4 — Production operations

### 4.1 Redis-backed realtime and rate limiting

**Goal**: make API/worker deployment safe across processes.

**Work**:
- Back WebSocket fanout with Redis pub/sub.
- Add token-bucket or leaky-bucket rate limiting middleware.
- Add tests for burst limits and Redis fallback/degraded behavior.

**Acceptance criteria**:
- multi-worker deployments receive events consistently;
- abusive request bursts are limited;
- Redis outage degrades safely.

### 9.2 Deployment smoke scripts

**Goal**: make Docker/systemd deployment repeatable.

**Work**:
- Add `scripts/smoke_deploy.sh` or equivalent.
- Validate compose build, API startup, `/health`, `/ready`, `/metrics`, migration head, and basic ops endpoint.
- Add systemd checklist for `/opt/portfolio-management/trading_system` deployment.

**Acceptance criteria**:
- local compose deployment can be verified with one script;
- deployment docs match actual commands.

### 9.3 Observability and governance runbooks

**Goal**: turn metrics/logs into operator actions.

**Work**:
- Document key metrics, alert thresholds, and remediation paths.
- Add incident-response checklist for kill switch, exchange trust degradation, DB failure, Redis failure, wallet safety event, and model drift.
- Add sample Grafana dashboard or dashboard spec.

**Acceptance criteria**:
- each critical alert has a runbook and owner action;
- operators can distinguish backtested, paper, shadow, canary, and live incidents.

## Phase 10 — Operator UI and reporting

### 10.1 Recommendation and approval UI

**Goal**: make the agentic system reviewable and controllable.

**Work**:
- Add dashboard for current holdings, fair-value bands, recommendations, pending approvals, active strategies, and live/paper performance.
- Add approval review UI with evidence, dissenting agent outputs, and approve/reject comments.
- Add daily portfolio briefing generated from audited data.

**Acceptance criteria**:
- operator can see why each position is buy/sell/hold;
- operator can approve strategy and trade separately;
- reports distinguish backtested, paper, shadow, canary, and live performance.
- operators can distinguish paper/shadow/live incidents.

## Phase 5 — Strategy and analytics expansion

### 5.1 Strategy quality certification

**Goal**: make the 100-strategy catalog useful and safe.

**Work**:
- Require metadata for risk tier, supported instruments, required data feeds, paper support, live support, and allocation bounds.
- Add per-strategy smoke tests and sample backtests.
- Prevent uncertified strategies from live/canary modes.

**Acceptance criteria**:
- each strategy has a certification status;
- live mode rejects uncertified strategies.

### 5.2 Backtest and replay evidence pack

**Goal**: make performance claims reproducible.

**Work**:
- Create canonical benchmark configs and fixtures.
- Store expected metrics ranges, not one-off fragile outputs.
- Document interpretation of Sharpe, drawdown, turnover, fees, slippage, and fill model assumptions.

**Acceptance criteria**:
- backtest/replay outputs are reproducible from committed configs;
- results distinguish research evidence from production readiness.

## Updated priority backlog

Detailed TODOs live in `TODO.md`. At a high level:

1. Commit and validate baseline Alembic revision.
2. Add DB-backed integration harness.
3. Remove generated/local artifacts from tracked repo state.
4. Add Plaid sandbox ingestion and canonical account/position ledger.
5. Add instrument master and symbol/security mapping.
6. Build paper signal-to-fill e2e test.
7. Add fair-market-price model contract and persistence.
8. Add agentic position-evaluation schema and specialist agents.
9. Add strategy hypothesis registry and backtest certification gates.
10. Add paper/shadow incubation reports.
11. Add strategy and trade approval packets.
12. Add equity broker adapter interface and paper broker adapter.
13. Add Coinbase read-only and shadow-mode staging harness.
14. Add onchain ingestion runtime.
15. Add Redis-backed pub/sub and rate limiting.
16. Add deployment smoke scripts, observability runbooks, and operator UI.

## Completion definition for the next major milestone

The next milestone is **staging-ready paper/shadow evaluation operations**. It is complete when:
4. Build paper signal-to-fill e2e test.
5. Wire worker/paper exchange/market data events to WebSockets.
6. Add Coinbase read-only and shadow-mode staging harness.
7. Add onchain ingestion runtime.
8. Add Redis-backed pub/sub and rate limiting.
9. Add deployment smoke scripts and observability runbooks.
10. Certify strategy catalog and backtest evidence pack.

## Completion definition for the next major milestone

The next milestone is **staging-ready paper/shadow operations**. It is complete when:

- migrations are committed and validated against fresh and seeded Postgres databases;
- `make ci` passes cleanly;
- DB-backed integration tests pass;
- Plaid sandbox ingestion maps holdings and transactions into internal account/position models;
- instrument master resolves all seeded holdings;
- signal-to-fill paper e2e test passes;
- fair-market-price snapshots and buy/sell/hold recommendations are persisted and auditable;
- strategy hypotheses can be backtested and certified or rejected by objective gates;
- approval packets are generated but cannot execute directly;
- WebSocket clients receive real worker/paper events;
- Coinbase read-only sync works and live order placement remains gated;
- docs, TODO, migration guide, and agentic evaluation plan match the actual codebase.
- signal-to-fill paper e2e test passes;
- WebSocket clients receive real worker/paper events;
- Coinbase read-only sync works and live order placement remains gated;
- deployment smoke script verifies compose startup;
- TODO and migration docs match the actual codebase.
