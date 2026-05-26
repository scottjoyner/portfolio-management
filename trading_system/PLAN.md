# Trading System — Implementation Plan

This plan reflects the current repository state after the May 26, 2026 implementation/demo pass. The project is no longer just a scaffold: the core service layout, Docker build assets, CI quality workflow, SQLAlchemy model set, paper exchange, Coinbase connector modules, WebSocket routes, Prometheus metrics, and broad risk/execution/onchain modules now exist.

The next implementation pass should focus less on adding new directories and more on proving real workflows end to end, hardening migrations, removing drift, and staging the system safely.

## Current state snapshot

| Area | Current state | Implementation posture |
|---|---|---|
| Packaging | `pyproject.toml` defines Python 3.12 package, dependencies, and dev extras | Present; keep dependency additions intentional |
| Local commands | `Makefile` has install, lint, typecheck, test, ci, api, worker, backtest, paper demo targets | Present; use `make ci` as local gate |
| CI | GitHub Actions runs lint, mypy, pytest, and wheel build for `trading_system/**` changes | Present; add DB service containers next |
| Docker | `Dockerfile`, local compose, and production deploy compose/systemd assets exist | Present; needs smoke validation and clean env handling |
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
3. **Prefer end-to-end proof over more scaffolding**: the next valuable work is connecting existing modules into tested runtime paths.
4. **Treat database migration as production infrastructure**: committed Alembic revisions, backup/restore proof, and CI migration checks are mandatory before staging.
5. **Keep generated artifacts out of source control** unless they are intentionally curated fixtures or evidence.

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

### 1.2 Worker-to-WebSocket event publishing

**Goal**: make realtime endpoints useful with actual producers.

**Work**:
- Publish order lifecycle events to `orders` channel.
- Publish market ticks/candles to `market:{product_id}` channel.
- Publish fill events from paper exchange.
- Add WebSocket integration tests that subscribe and assert received events.

**Acceptance criteria**:
- connected clients receive order and market events during paper-mode e2e test;
- disconnected clients do not break publisher flow.

### 1.3 Strategy lifecycle wiring

**Goal**: bridge strategy catalog, persisted configs, and runtime state.

**Work**:
- Sync catalog entries into `strategy_configs` at startup or via explicit command.
- Persist enable/disable/start/stop/pause/resume state.
- Ensure strategy metadata includes supported modes, risk hints, capital bucket, and backtest/replay flags.

**Acceptance criteria**:
- `/strategies/catalog` and ops strategy endpoints agree on strategy IDs and capabilities;
- disabled strategies cannot emit runtime orders.

## Phase 2 — Coinbase staging path

### 2.1 Read-only Coinbase sync

**Goal**: validate credentials and remote state without placing orders.

**Work**:
- Add command/API path to fetch accounts, portfolios, products, and open orders.
- Normalize remote objects into internal models.
- Persist exchange-state snapshots and trust-score evidence.

**Acceptance criteria**:
- read-only sync works with credentials;
- missing/invalid credentials fail closed;
- no order placement path is reachable with `LIVE_TRADING_ENABLED=false`.

### 2.2 Shadow-mode order preview

**Goal**: prove live connector payload generation without sending orders.

**Work**:
- Generate Coinbase order payloads from internal order intents.
- Run risk, sizing, slippage, and approval gates.
- Produce an approval packet and audit event instead of submitting.

**Acceptance criteria**:
- operators can inspect exact would-submit payloads;
- all shadow/live attempts are audit logged;
- live submit code path requires explicit live gate and approval state.

### 2.3 Reconciliation loop

**Goal**: prevent local/remote state divergence.

**Work**:
- Poll or stream remote open orders/fills.
- Compare remote state against local orders/fills.
- Degrade trust score on unknown fills, duplicate events, stale sequence gaps, or mismatched status.

**Acceptance criteria**:
- mismatches block live execution;
- reconciliation summary is visible through API and audit log.

## Phase 3 — Onchain runtime path

### 3.1 RPC ingestion service

**Goal**: turn implemented onchain adapters into a runtime feed.

**Work**:
- Add a poller for Ethereum/Base pools, token metadata, price feeds, and swap/transfer events.
- Persist feed health and snapshots.
- Add retry/backoff and stale-data detection.

**Acceptance criteria**:
- onchain data can be fetched in paper/shadow mode without signing transactions;
- RPC failures are visible in feed-health and do not trigger execution.

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

### 4.2 Deployment smoke scripts

**Goal**: make Docker/systemd deployment repeatable.

**Work**:
- Add `scripts/smoke_deploy.sh` or equivalent.
- Validate compose build, API startup, `/health`, `/ready`, `/metrics`, migration head, and basic ops endpoint.
- Add systemd checklist for `/opt/portfolio-management/trading_system` deployment.

**Acceptance criteria**:
- local compose deployment can be verified with one script;
- deployment docs match actual commands.

### 4.3 Observability runbooks

**Goal**: turn metrics/logs into operator actions.

**Work**:
- Document key metrics, alert thresholds, and remediation paths.
- Add incident-response checklist for kill switch, exchange trust degradation, DB failure, Redis failure, and wallet safety event.
- Add sample Grafana dashboard or dashboard spec.

**Acceptance criteria**:
- each critical alert has a runbook and owner action;
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
- signal-to-fill paper e2e test passes;
- WebSocket clients receive real worker/paper events;
- Coinbase read-only sync works and live order placement remains gated;
- deployment smoke script verifies compose startup;
- TODO and migration docs match the actual codebase.
