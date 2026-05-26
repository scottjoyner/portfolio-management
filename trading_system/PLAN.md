# Trading System — Enhancement Plan

This document outlines the gaps, priorities, and sequenced work needed to evolve the project from a well-architected scaffold into a functional, production-capable trading platform.

---

## Priority Tiers

| Tier | Label | Meaning |
|------|-------|---------|
| P0 | **Critical** | Blocks all real functionality; must fix before any live/paper usage |
| P1 | **High** | Core feature gaps that make the system non-functional outside of demo mode |
| P2 | **Medium** | Needed for production readiness, safety, and operational confidence |
| P3 | **Low** | Quality-of-life, performance, and completeness items |

---

## P0 — Critical (Blocking)

### P0.1 Docker image build path
- **Problem**: `docker-compose.yml` references `build: .` but there is no `Dockerfile`.
- **Work**: Write a production-grade `Dockerfile` (multi-stage, non-root user, healthcheck).
- **Files**: new `Dockerfile`, update `docker-compose.yml`.

### P0.2 Alembic migration scaffolding
- **Problem**: Migration versions exist but `alembic/env.py` is missing; migration commands are broken.
- **Work**: Create `alembic/env.py`, wire `alembic.ini`, ensure `alembic upgrade head` works.
- **Files**: new `alembic/env.py`, new `alembic.ini` (or inline in `pyproject.toml`).

### P0.3 Database model coverage
- **Problem**: Only a single `Order` table exists (`storage/postgres/models.py`). No models for portfolios, strategies, fills, capital buckets, approvals, or audit log.
- **Work**: Define all core domain tables (portfolios, strategies, orders, fills, capital_buckets, approvals, audit_events, feed_health, settings). Create Alembic revisions.
- **Files**: `storage/postgres/models.py`, new Alembic revisions.

### P0.4 Ops API state persistence
- **Problem**: `InMemoryOpsStore` in `apps/api/ops_layer.py` uses in-memory dicts; restarts lose all previews, orders, fills, audit events.
- **Work**: Back the store with PostgreSQL (via SQLAlchemy models) and optionally Redis for cache. Wire repository pattern.
- **Files**: `apps/api/ops_layer.py`, `storage/postgres/`.

---

## P1 — High (Core Functionality)

### P1.1 Exchange connectivity (Coinbase)
- **Problem**: All `exchange/coinbase/` sub-modules are empty `__init__.py` stubs: `accounts`, `auth`, `execution`, `normalizers`, `portfolio`, `rest/client`, `websocket`.
- **Work**: Implement the Coinbase Advanced Trade REST and WebSocket clients. Add authentication (JWT/JWS signing), account management, order execution, market data feeds, and portfolio sync.
- **Files**: `exchange/coinbase/`.

### P1.2 Worker implementation
- **Problem**: `apps/worker/main.py` is a 10-line heartbeat loop with no real workload.
- **Work**: Implement a proper worker that consumes market data, evaluates strategies, calculates risk, processes orders, and persists state. Add lifecycle management (startup, graceful shutdown, error handling).
- **Files**: `apps/worker/main.py`, new `apps/worker/` modules.

### P1.3 Paper exchange engine
- **Problem**: `apps/paper_exchange/runner.py` only prints a startup message.
- **Work**: Build a realistic paper trading engine that simulates fills, queue position, slippage, fees, and market impact. Wire it to the strategy and risk systems.
- **Files**: `apps/paper_exchange/`.

### P1.4 Market data feeds
- **Problem**: `market_data/` has 6 empty stubs (`candles`, `features`, `indicators`, `orderbook`, `storage`, `trades`). Only `microstructure/` has code.
- **Work**: Implement candle aggregation, order book management, trade recording, feature computation, and storage backends. Wire real-time feeds (WebSocket → Pub/Sub → consumers).
- **Files**: `market_data/`.

### P1.5 Onchain RPC connectivity
- **Problem**: All `onchain/chains/` directories are empty stubs (base, ethereum, evm_generic, solana_research). Simulation is deterministic/mock only.
- **Work**: Implement chain adapters with real RPC connectivity (Ethereum, Base). Wire real onchain data: pool state, token metadata, prices, events.
- **Files**: `onchain/chains/`, `onchain/data/pools/`, `onchain/data/prices/`, `onchain/data/tokens/`, `onchain/data/events/`.

### P1.6 Wallet infrastructure
- **Problem**: All wallet modules are stubs: `allowances/`, `gas_policy/`, `nonce_manager/`, `server_wallet/`, `signing/`, `smart_wallet/`.
- **Work**: Implement server wallet with key management (hardware/HSM-ready), transaction signing, nonce management, gas estimation, and allowance management.
- **Files**: `onchain/wallets/`.

---

## P2 — Medium (Production Readiness)

### P2.1 CI/CD pipeline
- **Problem**: No CI workflow exists (no `.github/workflows`).
- **Work**: Create GitHub Actions workflow (or equivalent) for lint, typecheck, tests, build, and optional deploy.
- **Files**: new `.github/workflows/ci.yml`.

### P2.2 API operational hardening
- **Problem**: Health endpoint is liveness-only; no readiness checks for DB/Redis/exchange. No rate limiting, no structured error responses, no OpenAPI extensions.
- **Work**: Add readiness probe endpoints, standardize error responses, add rate limiting middleware, add request ID tracing, add OpenAPI tags/descriptions.
- **Files**: `apps/api/main.py`.

### P2.3 WebSocket transport
- **Problem**: Despite realtime-oriented models (feed health, fills, strategy outcomes), there is no WebSocket transport. Clients must poll.
- **Work**: Implement WebSocket endpoint(s) for realtime dashboard updates, order fills, and market data.
- **Files**: `apps/api/` (new WebSocket routes).

### P2.4 Risk submodule implementations
- **Problem**: 7 risk submodules are empty stubs: `approvals`, `compliance`, `drawdown`, `kill_switch`, `limits`, `sizing`, `slippage`.
- **Work**: Implement each module to make risk engine fully operational. Add approval workflows, compliance checks, drawdown monitoring, kill switch automation, position limits, position sizing, and slippage controls.
- **Files**: `risk/`.

### P2.5 Execution infrastructure
- **Problem**: 4 execution submodules are empty stubs: `order_manager`, `router`, `smart_execution`, `trade_lifecycle`.
- **Work**: Implement order management (lifecycle, status tracking), execution router (venue selection), smart execution (iceberg, TWAP, pegged), and trade lifecycle (from signal to settlement).
- **Files**: `execution/`.

### P2.6 Notification system
- **Problem**: All notification modules are empty stubs: `email`, `push`, `templates`, `webhook`.
- **Work**: Implement email alerts, push notifications, webhook dispatcher, and notification templates.
- **Files**: `notifications/`.

### P2.7 Strategy persistence & lifecycle
- **Problem**: Strategies can be loaded via registry, but there's no persistence for strategy configs, run state, or historical performance.
- **Work**: Add database-backed strategy configuration, runtime state persistence, and a lifecycle manager (start/stop/pause/resume).
- **Files**: `strategies/`, `storage/postgres/models.py`.

### P2.8 Capital & portfolio persistence
- **Problem**: Portfolio management is in-memory only in the Ops API. `portfolio/ objectives`, `performance`, `rebalance` are stubs.
- **Work**: Implement database-backed portfolio management with capital allocation persistence, P&L tracking, and rebalance scheduling.
- **Files**: `portfolio/`, `storage/postgres/models.py`.

### P2.9 Monitoring & observability
- **Problem**: No structured metrics, tracing, or logging beyond basic `get_logger`. No alerting rules.
- **Work**: Add Prometheus metrics endpoints, structured logging with correlation IDs, optional OpenTelemetry tracing, and health/readiness probes.
- **Files**: `core/logging/`, `apps/api/main.py`.

### P2.10 Secrets management
- **Problem**: `.env.example` shows plaintext `COINBASE_API_KEY`/`SECRET`. No vault/encryption strategy.
- **Work**: Document/implement secrets strategy (env vars for dev, vault/HSM for production). Add `.env` to `.gitignore` (verify). Add warning if production env has unset secrets.
- **Files**: `.env.example`, `core/config/settings.py`.

---

## P3 — Low (Quality & Completeness)

### P3.1 Test coverage expansion
- **Problem**: No tests for Ops API endpoints (only health check integration test). No stress, fuzz, or E2E tests.
- **Work**: Add contract tests for all Ops API response models. Add stress tests for risk engine and path analyzer. Add E2E test that exercises a full strategy → execution → fill cycle (in paper mode).
- **Files**: `tests/`.

### P3.2 Onchain bridge & MEV modules
- **Problem**: `onchain/bridges/` (4 stubs) and `onchain/mev_protection/` (4 stubs) are unimplemented.
- **Work**: Implement bridge adapters (quoting, risk, settlement) and MEV protection (bundle building, private tx submission, sandwich/MEV risk detection).
- **Files**: `onchain/bridges/`, `onchain/mev_protection/`.

### P3.3 DEX adapter layer
- **Problem**: `onchain/dex/adapters/`, `fee_harvest/`, `lp_manager/`, `pool_discovery/`, `quoter/`, `routers/`, `route_solver/`, `swap_execution/` are all stubs.
- **Work**: Implement DEX adapters with real pool discovery, quoting, routing, swap execution, fee harvesting, and LP management.
- **Files**: `onchain/dex/`.

### P3.4 Security module expansion
- **Problem**: `onchain/security/incident_response/` and `wallet_safety/` are stubs.
- **Work**: Implement incident response automation (pause, unwind, alert) and wallet-level safety checks (allowlist, rate limits, anomaly detection).
- **Files**: `onchain/security/`.

### P3.5 Contract infrastructure
- **Problem**: 6 contract submodules are stubs: `abi_store`, `codehash`, `proxy_detection`, `risk_scoring`, `upgradeability`, `verification`.
- **Work**: Implement ABI management, code hash verification, proxy detection, contract risk scoring, upgrade safety checks, and contract verification.
- **Files**: `onchain/contracts/`.

### P3.6 Analytics & reporting
- **Problem**: `analytics/attribution/`, `reports/`, `tearsheets/` are stubs.
- **Work**: Implement performance attribution (Brinson, factor), report generation, and interactive tearsheets.
- **Files**: `analytics/`.

### P3.7 Storage backends
- **Problem**: `storage/parquet/` and `storage/redis/` are stubs.
- **Work**: Implement Parquet storage for historical data and Redis caching/rate-limiting infrastructure.
- **Files**: `storage/`.

### P3.8 Core utility & events
- **Problem**: `core/events/`, `core/security/`, `core/utils/` are stubs.
- **Work**: Implement event bus/pub-sub, security utilities (key derivation, encryption helpers), and general utilities (time helpers, math, retry).
- **Files**: `core/`.

### P3.9 Replay scenario library
- **Problem**: Only one replay fixture exists (`maker_toxic_flow.jsonl`). No stress scenario library.
- **Work**: Create a library of replay scenarios covering common market regimes (crash, rally, liquidations, low liquidity, volatility events).
- **Files**: `replay_scenarios/`, `apps/replay_engine/fixtures/`.

### P3.10 Deployment manifests
- **Problem**: No Kubernetes manifests or infrastructure-as-code for production deployment.
- **Work**: (Optional) Create Helm charts or Kustomize overlays for production deployment.
- **Files**: new `deploy/` directory.

---

## Suggested Sequencing

### Phase 1 — Foundation (P0)
1. **P0.1** Dockerfile
2. **P0.2** Alembic scaffolding
3. **P0.3** Database models
4. **P0.4** Ops API persistence

### Phase 2 — Trading Core (P1)
5. **P1.1** Exchange connectivity
6. **P1.3** Paper exchange engine
7. **P1.4** Market data feeds
8. **P1.2** Worker implementation

### Phase 3 — Onchain (P1)
9. **P1.5** Onchain RPC connectivity
10. **P1.6** Wallet infrastructure

### Phase 4 — Production Hardening (P2)
11. **P2.1** CI/CD pipeline
12. **P2.2** API hardening
13. **P2.3** WebSocket transport
14. **P2.4** Risk submodules
15. **P2.5** Execution infrastructure
16. **P2.6** Notifications
17. **P2.7** Strategy persistence
18. **P2.8** Portfolio persistence
19. **P2.9** Monitoring
20. **P2.10** Secrets

### Phase 5 — Completeness (P3)
21. **P3.1** – **P3.10** Remaining modules

---

## Current Project Stats

| Metric | Value |
|--------|-------|
| Python modules with real code | ~60 |
| Empty `__init__.py` stubs | ~50 |
| Concrete strategies | 14 |
| Catalog strategies | 100 |
| Tests (passing) | 50+ |
| Lint/typecheck | Passing |
| CI pipeline | ❌ Missing |
| Docker image | ❌ Missing |
| DB models | 1 / ~20 needed |
| Exchange connectivity | ❌ All stubs |
| Onchain RPC | ❌ All stubs |
| Worker implementation | ❌ Stub (heartbeat only) |
| Paper exchange | ❌ Stub (print only) |
