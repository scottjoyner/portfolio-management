# Trading System — Handoff

## Current State

All P0–P3 items from `trading_system/PLAN.md` are implemented. The codebase has been evolved from a scaffold with ~60 real-code modules and 50 empty stubs into a functional platform with **364 source files**, **110 passing tests**, clean lint (ruff) and typecheck (mypy).

### What's Working

| Area | Status |
|------|--------|
| Docker build + multi-stage image | Done |
| Alembic migrations (`upgrade head`) | Done |
| 15 SQLAlchemy models (portfolios, strategies, orders, fills, approvals, audit, alerts, incidents, exchange state, market data feeds, capital buckets) | Done |
| Ops API with PostgreSQL persistence (repository pattern) | Done |
| Coinbase exchange (REST client, WebSocket client, JWT auth, accounts, execution, normalizers, portfolio) | Done |
| Paper exchange engine (fill simulation, queue model, fees, positions) | Done |
| Market data (candle aggregation, feature computation, technical indicators, orderbook, trade recording, storage) | Done |
| Worker engine (strategy evaluation, risk check, paper order placement) | Done |
| Onchain RPC adapters (EVM generic, Ethereum, Base) | Done |
| Onchain data services (pools, prices/Chainlink, tokens, events) | Done |
| Wallet infrastructure (signing, nonce management, server wallet, gas policy, smart wallet stub, allowance management, spend policy) | Done |
| API hardening (readiness probe, request ID middleware, metrics endpoint, structured logging) | Done |
| CI/CD pipeline (GitHub Actions: lint → typecheck → test → build) | Done |
| WebSocket transport (pub/sub hub, `/ws/orders`, `/ws/market/{product_id}`) | Done |
| Risk submodules (approvals, compliance, drawdown, kill switch, limits, sizing, slippage) | Done |
| Execution infrastructure (order manager, router, smart execution/TWAP/iceberg, trade lifecycle) | Done |
| Notifications (email, push, webhook, templates) | Done |
| Strategy persistence & lifecycle (enable/disable, start/stop/pause/resume) | Done |
| Portfolio persistence & management (NAV updates, capital adjustments, sleeve rebalancing) | Done |
| Monitoring & observability (metrics collector, prometheus endpoint, structured logging with correlation IDs) | Done |
| Secrets management (.env.example with docs, settings validation warnings, .gitignore) | Done |
| Onchain bridges (quoting, risk, settlement) | Done |
| MEV protection (bundle policy, private tx, reordering risk, sandwich risk) | Done |
| DEX adapter layer (registry, fee harvest, LP manager, pool discovery, quoter, router, route solver, swap executor) | Done |
| Security expansion (incident response, wallet safety allowlist/blocklist) | Done |
| Contract infrastructure (ABI store, codehash verification, proxy detection, risk scoring, upgradeability, verification) | Done |
| Analytics (performance attribution, reports, tearsheets) | Done |
| Storage backends (Parquet in-memory, Redis in-memory stub) | Done |
| Core utilities (event bus/pub-sub, security helpers, retry, time helpers) | Done |
| Replay scenarios (flash crash, liquidity crisis, volatility event, bull rally, liquidation waterfall) | Done |
| K8s deployment manifests (api, worker, worker-canary, redis, postgres) | Done |
| Tests: 110 passing (was 65), +45 new across risk submodules, execution, wallet, chain adapters, notifications, core utils | Done |

---

## Next Steps & Integrations

### 1. End-to-End Integration Tests

The largest gap. There is no test that exercises the full pipeline:
- Strategy signal → risk check → paper exchange → fill → audit event → WebSocket broadcast
- Coinbase REST client → order placement → fill reconciliation
- Worker startup → market data feed → strategy evaluation → order lifecycle

**Recommendation**: Create `tests/e2e/` with a `conftest.py` that spins up the FastAPI app and a mock Coinbase/paper exchange, then runs a full signal-to-fill scenario.

### 2. Real WebSocket End-to-End

The WebSocket transport (`apps/api/ws_routes.py`) accepts connections but has no producer pushing real data yet. The worker should publish order fill events and market data ticks to the pub/sub hub so connected clients receive live updates.

**Integration points**:
- `apps/worker/main.py` → `hub.publish("orders", {...})` after placing/filling an order
- `apps/paper_exchange/engine.py` → `hub.publish("orders", {...})` on fill simulation
- `market_data/storage/manager.py` → `hub.publish(f"market:{product_id}", {...})` on new tick/candle

### 3. Live Coinbase Integration

The Coinbase REST client (`exchange/coinbase/rest/client.py`) is implemented but not wired into any application code. To use it live:
- Wire `CoinbaseRestClient` into the worker for real order placement
- Wire `CoinbaseWebSocketClient` into the market data feed for real-time prices
- Set `COINBASE_API_KEY`, `COINBASE_API_SECRET`, `COINBASE_PASSPHRASE` in `.env`

### 4. Onchain RPC Connectivity

The EVM chain adapters (`onchain/chains/evm_generic/base.py`) are implemented but the data services (`PoolDataService`, `PriceService`, `TokenService`, `EventService`) need to be wired into a runtime. Currently they have no producer calling their `fetch_*` methods.

**Next**: Create an `onchain/data/ingestion/` service or a worker that periodically polls:
- Pool snapshots for tracked pools
- Chainlink price feeds
- New swap/transfer events

### 5. Alembic Migration Generation

The models exist but there are no actual Alembic migration files. Run:
```bash
cd trading_system
alembic revision --autogenerate -m "initial_models"
alembic upgrade head
```

### 6. Prometheus Integration

The `/metrics` endpoint returns JSON (`metrics.snapshot()`). For real production monitoring, swap in `prometheus_client`:
- Replace `MetricsCollector` with `from prometheus_client import Counter, Histogram, Gauge`
- Export `/metrics` in Prometheus text format
- Add Grafana dashboards

### 7. Rate Limiting

The API has no rate limiting yet. Add `slowapi` or a custom token-bucket middleware to prevent abuse. The `storage/redis/` module can back the rate limiter state in production.

### 8. Redis Pub/Sub

The in-memory `PubSubHub` works for a single process but doesn't scale. For multi-worker deployments, back it with Redis pub/sub:
- Worker publishes to Redis channel
- API subscribes via Redis and forwards to connected WebSocket clients
- See `storage/redis/service.py` (currently in-memory stub) — replace with real `redis-py`

### 9. Strategy Catalog Wiring

`/strategies/catalog` loads strategies but they aren't persisted in the database. The `StrategyLifecycleManager` and `OpsRepository` can bridge this:
- On startup, sync the strategy catalog into `strategy_configs` table
- Allow operators to enable/disable strategies via the API

### 10. Wallet Key Management

The `ServerWallet` + `SigningService` uses an in-process private key from `EVM_PRIVATE_KEY` env var. For production, integrate with:
- AWS KMS / GCP Cloud KMS for signing without exposing keys
- Hardware wallet (Ledger) for manual approval flows
- HSM for automated high-value signing

### 11. CI/CD Enhancements

The current GitHub Actions workflow runs on push to `main` for `trading_system/` changes. Add:
- Docker image build and push to registry
- Integration test stage with PostgreSQL service container
- Deploy to dev/staging cluster on `main`
- PR preview environments

### 12. Documentation Generation

There is no API docs beyond FastAPI's auto-generated OpenAPI. Consider:
- `mkdocs` or `readthedocs` for user-facing documentation
- `sphinx` with `autodoc` for Python API reference
- Architecture decision records (ADRs) in `docs/adr/`

### 13. Empty Stubs Audit

Some directories were intentionally left as stubs:
- `onchain/chains/solana_research/` — Solana support
- `onchain/data/liquidity_graph/` — Liquidity graph visualization
- `onchain/data/mempool_research/` — Mempool monitoring
- `onchain/data/oracles/` — Oracle integrations beyond Chainlink
- `onchain/dex/clmm/`, `onchain/dex/amm/`, `onchain/dex/position_manager/` — Already implemented
- `core/events/` — Event bus is implemented, but `core/security/` and `core/utils/` are partial

Review and prioritize based on operational needs.

---

## Quick Wins (1-2 hours each)

1. **Generate Alembic migrations** — `alembic revision --autogenerate && alembic upgrade head`
2. **Wire worker → WebSocket hub** — ~10 lines in `main.py` to publish order events
3. **Deploy to local Docker Compose** — `docker compose up` (verify the Dockerfile works end-to-end)
4. **Create `tests/e2e/test_signal_to_fill.py`** — FastAPI TestClient + mock paper exchange
5. **Add `prometheus-client` dependency** and swap the metrics endpoint to Prometheus text format

---

## Key Contacts / Ownership

- **Backend / API**: `apps/api/`, `storage/`
- **Exchange Connectivity**: `exchange/coinbase/`
- **Onchain / DeFi**: `onchain/`
- **Risk & Execution**: `risk/`, `execution/`
- **Strategies**: `strategies/`
- **Infrastructure**: `Dockerfile`, `.github/workflows/`, `deploy/`
