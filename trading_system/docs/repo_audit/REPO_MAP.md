# Repository Map

## Top-level architecture summary
- **Core domain + config**: `core/` contains shared models, structured logging helpers, and environment settings bootstrap used by app entry points.
- **Runtime apps**: `apps/api`, `apps/worker`, `apps/backtester`, `apps/replay_engine`, and `apps/paper_exchange` are operational entry points.
- **Trading engines**: `execution/`, `risk/`, `portfolio/`, `strategies/`, `exchange/coinbase/`, and `onchain/` implement strategy, routing, risk, execution, and settlement logic.
- **Data + research**: `market_data/`, `analytics/`, `research/`, and `storage/` host feature generation, metrics, and persistence adapters.
- **Operations assets**: `docs/`, `examples/`, `configs/`, and `scripts/` contain operator docs and sample artifacts.

## Entrypoints
- API server: `uvicorn apps.api.main:app`.
- Worker heartbeat loop: `python -m apps.worker.main`.
- Backtester: `python -m apps.backtester.runner --config ...`.
- Replay engine: `python -m apps.replay_engine.runner --fixture ...`.
- Paper exchange runner: `python -m apps.paper_exchange.runner --config ...`.

## Runtime dependencies
- Python 3.12 project using FastAPI, Pydantic v2, SQLAlchemy/Alembic, Redis, NumPy/Pandas/Polars.
- Docker compose provisions Postgres + Redis + API service.
- Makefile wraps install/lint/test + app commands.

## Important data flows
1. **API control plane**: HTTP requests enter `apps/api/main.py`, call risk engine / reconciler / onchain path analyzers, and return read-model payloads.
2. **Strategy/backtest path**: strategies loaded via `strategies.registry.registry`, evaluated in backtester, and written as artifacts + experiment tracker records.
3. **Maker replay path**: replay fixture feeds microstructure estimator + maker quote engine, emitting operational stats.
4. **Onchain approval path**: route analysis (`onchain/simulation/path_simulator/analyzer.py`) -> approval packet builder -> API response.

## Critical stateful services
- In-memory risk state in `risk.engine.RiskEngine`.
- Reconciliation snapshot state in `exchange.coinbase.reconciliation.service.ExchangeStateReconciler`.
- In-memory operator view state in `apps/api/ops_layer.py` (`InMemoryOpsStore`).
- Profit capture ledger state in `onchain/strategies/treasury/profit_sweep.py`.
