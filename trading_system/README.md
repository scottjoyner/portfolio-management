# Trading System (Coinbase Advanced Trade)

Production-oriented modular scaffold for a Coinbase-focused algorithmic trading and research platform with explicit risk gates, approvals, paper-first execution, onchain route-analysis support, and an agentic evaluation roadmap for position research and strategy approval.

## Highlights

- Modular runtime apps: API, worker, backtester, replay engine, and paper exchange runners.
- Risk engine with explicit mode gating and exchange trust state integration.
- Ops API, PostgreSQL model layer, Alembic wiring, and deployment assets.
- Coinbase Advanced Trade connector modules plus paper/shadow-first execution posture.
- Agentic evaluation roadmap for buy/sell/hold recommendations, fair-market-price bands, investment philosophy, and holding-period estimates.
- Strategy registry with broad catalog, replay/backtest utilities, and planned certification gates.
- Planned Plaid account-data integration for bank/brokerage holdings and transactions.
- Planned equity broker adapter layer for stock/ETF/options execution separate from Plaid.
- Onchain route analysis + approval packet generation path.
- Test suite spanning unit, integration, replay/sim, and performance-smoke checks.

## Repository layout

- `apps/`: runtime entrypoints for API, worker, paper exchange, backtester, and replay engine.
- `core/`, `risk/`, `execution`, `exchange/`, `portfolio/`: core trading subsystems.
- `market_data/`: candles, order book, trades, indicators, features, and storage services.
- `onchain/`: chain adapters, wallets, DEX/bridge/MEV, safety, simulation, and strategy modules.
- `storage/`: PostgreSQL, Redis, and Parquet-oriented storage layers.
- `tests/`: automated unit, integration, replay/sim, and performance-smoke suites.
- `docs/`: architecture, migration, operations, repo audit, agentic evaluation, and testing evidence.
- `deploy/`: production-style Docker Compose, systemd, environment, and bootstrap assets.

## Local setup

```bash
cd trading_system
pip install -e .[dev]
```

Optional local infra for API dependencies:

```bash
docker compose up -d postgres redis
```

## Run services

```bash
# API
uvicorn apps.api.main:app --reload --host 0.0.0.0 --port 8000

# Worker
python -m apps.worker.main

# Backtest demo
python -m apps.backtester.runner --config configs/backtest_demo.yaml

# Replay demo
python -m apps.replay_engine.runner --fixture apps/replay_engine/fixtures/maker_toxic_flow.jsonl
```

## Deployment

For production deployment, use the `trading_system/deploy/` assets:

- `trading_system/deploy/docker-compose.prod.yml` for a Docker stack.
- `trading_system/deploy/systemd/portfolio.service` as a sample systemd unit.
- `trading_system/deploy/.env.example` for runtime configuration.
- `trading_system/deploy/bootstrap.sh` to bootstrap a Python 3.12 virtual environment and install dependencies.

Copy `trading_system/deploy/.env.example` to `trading_system/deploy/.env`, then update settings and secrets before launching.

## Migration and implementation docs

- `docs/MIGRATION_GUIDE.md` — database migration workflow, safety gates, rollback posture, and validation checklist.
<<<<<<< HEAD
- `docs/AGENTIC_EVALUATION_PLAN.md` — agentic position evaluation, fair-market-price, strategy certification, approval, Plaid, broker, crypto, and onchain execution roadmap.
=======
>>>>>>> b5e23b51 (Added falcon updates)
- `PLAN.md` — current implementation plan and staged roadmap.
- `TODO.md` — current prioritized backlog.
- `docs/testing/TEST_PLAN.md` — test layers and canonical local commands.
- `docs/testing/TEST_RUN_RESULTS.md` — latest documented verification pass.

## Testing and checks

```bash
# Full local quality gate
make ci

# Or run individually
pytest -q
ruff check .
mypy .
```

## Safety defaults

- Live trading disabled by default.
- Live modes require explicit `LIVE_TRADING_ENABLED=true`.
- `TRADING_MODE=CANARY` requires non-zero `CANARY_ROLLOUT_PCT`.
- `QUEUE_MODEL` constrained to `simple`, `priority`, or `pro_rata`.
- Migration validation should run in paper mode with approvals required.
<<<<<<< HEAD
- Agentic evaluators may generate recommendations and approval packets, but may not execute trades directly.
- Plaid integrations are for account/holding/transaction data, not order execution.
=======
>>>>>>> b5e23b51 (Added falcon updates)

## Known limitations / next work

- A reviewed baseline Alembic revision should be committed and validated if `alembic/versions/` only contains package markers.
- DB-backed integration tests need to prove migrations, repository persistence, and restart behavior against a real Postgres database.
<<<<<<< HEAD
- Plaid account-data ingestion, canonical account ledger, and instrument master are not yet implemented.
- Fair-market-price snapshots, agentic recommendation outputs, and strategy certification gates are not yet implemented.
- WebSocket routes exist, but worker/paper/market-data producers still need full event publishing coverage.
- Coinbase live order placement should remain disabled until read-only sync, shadow-mode preview, reconciliation, and approval gates are proven.
- Equity broker execution requires a broker adapter layer separate from Plaid.
=======
- WebSocket routes exist, but worker/paper/market-data producers still need full event publishing coverage.
- Coinbase live order placement should remain disabled until read-only sync, shadow-mode preview, reconciliation, and approval gates are proven.
>>>>>>> b5e23b51 (Added falcon updates)
- In-memory WebSocket fanout should be replaced or augmented with Redis pub/sub before multi-worker production deployment.
