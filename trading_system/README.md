# Trading System (Coinbase Advanced Trade)

Production-oriented modular scaffold for a Coinbase-focused algorithmic trading and research platform with explicit risk and approvals.

## Highlights
- Modular runtime apps: API, worker, backtester, replay engine, and paper exchange runners.
- Risk engine with explicit mode gating and exchange trust state integration.
- Onchain route analysis + approval packet generation path.
- Strategy registry with broad catalog and replay/backtest utilities.
- Test suite spanning unit, integration, replay/sim, and performance-smoke checks.

## Repository layout
- `apps/`: runtime entrypoints.
- `core/`, `risk/`, `execution/`, `exchange/`, `portfolio/`: core trading subsystems.
- `onchain/`: onchain simulation/security/strategy modules.
- `tests/`: automated test suites.
- `docs/`: architecture, operations, and audit/testing evidence.

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

## Testing and checks
```bash
# Full local quality gate
make ci

# Or run individually
pytest -q
ruff check .
mypy .
```

See `docs/testing/TEST_PLAN.md` and `docs/testing/TEST_RUN_RESULTS.md` for tested commands, scope, and caveats.

## Safety defaults
- Live trading disabled by default.
- Live modes require explicit `LIVE_TRADING_ENABLED=true`.
- `TRADING_MODE=CANARY` requires non-zero `CANARY_ROLLOUT_PCT`.
- `QUEUE_MODEL` constrained to `simple`, `priority`, or `pro_rata`.

## Known limitations
- Ops API state is in-memory only (non-persistent).
- No repository CI workflow is currently defined.
- Alembic migration scaffolding is partial (`alembic/env.py` missing).
