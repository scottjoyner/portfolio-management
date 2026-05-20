# Subsystem Inventory

| Subsystem | Status | Key files | Dependencies | Noted gaps |
|---|---|---|---|---|
| API control plane | **partial** | `apps/api/main.py`, `apps/api/ops_layer.py` | FastAPI, Pydantic | Mostly in-memory state; no persistence wiring for ops routes. |
| Config/bootstrap | **fragile -> improved** | `core/config/settings.py` | Env vars, Pydantic | Boolean parsing and safety validation were underspecified before this pass. |
| Risk engine | **solid** | `risk/engine.py`, `tests/unit/test_risk_engine.py` | domain models | Good unit coverage, but no external integration guardrails. |
| Exchange reconciliation | **partial** | `exchange/coinbase/reconciliation/service.py` | risk engine trust score | Snapshot lifecycle is in-memory only. |
| Portfolio capital buckets | **partial** | `portfolio/capital_buckets/service.py` | ops APIs | No durable audit store behind transfer previews/execution. |
| Execution & maker engine | **solid** | `execution/maker_engine/engine.py`, replay tests | queue model, market data features | Useful tests; limited failure observability surface. |
| Onchain simulation/security | **partial** | `onchain/simulation/*`, `onchain/security/*` | contract/token registries | Heavy scaffolding; many modules are stubs (`__init__.py` only). |
| Replay/backtest | **solid** | `apps/backtester/runner.py`, `apps/replay_engine/runner.py`, `tests/sim/*` | analytics + strategies registry | Uses synthetic assumptions; realism documented but limited fixture diversity. |
| Storage/migrations | **fragile** | `storage/*`, `alembic/versions/0001_initial.py` | SQLAlchemy/Alembic | `alembic/env.py` missing, so migrations are not runnable as-is. |
| CI/tooling | **fragile -> improved docs** | `Makefile`, `pyproject.toml` | pytest/ruff/mypy | No `.github/workflows` present; local workflow had typecheck footguns. |
