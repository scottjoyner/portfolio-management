# Interface Map

## Backend API contracts (`apps/api/main.py`)
- `GET /health` -> `{status}` liveness response.
- `GET /mode` -> `{mode}` from `Settings.trading_mode`.
- `GET /strategies/catalog` -> strategy metadata list.
- `POST /risk/mode/{mode}/enable` -> enabled mode set.
- `GET /reconciliation/trust-score` -> trust score + snapshot dictionary.
- `POST /risk/evaluate` -> allow/deny decision for `OrderIntent`.
- Onchain routes:
  - `/onchain/contracts/register`
  - `/onchain/tokens/register`
  - `/onchain/opportunities/rank`
  - `/onchain/path/analyze`
  - `/onchain/approvals/build`
  - `/onchain/hedge/coinbase`
  - `/onchain/profit/sweep`

## Operator API contracts (`apps/api/ops_layer.py`, prefix `/ops`)
- Dashboard and feed endpoints:
  - `GET /ops/dashboard/snapshot`
  - `GET /ops/dashboard/delta`
  - `GET /ops/feeds/health`
- Portfolio + treasury endpoints:
  - `GET /ops/portfolios`
  - `GET /ops/portfolios/{portfolio_id}`
  - `POST /ops/treasury/preview`
  - `POST /ops/treasury/execute`
- Liquidity/order endpoints:
  - `GET /ops/liquidity/map`
  - `GET /ops/liquidity/recommendations`
  - `POST /ops/orders/preview`
  - `POST /ops/orders/submit`
  - `GET /ops/orders/open`
  - `POST /ops/orders/{order_id}/cancel`
  - `GET /ops/fills`
- Strategy/ops UI endpoints:
  - `POST /ops/strategies/backtest/start`
  - `POST /ops/strategies/{strategy_id}/start`
  - `GET /ops/strategies/outcomes/realtime`
  - `GET /ops/ui/theme`
  - `GET /ops/ui/labels`
  - `GET /ops/risk/summary`
  - `GET /ops/approvals`
  - `GET /ops/alerts`
  - `GET /ops/incidents`
  - `GET /ops/audit`

## Websocket/realtime contracts
- No websocket endpoints are exposed in FastAPI entrypoints during this pass; realtime semantics are represented as pollable typed responses in ops layer.

## Internal service boundaries
- Risk policy decisions encapsulated by `risk.engine.RiskEngine`.
- Exchange trust set via reconciliation service and consumed by risk engine.
- Onchain path analysis boundary: registry/safety engines + simulator -> plan object.
- Strategy discovery boundary: `strategies.registry.registry.load_strategies()` used by API and backtester.

## Known contract mismatches / drift risks
- `/mode` currently returns enum object value via JSON serialization; consumers should treat as string, but this is not explicitly versioned.
- Ops read models are richer than persisted state (none), so refresh/restart loses operator context.
- `docker-compose.yml` declares API image build but repository lacks a Dockerfile, making compose contract incomplete.
- Alembic migration path is incomplete (`alembic/env.py` absent), so DB migration contract is partially wired.
