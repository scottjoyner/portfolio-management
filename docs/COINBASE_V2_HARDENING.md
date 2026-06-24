# Coinbase V2 Hardening

This doc summarizes the current hardened Coinbase trader path.

## Runtime

- Entry point: `python3 coinbase/src/run_trader_v2.py`
- Paper mode is the default.
- `--health-port <port>` starts a small `/health` endpoint.

Example:

```bash
python3 coinbase/src/run_trader_v2.py --mode paper --health-port 9090
```

## Hardening

- Startup validation checks products, equity, poll interval, and max positions.
- `SIGINT` / `SIGTERM` trigger clean shutdown.
- Strategy ranking state is loaded on startup and saved on shutdown.
- `_tick()` has stage-level guards so one broken source does not stop the loop.
- Price history is warmed so regime detection is not blocked by an empty buffer.
- Paper mode can run without Coinbase credentials; the feed uses `CBClient` + CLI/public-data fallback.
- Live / approval mode fails fast at startup if the Coinbase CLI is unavailable.

## Current Coinbase v2 Modules

- `product_rotation.py` - top-N momentum rotation
- `adaptive_mode.py` - SCALP / SWING / TREND / HOLD switching
- `dual_mm.py` - inventory-skewed market making
- `ranking.py` - rolling strategy ranking + persistence
- `news_risk.py` - knowledge-graph based risk adjustment
- `market_condition.py` - archetype-based opportunity gating
- `graph/` - CoinGecko sync helpers, `GraphSignalStrategy`, and graph-based overlays
- `portfolio_optimizer.py` - cached Neo4j graph scores bias strategy-signal sizing and rebalance candidate selection
- `trading_system/ui/dashboard_server.py` - `/market/universe` and `/execution/status` include graph summaries/overlays

## CoinGecko Sync

- Script: `python3 -m coinbase.src.graph.sync_coingecko_universe`
- Default source: cached top-5000 markets JSON under `coinbase/data/alt/json/`
- Optional live refresh: `--fetch-live`
- Cached asset metadata is ingested when `coingecko_assets_meta.json` is present

## Notes

- `Ticker.price` is the canonical price field.
- `TickerCache.get_ticker()` is the canonical cache accessor.
- `PollingFeed(cb_client=None, ...)` is inert in harnesses; the trader itself now wires a `CBClient` and subscribes its configured products.
- Ranking state lives at `data/ranking_state.json`.
