# System Gap Analysis & Roadmap

> Status: as of 2026-07-15. Scope: the trading pipeline end-to-end (data →
> signals → confidence → optimization → approval → execution → brackets →
> persistence → UI), with a focus on **data durability for backtesting** and
> **UI functional completeness**.

---

## 1. Current State (what works today)

### Pipeline (verified)
- **Signal engines**: 74 strategies (50 Rust + external data) in `strategy_engine.py`; 25-strategy `SignalAggregator.scan_universe()`; graph-alpha-bot (~30). All wired into `portfolio_optimizer.py` (10 detection dimensions).
- **Confidence**: `ConfidenceMatrix` (12 independence groups) + `ConfidenceEngine` (8 modifiers). New `order_flow` / `onchain` / `derivatives` groups added.
- **New order-flow / on-chain strategies** (this cycle): `OrderFlowCVD`, `WickPressureFlow`, `ExchangeNetflowSignal`, `StablecoinFlowSignal` — integrated into `_detect_funding_and_onchain_signals` (fixed a `currency=pid` wiring bug) and into the unified signal cache with real labels (fixed generic `STRATEGY_SIGNAL` naming).
- **Execution**: `coinbase/src/execution.py` bracket orders verified by live proof (entry + stop + target + trailing).
- **UI dashboard** (`trading_system/ui/dashboard_server.py` + `dashboard.html`): rebuilt as a trading terminal. **All 15 endpoints probed and returning 200** (health, accounts, positions, strategies, approvals, performance, signals/opportunities, signals/feed, strategies/performance, market/regime, market/universe, market/candles, market/watchlist, order submit). Candles render client-side (canvas + volume + crosshair + 1m–1D). Order ticket posts `size_usd` and the server validates, prices, and computes fee/stop/target correctly.

### Test harness (verified green)
`run_all_tests.sh` → **8/8 suites pass**: Rust core, execution unit, orchestrator unit, execution live proof, trader v4 extra6/extra7, optimizer new-signal integration, **NAS feed cache (new)**.

---

## 2. Gap Analysis

### G2.1 — No durable feed cache for backtesting  **(PARTIALLY CLOSED)**
**Before**: every external feed lived only in in-process dicts (`rest_feed._CANDLE_CACHE`, `data.py._CACHE`) that vanished on restart. Historical replay for backtesting depended on re-fetching live or CSVs in `data/`.
**Now**: `data/feed_cache.py` persists every Coinbase candle fetch (full **and** incremental) to durable storage — parquet append + de-dup by timestamp — and provides `save_records`/`load_records` for non-OHLCV feeds (on-chain, prediction markets, news). Dashboard candles/watchlist now persist + fall back to the cache when the network is down.
**Remaining**:
- **Backfill** (E2): no job yet to pre-populate history for symbols/granularities never fetched live.

### G2.2 — Data-collection efficiency
- **REST incremental fetches were never cached** (`fetch_incremental_batch` only updated in-memory cache for full fetches). Now persisted to NAS via `feed_cache`.
- **Watchlist cold-start** did a live 24-pair discovery + batch fetch on every request. Added a 30s in-process TTL so the expensive path runs once per poll window.
- **CLI candles** (`coinbase/src/data.py`) now persist every fetch through `feed_cache` (E4 — CLOSED).
- Cache-hit / efficiency metrics are now tracked in `feed_cache.get_metrics()` (E5 — CLOSED); `rest_feed` circuit-breaker stats already exist.

### G2.3 — UI end-to-end gaps
- **Manual order submit persistence (CLOSED — E6).** The dashboard now writes every manual order to a shared, permission-tolerant `data/approvals_inbox/` (world-readable directory) in addition to the canonical `pending_approvals.json` (best-effort). The optimizer scans the inbox — approved → execute + delete, denied → delete — and the approval server's `/approve/<token>` & `/deny/<token>` links resolve inbox tokens. End-to-end flow verified regardless of which user runs each process.
- **`/health` shows `degraded`** only because the optimizer daemon heartbeat is stale (~47h) — the daemon is not running in this dev environment. This is expected here, not a code defect.
- **`/market/universe` returns `coinbase_total: 0`** in this environment (graph/neo4j + pair discovery not wired to live data here). Needs live daemon to populate.

### G2.4 — Coverage gate below 90%
- `portfolio_optimizer.py` ≈ 80% line / 80% branch (after E8 test campaign).
- `coinbase/src/run_trader_v4.py` ≈ 75% line.
Both are large legacy files. The user has accepted the **80% pragmatic target**; full 90% gate on these two remains open work (additional unit tests, not behavior changes).
- **`best_product` BUY→USDC divergence (FIXED):** `PortfolioOptimizer.best_product(currency, "BUY")` previously preferred `-USDC` pairs while the entire trading universe (incl. `coinbase_universe.py` and every detection path's `product_id`) uses `-USD`. Now `best_product` prefers `-USD` for both BUY and SELL, falling back to `-USDC` only when no `-USD` product exists. Updated the three tests that encoded the old USDC-for-BUY preference.

### G2.5 — Cross-user / file-ownership robustness
Beyond approvals: any file the optimizer writes as root (`pending_approvals.json`, `optimizer_state.db`, `.unified_signal_cache.json`) is not writable by a dev-launched dashboard. Should standardize on a shared, group-writable runtime dir.

### G2.6 — Peripheral stubs (out of core scope, noted)
- `trading_system/plaid/*` — banking integration is ~18 `TODO`s, not wired to the trading loop.
- `graph-alpha-bot/app/exec/broker_adapters.py` — SnapTrade/Merrill adapters raise `NotImplementedError`.
- `coinbase/cdp/__init__.py` — `x402`/`webhooks` raise `NotImplementedError`.
- `trading_system/core/compute_backend.py` — abstract base; concrete backends not implemented (acceptable; it's an interface).
These are intentionally out of scope for the trading pipeline and are not blocking.

---

## 3. Target State

1. **Every consumed feed is durable and replayable.** Coinbase candles, on-chain metrics, prediction-market snapshots, and news are all written to the NAS (`/media/scott/NAS3/feed_cache`, env `NAS_FEED_ROOT`) with parquet append + de-dup, and fall back to `<repo>/data/feed_cache` when the NAS is not writable by the running user. Backtests read from the cache, never the live API.
2. **Zero cold-start latency on the UI.** Watchlist + candles served from cache; live fetch only fills gaps. Dashboard is fully functional when the daemon is down (offline replay mode).
3. **Manual order entry works in dev and prod.** Approvals persist regardless of which user runs the dashboard.
4. **Measured collection efficiency.** Cache-hit rate and per-feed latency logged; re-fetches minimized via incremental + `after_ts`.
5. **Coverage**: core detection/execution paths at the agreed 80%+; the two legacy files have focused unit tests closing the biggest branches.

---

## 4. Enhancements / Roadmap

| # | Enhancement | Area | Effort | Status |
|---|-------------|------|--------|--------|
| E1 | Route on-chain / prediction-market / news feeds through `feed_cache` | Data | S | DONE |
| E2 | Backfill job: `scripts/backfill_feed_cache.py` populates top-N pairs × granularities from live API into NAS (cursor-paginated) | Data | M | DONE |
| E3 | Retention/compaction policy (keep 1m for 7d, 1h for 180d, 1d indefinitely); `compact_all()` wired into optimizer daemon loop every 3600 ticks | Data | S | DONE |
| E4 | Route CLI candles (`data.py`) through `feed_cache` | Data | S | DONE |
| E5 | Cache-hit + latency metrics in `feed_cache` | Obs | S | DONE |
| E6 | Fix cross-user approval persistence (shared `approvals_inbox`) | UI | M | DONE |
| E7 | Offline replay: watchlist + candles serve from `feed_cache` when live feed is down | UI | M | DONE |
| E8 | Unit tests to lift `portfolio_optimizer.py` / `run_trader_v4.py` to 80%+ branch | QA | M | DONE (optimizer suite 500 passed / 2 skipped; 80% line+branch met; also fixed a latent `Bar(instrument_type=…)` crash in smart-money detection, a `currency=pid`→base-ticker inconsistency across 6 detection paths, an invalid `currency=` kwarg passed to `ExchangeNetflowSignal.on_bar`, and a mis-indented `Opportunity(...)` constructor in `_detect_strategy_signals`) |
| E9 | Live `/market/universe` population via running daemon (verify graph scores) | UI | S | OPEN |

---

## 5. Next Steps (prioritized)

1. **E9 — Live `/market/universe`** — verify graph-score population via the running daemon.
2. **E8 follow-up** — `portfolio_optimizer.py` is at the agreed 80% gate; if the strict 90% gate is later required, deepen unit coverage on the remaining branches (mostly live-only / error paths). `run_trader_v4.py` remains the larger residual (interactive/WebSocket paths) and is still below 90%.
3. **Production verification** — under the systemd unit (root), confirm NAS writes land on `/media/scott/NAS3/feed_cache`; `compact_all()` now runs automatically every 3600 optimizer ticks, so schedule a one-off `backfill_feed_cache.py` run to seed history.
4. **Backfill scheduling** — cron/launchd a periodic `scripts/backfill_feed_cache.py --top-n 50 --days 2` to keep recent history fresh between daemon ticks.

---

## 6. Verification Checklist (this cycle)

- [x] Dashboard all endpoints return 200; candles/watchlist/order-submit wired.
- [x] Order submit validates + prices + computes fee, and now **persists cross-user** via `data/approvals_inbox/` (E6 verified end-to-end).
- [x] `data/feed_cache.py` created; parquet append + de-dup verified; NAS→local fallback verified.
- [x] `rest_feed` persists every fetch (full + incremental) to durable store.
- [x] CLI candles (`data.py`) persist through `feed_cache` (E4).
- [x] On-chain / prediction-market / news feeds persist through `feed_cache` (E1).
- [x] Retention/compaction (`compact_all`) + cache-hit metrics (`get_metrics`) added (E3/E5); `compact_all` wired into the optimizer daemon loop (E3 ops).
- [x] CLI candles (`data.py`) persist through `feed_cache` (E4).
- [x] Watchlist 30s TTL cache + offline fallback to `feed_cache` (E7).
- [x] `scripts/backfill_feed_cache.py` added + verified (E2).
- [x] `tests/coverage/test_feed_cache.py` (9) + `test_optimizer_inbox.py` + `test_dashboard_offline.py` added + wired into `run_all_tests.sh`.
- [x] Full harness green (9 suites); `tests/coverage/optimizer/` suite green at 499 passed / 2 skipped; `portfolio_optimizer.py` coverage 80% line+branch (E8 target met).
- [x] 12 previously-failing optimizer tests fixed (realistic candle trends, buy-capacity/state setup, `SOL-USD` currency assertion) + a latent `Bar(instrument_type=…)` crash in smart-money detection repaired.
- [ ] E9 live `/market/universe` verification under daemon.
- [ ] Production NAS write confirmed under systemd (root).
