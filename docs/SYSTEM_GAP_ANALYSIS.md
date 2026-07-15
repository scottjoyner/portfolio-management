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
- On-chain (CoinGecko netflow/stablecoin), prediction-market (Kalshi/Polymarket), and news feeds are **not yet** routed through `feed_cache`.
- No backfill job to populate history for symbols/granularities never fetched live.
- No retention/compaction policy (unbounded growth).

### G2.2 — Data-collection efficiency
- **REST incremental fetches were never cached** (`fetch_incremental_batch` only updated in-memory cache for full fetches). Now persisted to NAS via `feed_cache`.
- **Watchlist cold-start** did a live 24-pair discovery + batch fetch on every request. Added a 30s in-process TTL so the expensive path runs once per poll window.
- **CLI candles** (`coinbase/src/data.py`) still use a 300s in-memory TTL with no durability — should route through `feed_cache` too.
- No cache-hit metrics; collection efficiency is unmeasured.

### G2.3 — UI end-to-end gaps
- **Manual order submit cannot persist approvals when run by a non-root user.** `data/pending_approvals.json` is **root-owned** (optimizer runs as root via systemd). The dashboard, when launched as `scott`, returns `400 failed to persist approval: Permission denied`. In production (dashboard also root) this works; in dev it does not. *Fix options*: run the dashboard under the same user as the optimizer, or write approvals to a group-writable / shared path, or have the dashboard POST to the approval server instead of writing the file directly.
- **`/health` shows `degraded`** only because the optimizer daemon heartbeat is stale (~47h) — the daemon is not running in this dev environment. This is expected here, not a code defect.
- **`/market/universe` returns `coinbase_total: 0`** in this environment (graph/neo4j + pair discovery not wired to live data here). Needs live daemon to populate.

### G2.4 — Coverage gate below 90%
- `portfolio_optimizer.py` ≈ 76% line / 82% branch.
- `coinbase/src/run_trader_v4.py` ≈ 75% line.
Both are large legacy files. The user has accepted the **80% pragmatic target**; full 90% gate on these two remains open work (additional unit tests, not behavior changes).

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

| # | Enhancement | Area | Effort |
|---|-------------|------|--------|
| E1 | Route on-chain / prediction-market / news feeds through `feed_cache` | Data | S |
| E2 | Backfill job: populate N-day history for universe × granularity from live API into NAS | Data | M |
| E3 | Retention/compaction policy (e.g. keep 1m for 7d, 1h/1d indefinitely) | Data | S |
| E4 | Route CLI candles (`data.py`) through `feed_cache` | Data | S |
| E5 | Cache-hit + latency metrics in `feed_cache` and `rest_feed` circuit breakers | Obs | S |
| E6 | Fix cross-user approval persistence (shared runtime dir or dashboard→approval-server POST) | UI | M |
| E7 | Offline replay mode for dashboard when daemon/network down | UI | M |
| E8 | Unit tests to lift `portfolio_optimizer.py` / `run_trader_v4.py` to 80%+ branch | QA | M |
| E9 | Live `/market/universe` population via running daemon (verify graph scores) | UI | S |

---

## 5. Next Steps (prioritized)

1. **Close G2.1 remainder (E1)** — wrap CoinGecko on-chain, Kalshi/Polymarket, and news fetches with `feed_cache.save_records` so backtests have full history. *Low risk, high value.*
2. **E6** — make manual order entry persist in dev (same user / shared dir / POST to approval server). Unblocks end-to-end trading from the UI.
3. **E2** — one-shot backfill of the top-50 pairs × {60,300,900,3600,86400} for the last 90 days into the NAS.
4. **E3/E5** — add retention + cache-hit metrics; observe for a week.
5. **E8** — targeted unit tests for the two legacy files to hit the 80% branch gate.
6. **Verify in production**: under the systemd unit (root), confirm NAS writes land on `/media/scott/NAS3/feed_cache` and the dashboard renders from cache after a daemon restart.

---

## 6. Verification Checklist (this cycle)

- [x] Dashboard all endpoints return 200; candles/watchlist/order-submit wired.
- [x] Order submit validates + prices + computes fee (persist blocked only by file ownership — see G2.3).
- [x] `data/feed_cache.py` created; parquet append + de-dup verified; NAS→local fallback verified.
- [x] `rest_feed` persists every fetch (full + incremental) to durable store.
- [x] Watchlist 30s TTL cache added.
- [x] `tests/coverage/test_feed_cache.py` (7 tests) added + wired into `run_all_tests.sh`.
- [x] Full harness **8/8 green**.
- [ ] E1–E9 implemented (next cycle).
- [ ] Production NAS write confirmed under systemd (root).
