# Fast Robust Trading Path Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Make the portfolio-management trading system robust enough for unattended operation while reducing opportunity-to-order latency when high-quality opportunities appear.

**Architecture:** Keep Python as the strategy/risk/control plane, but introduce a clearly bounded fast path: fresh market data -> precomputed opportunity/risk snapshot -> atomic execution intent -> idempotent order placement. Do not bypass risk limits, dry-run/live mode gates, bucket accounting, kill switches, or audit logs.

**Tech Stack:** Python 3 trading engine, Coinbase CLI/SDK wrappers, existing Rust operator-actions sidecar pattern, Docker Compose, pytest once test dependency is restored.

---

## Audit Findings From Current Code

1. The main trader loop is batch/poll oriented.
   - `coinbase/src/run_trader_v2.py` defaults to `poll_interval_secs=30.0`.
   - It uses `PollingFeed` with a 5 second background poll interval.
   - A `WebSocketFeed` exists in `coinbase/src/feed.py`, but `UnifiedTrader` currently instantiates `PollingFeed`, not the WebSocket feed.

2. Opportunity-to-order latency is dominated by full-cycle strategy processing.
   - `_tick()` gathers all tickers, builds bars, updates regime/news/market profile, gathers opportunities, applies overlays, sizes risk, then executes.
   - That is robust for slower strategy loops, but it is not a minimal-latency execution lane.

3. Live spot execution is CLI subprocess based.
   - `coinbase/src/cb_client.py` calls the Coinbase CLI via `subprocess.run()` for market data and order placement.
   - This is safer/easier to debug, but slow for opportunity bursts.
   - Futures execution already uses the Python SDK in `coinbase/src/futures_execution.py`, which is a better pattern for low-latency order placement.

4. Safety gates exist but need hardening before faster trading.
   - `ExecutionOrchestrator.process_opportunities()` runs portfolio risk, ranking, news risk, fee/liquidity sizing, Kelly sizing, and trade checks.
   - `execute_signals()` can place multiple signals sequentially without a clear per-tick max, latency budget, duplicate-order window, or global kill switch check.

5. State persistence is too file/JSON oriented for high-confidence live execution.
   - Buckets and approvals are JSON/file based.
   - State writes are acceptable for dashboard and operator actions, but live order idempotency and replay protection should be durable and atomic.

6. Test harness is currently not runnable on the host.
   - `python3 -m pytest coinbase/tests tests -q` failed because `pytest` is not installed.
   - `.venv/bin/python -m pytest ...` also failed because the local venv lacks pytest.
   - `python3 -m py_compile coinbase/src/*.py trading_system/ui/dashboard_server.py` passes.

---

## Design Principles

- Fast does not mean unsafe. No path can skip risk checks, mode checks, kill switches, bucket limits, or idempotency.
- Separate latency-sensitive paths from slow enrichment. News, graph overlays, ranking updates, and reports should enrich decisions but not block emergency exits or pre-qualified high-conviction entries.
- Use WebSocket/cache for prices; never shell out per product in the hot path.
- Use SDK/native calls for live order placement where possible; keep CLI as fallback/admin tooling.
- Every live order gets an idempotency key, persisted intent, persisted response, and reconciliation path.
- Default deployment remains dry-run/paper unless explicitly configured otherwise.

---

## Milestone 1: Restore Test/Verification Baseline

### Task 1: Add reproducible Python test dependencies

**Objective:** Make tests runnable on a fresh machine.

**Files:**
- Create or modify: `requirements-dev.txt`
- Possibly modify: `README.md` or deployment docs if present

**Steps:**
1. Add `pytest` and any existing test-only dependencies to `requirements-dev.txt`.
2. Add setup command documentation:
   - `python3 -m venv .venv`
   - `.venv/bin/python -m pip install -r requirements-dev.txt`
3. Verify:
   - `.venv/bin/python -m pytest coinbase/tests tests -q`

### Task 2: Add smoke tests around execution safety defaults

**Objective:** Prove default mode cannot place live orders accidentally.

**Files:**
- Create: `tests/test_trader_safety_defaults.py`

**Required assertions:**
- `TraderConfig.from_env()` defaults to paper mode.
- Missing live/futures credentials fail startup validation.
- `COINBASE_DRY_RUN` defaults to true unless live/futures is explicitly chosen.

---

## Milestone 2: Add a Fast Market Data Lane

### Task 3: Prefer WebSocketFeed with PollingFeed fallback

**Objective:** Reduce market-data latency without breaking current polling reliability.

**Files:**
- Modify: `coinbase/src/run_trader_v2.py`
- Modify: `coinbase/src/feed.py`
- Test: `tests/test_feed_selection.py`

**Implementation sketch:**
- Add env flag: `TRADER_FEED_MODE=auto|websocket|polling`.
- In `UnifiedTrader.__init__`, construct `WebSocketFeed` first when mode is `auto` or `websocket`.
- If WebSocket startup returns false or no ticker arrives within a bounded startup window, fallback to `PollingFeed`.
- Preserve current polling behavior for missing dependencies.

**Verification:**
- Unit test fallback when `websocket-client` is absent.
- Unit test `TRADER_FEED_MODE=polling` forces polling.

### Task 4: Add stale-price detection and trading block

**Objective:** Prevent fast trading on stale or synthetic data.

**Files:**
- Modify: `coinbase/src/feed.py`
- Modify: `coinbase/src/run_trader_v2.py`
- Test: `tests/test_price_freshness_gate.py`

**Rules:**
- Add `TRADER_MAX_PRICE_AGE_MS` defaulting to a conservative value, e.g. `3000` for live/futures and `15000` for paper.
- In live/futures mode, block execution when a signal's product price is older than max age or source is synthetic.
- Dashboard/status should expose stale-product count.

---

## Milestone 3: Add Execution Idempotency and Kill Switches

### Task 5: Add global kill switch gate

**Objective:** One file/env flag must stop all new orders instantly.

**Files:**
- Modify: `coinbase/src/orchestrator.py`
- Create: `tests/test_execution_kill_switch.py`

**Rules:**
- Env: `TRADER_KILL_SWITCH=true` blocks all execution except paper logging.
- File: `data/trading_kill_switch` blocks live/futures execution if present.
- Return result must clearly say `status=blocked`, `reason=kill_switch`.

### Task 6: Add duplicate-order/idempotency window

**Objective:** Prevent repeated orders for the same opportunity during retries or rapid ticks.

**Files:**
- Modify: `coinbase/src/orchestrator.py`
- Create: `tests/test_execution_idempotency.py`

**Rules:**
- Compute deterministic intent key from product, side, strategy, bucket, rounded entry/stop/target, and time window.
- Store recent intent keys in memory and persist live/futures accepted keys to `state/order_intents.jsonl` or SQLite.
- If duplicate appears inside window, skip with `reason=duplicate_intent`.

### Task 7: Cap per-tick order burst

**Objective:** Prevent runaway multi-signal execution.

**Files:**
- Modify: `coinbase/src/orchestrator.py`
- Test: `tests/test_execution_burst_limits.py`

**Rules:**
- Env: `TRADER_MAX_ORDERS_PER_TICK`, default conservative, e.g. `1` live/futures, `3` paper.
- Env: `TRADER_MAX_NOTIONAL_PER_TICK`.
- Sort by `opportunity_score`, execute only allowed subset, mark rest as `deferred`.

---

## Milestone 4: Convert Live Hot Path Away From CLI Subprocesses

### Task 8: Add SDK-backed spot execution client

**Objective:** Avoid shelling out to Coinbase CLI for live spot order placement.

**Files:**
- Create: `coinbase/src/spot_execution.py`
- Modify: `coinbase/src/execution_v2.py`
- Test: `tests/test_spot_execution_adapter.py`

**Approach:**
- Follow `coinbase/src/futures_execution.py` import-isolation pattern to avoid local package name collision.
- Implement SDK-backed market, limit, stop-limit, and cancel methods.
- Keep `CBClient` CLI path as fallback.

### Task 9: Add latency instrumentation around order placement

**Objective:** Know whether the fast path is actually fast.

**Files:**
- Modify: `coinbase/src/execution_v2.py`
- Modify: `coinbase/src/futures_execution.py`
- Modify: `coinbase/src/orchestrator.py`

**Metrics to log:**
- market data age ms
- signal processing ms
- risk check ms
- order placement ms
- total opportunity-to-order ms
- exchange response order_id/client_order_id

---

## Milestone 5: Fast Path for Prequalified Opportunities

### Task 10: Add `fast_execute_prechecked_signal()` API

**Objective:** Create a minimal-latency lane for signals that have already passed risk checks.

**Files:**
- Modify: `coinbase/src/orchestrator.py`
- Test: `tests/test_fast_execute_prechecked_signal.py`

**Rules:**
- This method still checks kill switch, freshness, duplicate key, burst caps, max notional, and mode/dry-run.
- It does not rerun slow overlays like news/graph/ranking.
- It accepts only a typed `TradeSignal` plus a freshness snapshot.

### Task 11: Add hot-opportunity cache

**Objective:** Precompute and refresh candidate opportunities continuously so execution does not wait for the entire slow pipeline.

**Files:**
- Modify: `coinbase/src/run_trader_v2.py`
- Possibly create: `coinbase/src/hot_opportunity_cache.py`
- Test: `tests/test_hot_opportunity_cache.py`

**Rules:**
- Slow pipeline updates candidate cache.
- Fast feed events only reprice/validate existing candidate entries.
- Candidate expires quickly, e.g. 5-15 seconds.
- Candidate must carry the risk profile version used when it was approved.

---

## Milestone 6: Deployment Hardening

### Task 12: Add health endpoints/status fields for live readiness

**Objective:** Dashboard and health checks should distinguish running from live-ready.

**Files:**
- Modify: `trading_system/ui/dashboard_server.py`
- Modify: `coinbase/src/run_trader_v2.py`

**Readiness fields:**
- feed mode
- feed freshness
- kill switch status
- dry_run status
- execution backend CLI vs SDK
- open positions count
- duplicate-intent cache size
- last order latency ms
- last order error

### Task 13: Add deployment runbook

**Objective:** Make another-machine deployment safe.

**Files:**
- Create: `docs/runbooks/live-trading-deployment.md`

**Must include:**
- clone/setup commands
- venv/dependency install
- env variables
- dry-run smoke test
- paper mode soak test
- live readiness checklist
- kill switch procedures
- rollback procedure

---

## Immediate Recommendation

Do not jump straight to live fast trading yet. The next safe implementation order is:

1. Restore test dependencies and add safety tests.
2. Add kill switch, stale-price gate, duplicate-intent guard, and burst caps.
3. Switch market data to WebSocket-with-polling-fallback.
4. Instrument latency.
5. Only then add SDK-backed spot execution and the prechecked fast path.

This gives speed without creating a system that can accidentally fire repeated or stale orders.
