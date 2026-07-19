# Portfolio OS System Truth Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Add a read-only, source-labeled System Truth layer to the Portfolio OS dashboard so operators can immediately see trading mode, data freshness, NAS cache reality, runtime health, current exposure, and the canonical terminal link.

**Architecture:** `dashboard_server.py` will expose a single `/system-truth` JSON contract assembled only from local persisted state, read-only health probes, cache inspection, and a host-written health snapshot if available. `dashboard.html` will render a compact diagnostic strip below the mode banner and refresh it with the existing polling loop. Missing, stale, conflicting, or fallback evidence is always reported as unknown/warn—not healthy.

**Tech Stack:** Python standard-library HTTP server, pytest, static HTML/CSS/JavaScript, existing JSON state files, NAS-backed feed cache.

---

### Task 1: Define and test the System Truth contract

**Objective:** Establish a deterministic, source-labeled API schema before production code.

**Files:**
- Modify: `tests/coverage/test_dashboard_offline.py`
- Modify: `trading_system/ui/dashboard_server.py`

**Step 1: Write failing tests**
- Assert mode precedence: explicit trader health beats persisted state; absent evidence yields `unknown`, never paper.
- Assert heartbeat and health-snapshot ages become `fresh`, `stale`, or `unknown`.
- Assert the output contains `generated_at`, `trading_mode`, `feed`, `cache`, `services`, `exposure`, `terminal`, and `warnings`.

**Step 2: Verify RED**
- Run: `pytest tests/coverage/test_dashboard_offline.py -q`

**Step 3: Implement the minimal read-only helpers and `api_system_truth()`**
- Probe the local trader health endpoint with a short timeout.
- Read persisted paper state and heartbeat safely.
- Read an optional atomic `data/system-health.json`; label missing or stale snapshots unknown.
- Use only actual marked open-position values for gross exposure and separately label capital-in-play.
- Add `GET /system-truth` to `DashboardHandler`.

**Step 4: Verify GREEN**
- Run focused tests and then `pytest tests/coverage/test_dashboard_offline.py tests/coverage/test_feed_cache.py -q`.

### Task 2: Add cache inspection with NAS/fallback truth

**Objective:** Report configured versus resolved cache root, fallback status, access health, file/byte totals, and coverage freshness without mutating storage.

**Files:**
- Modify: `data/feed_cache.py`
- Modify: `tests/coverage/test_feed_cache.py`

**Step 1: Write failing tests**
- Create temporary cache trees and assert read-only inspection reports path, selected root, fallback, totals, per-kind details, and missing/unavailable conditions.

**Step 2: Verify RED**
- Run: `pytest tests/coverage/test_feed_cache.py -q`

**Step 3: Implement `inspect_cache()`**
- Do not call `ensure_root()` or perform write probes while rendering status.
- Inspect existing configured/resolved root only; report the current process root explicitly.
- Bound directory traversal and tolerate unreadable files.

**Step 4: Wire into `api_system_truth()` and verify GREEN**
- Run focused tests plus the dashboard contract tests.

### Task 3: Render the diagnostic System Truth strip

**Objective:** Make the server truth visible directly below the mode banner without obscuring existing trading metrics.

**Files:**
- Modify: `trading_system/ui/dashboard.html`
- Modify: `tests/coverage/test_dashboard_offline.py` (static contract checks only if appropriate)

**Step 1: Add a failing static contract test**
- Assert the page references `/system-truth` and includes stable System Truth element IDs.

**Step 2: Implement compact UI**
- Cells: mode/evidence, trader health, feed age, cache root/coverage, service snapshot, gross exposure, terminal link.
- Classes: green fresh/paper, red live/failure, amber stale/unknown/conflict.
- Replace account-derived banner mode with `/system-truth` mode.
- Extend the existing ten-second `tick()` request set with `/system-truth`.

**Step 3: Verify GREEN**
- Run the focused pytest set.

### Task 4: Validate runtime and preserve existing work

**Objective:** Verify API/UI behavior on the active dashboard and commit only intentional changes.

**Files:**
- Review: `trading_system/ui/dashboard_server.py`, `trading_system/ui/dashboard.html`, `data/feed_cache.py`, focused tests, plan.

**Steps:**
1. Run focused tests and the project test wrapper.
2. Start or reuse a non-production local test server if necessary; fetch `/system-truth` and validate JSON shape.
3. Verify the existing active dashboard endpoint serves the updated System Truth only after safely restarting its owning service/container.
4. Review `git diff`; explicitly avoid unrelated pre-existing dirty data and checklist edits.
5. Commit only the system-truth code, tests, and plan.
