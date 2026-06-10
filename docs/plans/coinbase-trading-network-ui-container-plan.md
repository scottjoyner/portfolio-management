# Coinbase Trading Network UI + Container Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task. Do not enable live order execution until the read-only API, paper worker, UI, and safety controls are verified.

**Goal:** Build a containerized trading control plane around the verified Coinbase v3 integration, while preserving adhoc scripting workflows for operator use.

**Architecture:** One shared trading core should power both adhoc scripts and long-running services. Strategies emit signals/order intents; risk/execution services decide whether to preview, reject, paper-fill, or live-submit. The UI observes runtime state, strategy status, balances, orders, events, and risk controls through FastAPI endpoints and WebSocket/event polling.

**Tech Stack:** Python 3.12, FastAPI, Uvicorn, Coinbase CLI + `trading_system/connectors/coinbase_v3.py`, Postgres, Redis, Docker Compose, minimal UI via Streamlit or FastAPI static/HTMX. Default runtime mode is paper/read-only.

---

## Current Verified Foundation

Known working files:

- `trading_system/connectors/coinbase_v3.py`
  - Verified with live Coinbase balance access.
  - Uses Coinbase CLI and ECDSA/JWT authentication.

- `check_balance.py`
  - Verified direct balance checker.

- `verify_coinbase_v3.py`
  - Coinbase verification script.

- `scripts/setup_coinbase_credentials.py`
  - Host setup script for Coinbase key configuration.

- `trading_system/apps/api/main.py`
  - Existing FastAPI control API scaffold.

- `trading_system/apps/worker/main.py`
  - Existing worker scaffold, currently paper/synthetic-market oriented.

- `trading_system/catalog/strategy_registry.py`
  - Existing strategy catalog metadata.

Known live Coinbase verification:

- `coinbase balance` succeeded.
- 16 Coinbase accounts were returned.
- `CoinbaseConnectorV3.get_balances()` succeeded.
- `CoinbaseConnectorV3.get_price('BTC-USD')` succeeded.

Important rule:

- The first containerized system must be read-only plus paper execution by default.
- Live order placement must remain disabled until the UI, event log, risk gates, and kill switch are visible and tested.

---

## Target Runtime Shape

```text
Adhoc CLI Scripts
        |
        v
Shared Trading Core
  - Coinbase connector
  - Strategy catalog
  - Strategy runner
  - Risk engine
  - Order manager
  - Event recorder
        |
        +----------------------+
        |                      |
        v                      v
FastAPI Control API       Worker/Scheduler
        |                      |
        v                      v
Postgres + Redis state/event layer
        |
        v
Web UI Dashboard
```

---

## Safety Principles

1. Paper mode by default.
2. Live trading disabled by default.
3. Live mode requires `LIVE_TRADING_ENABLED=true`.
4. Strategy-level live enablement required.
5. All live orders must pass risk evaluation.
6. All live orders must be previewed before submission.
7. All orders must use idempotent `client_order_id`.
8. All signals, previews, risk decisions, orders, fills, and errors must be recorded as events.
9. UI must expose kill switch before any live execution is enabled.
10. Container credentials must be mounted read-only or injected as Docker secrets, not baked into images.

---

# Phase 1: Shared Runtime Models + Event Spine

## Task 1: Create runtime model module

**Objective:** Define the shared data shapes used by scripts, API, worker, and UI.

**Files:**

- Create: `trading_system/core/runtime/models.py`
- Test: `trading_system/tests/unit/test_runtime_models.py`

**Step 1: Write failing tests**

Test expectations:

- `StrategyStatus` can serialize to dict.
- `TradingEvent` includes timestamp, source, event_type, and payload.
- `OrderIntent` requires strategy_id, product_id, side, and sizing.
- `RuntimeStatus` reports mode, live_enabled, coinbase_connected, worker_status.

**Step 2: Implement models**

Use Pydantic v2 if already available in the trading-system package, otherwise dataclasses with explicit `to_dict()` methods.

Suggested models:

- `RuntimeStatus`
- `StrategyStatus`
- `OrderIntent`
- `OrderPreviewRecord`
- `ExecutionStatus`
- `TradingEvent`
- `AccountSnapshot`
- `RiskDecision`

**Step 3: Verify**

Run:

```bash
cd /home/scott/git/portfolio-management
pytest trading_system/tests/unit/test_runtime_models.py -v
```

Expected: all tests pass.

---

## Task 2: Create event recorder with file fallback

**Objective:** Provide a simple event spine before Postgres/Redis integration.

**Files:**

- Create: `trading_system/core/runtime/events.py`
- Test: `trading_system/tests/unit/test_runtime_events.py`

**Step 1: Write failing tests**

Test expectations:

- `EventRecorder.record(event)` appends event to JSONL file.
- `EventRecorder.tail(limit=10)` returns newest events.
- Events can be filtered by `strategy_id`, `source`, or `event_type`.

**Step 2: Implement file-backed recorder**

Default path:

```text
runtime/events/trading-events.jsonl
```

Environment override:

```text
TRADING_EVENTS_PATH
```

**Step 3: Verify**

Run:

```bash
pytest trading_system/tests/unit/test_runtime_events.py -v
```

Expected: all tests pass.

---

## Task 3: Add Coinbase runtime wrapper

**Objective:** Wrap `CoinbaseConnectorV3` in a runtime-safe service that normalizes balances, prices, and auth status for API/UI/scripts.

**Files:**

- Create: `trading_system/core/exchange/coinbase_service.py`
- Test: `trading_system/tests/unit/test_coinbase_service.py`

**Step 1: Write failing tests with fake connector**

Test expectations:

- `get_connection_status()` returns green/red plus error details.
- `get_balances_snapshot()` returns `AccountSnapshot` shape.
- `get_price(product_id)` returns normalized price payload.
- Service does not leak private key or credential file contents.

**Step 2: Implement service wrapper**

The service should accept a connector object for dependency injection.

Do not call live Coinbase in unit tests.

**Step 3: Verify**

Run:

```bash
pytest trading_system/tests/unit/test_coinbase_service.py -v
```

Expected: all tests pass.

---

# Phase 2: Adhoc Operator Scripts

## Task 4: Add `scripts/trading/status.py`

**Objective:** Provide one command to show runtime status, Coinbase connection, balances, and watched prices.

**Files:**

- Create: `scripts/trading/status.py`

**Behavior:**

```bash
python3 scripts/trading/status.py
python3 scripts/trading/status.py --json
python3 scripts/trading/status.py --products BTC-USD,ETH-USD,SOL-USD
```

Output should include:

- runtime mode
- Coinbase connection status
- balance count
- top balances
- watched product prices
- last event timestamp if event log exists

**Verification:**

Run:

```bash
python3 scripts/trading/status.py --json
```

Expected: valid JSON and no credential leakage.

---

## Task 5: Add `scripts/trading/list_strategies.py`

**Objective:** List all known strategies and implementation status.

**Files:**

- Create: `scripts/trading/list_strategies.py`

**Behavior:**

```bash
python3 scripts/trading/list_strategies.py
python3 scripts/trading/list_strategies.py --json
python3 scripts/trading/list_strategies.py --category trend_following
```

Sources:

- Prefer existing `trading_system/catalog/strategy_registry.py`.
- If needed, add adapter function but avoid rewriting the registry.

**Verification:**

Run:

```bash
python3 scripts/trading/list_strategies.py --json
```

Expected: strategy count and strategy metadata.

---

## Task 6: Add `scripts/trading/preview_order.py`

**Objective:** Preview an order from CLI without executing.

**Files:**

- Create: `scripts/trading/preview_order.py`

**Behavior:**

```bash
python3 scripts/trading/preview_order.py \
  --product BTC-USD \
  --side BUY \
  --quote-size 100
```

Requirements:

- Must call Coinbase preview/dry-run only.
- Must record a `TradingEvent` with event_type `order_preview_requested`.
- Must never execute live orders.

**Verification:**

Run with small quote size.

Expected: preview payload or clear Coinbase permission/auth error.

---

## Task 7: Add `scripts/trading/run_strategy_once.py`

**Objective:** Run one strategy once against current market state and print the proposed signal/order intent.

**Files:**

- Create: `scripts/trading/run_strategy_once.py`

**Behavior:**

```bash
python3 scripts/trading/run_strategy_once.py \
  --strategy triplema \
  --product BTC-USD \
  --mode paper
```

Requirements:

- No live order execution.
- Emit event `strategy_tick_completed`.
- If strategy cannot run due to missing historical data, return a clear reason and record an event.

---

# Phase 3: FastAPI Read-Only Control API

## Task 8: Add runtime status endpoint

**Objective:** Expose runtime health in API.

**Files:**

- Modify: `trading_system/apps/api/main.py`
- Test: `trading_system/tests/integration/test_runtime_api.py`

**Endpoint:**

```text
GET /runtime/status
```

Response fields:

- mode
- live_trading_enabled
- coinbase_connected
- worker_status
- event_log_status
- timestamp

**Verification:**

Run:

```bash
pytest trading_system/tests/integration/test_runtime_api.py -v
```

Expected: endpoint returns expected shape.

---

## Task 9: Add Coinbase balance and price endpoints

**Objective:** Expose verified Coinbase data to UI and operators.

**Files:**

- Modify: `trading_system/apps/api/main.py`
- Test: `trading_system/tests/integration/test_coinbase_api.py`

**Endpoints:**

```text
GET /coinbase/status
GET /coinbase/balances
GET /coinbase/prices/{product_id}
```

Requirements:

- No private key leakage.
- Clear error on missing Coinbase configuration.
- `GET /coinbase/balances` may hit live Coinbase in manual verification, but tests must mock/fake the service.

**Manual verification:**

```bash
uvicorn trading_system.apps.api.main:app --host 0.0.0.0 --port 8000
curl http://localhost:8000/coinbase/balances
curl http://localhost:8000/coinbase/prices/BTC-USD
```

---

## Task 10: Add strategy status/catalog endpoints

**Objective:** Expose strategy list and runtime status to UI.

**Files:**

- Modify: `trading_system/apps/api/main.py`
- Test: `trading_system/tests/integration/test_strategy_api.py`

**Endpoints:**

```text
GET /strategies/catalog
GET /strategies/status
GET /strategies/{strategy_id}
```

Requirements:

- `catalog` returns metadata from existing registry.
- `status` returns runtime status objects even if no worker is running.
- Missing strategy returns 404.

---

## Task 11: Add event log endpoint

**Objective:** Let UI show recent activity.

**Files:**

- Modify: `trading_system/apps/api/main.py`
- Test: `trading_system/tests/integration/test_events_api.py`

**Endpoint:**

```text
GET /events?limit=100&strategy_id=&event_type=&source=
```

Requirements:

- Reads from file-backed `EventRecorder` initially.
- Later replaceable with Postgres.

---

# Phase 4: First UI Dashboard

## Task 12: Choose UI implementation and add app shell

**Objective:** Add minimal web UI service.

**Recommendation:** Start with FastAPI static/HTMX if we want a permanent low-dependency control panel; use Streamlit if speed is more important than long-term structure.

**Preferred files for FastAPI static/HTMX:**

- Create: `trading_system/apps/ui/main.py`
- Create: `trading_system/apps/ui/templates/index.html`
- Create: `trading_system/apps/ui/static/app.css`

**Initial pages:**

- Overview
- Coinbase Account
- Strategies
- Execution/Event Log
- Risk Controls placeholder

**Verification:**

```bash
uvicorn trading_system.apps.ui.main:app --host 0.0.0.0 --port 8501
```

Open:

```text
http://localhost:8501
```

---

## Task 13: Add Overview page

**Objective:** Show high-level system health.

**UI content:**

- API status
- trading mode
- live trading enabled/disabled
- Coinbase connection status
- worker status
- last event timestamp
- balance count

**Data source:**

- API `GET /runtime/status`
- API `GET /coinbase/status`
- API `GET /events?limit=1`

---

## Task 14: Add Coinbase Account page

**Objective:** Display live balances and watched product prices.

**UI content:**

- balances table
- held amount
- ready/active status
- watched product prices
- refresh button

**Data source:**

- API `GET /coinbase/balances`
- API `GET /coinbase/prices/{product_id}`

---

## Task 15: Add Strategy Catalog page

**Objective:** Show all strategies and their runtime statuses.

**UI content:**

- strategy name
- category
- implementation status
- enabled/disabled
- mode: paper/live/disabled
- last tick
- last signal
- last error

**Data source:**

- API `GET /strategies/catalog`
- API `GET /strategies/status`

---

## Task 16: Add Event Log page

**Objective:** Show recent events across the trading network.

**UI content:**

- timestamp
- level
- source
- strategy_id
- event_type
- payload summary
- raw JSON toggle

**Data source:**

- API `GET /events?limit=100`

---

# Phase 5: Containerized Runtime

## Task 17: Create Dockerfile for trading system

**Objective:** Build one image that can run API, worker, scripts, or UI.

**Files:**

- Create: `Dockerfile.trading`
- Modify: `.dockerignore` if needed

**Requirements:**

- Python 3.12
- Node.js/npm
- `@coinbase/coinbase-cli`
- Python dependencies from `trading_system/pyproject.toml` or consolidated requirements
- Non-root app user if practical
- No credentials baked into image

**Verification:**

```bash
docker build -f Dockerfile.trading -t portfolio-trading:local .
docker run --rm portfolio-trading:local coinbase --version
```

---

## Task 18: Create container entrypoint for Coinbase configuration

**Objective:** Configure Coinbase CLI from Docker secret or mounted key file at startup.

**Files:**

- Create: `docker/trading-entrypoint.sh`

**Behavior:**

If `/run/secrets/coinbase_cdp_api_key` exists:

```bash
coinbase env live --key-file /run/secrets/coinbase_cdp_api_key
```

If not, continue only in paper/read-only mode and report degraded Coinbase status.

**Security:**

- Do not print key contents.
- Do not copy key into image layers.
- Key mount must be read-only.

---

## Task 19: Create `docker-compose.trading.yml`

**Objective:** Run the full local trading network.

**Files:**

- Create: `docker-compose.trading.yml`

**Services:**

- `postgres`
- `redis`
- `api`
- `worker`
- `ui`

**Default environment:**

```text
TRADING_MODE=paper
LIVE_TRADING_ENABLED=false
REQUIRE_APPROVALS=true
```

**Credential mount:**

```yaml
secrets:
  coinbase_cdp_api_key:
    file: /home/scott/.coinbase/cdp_api_key.json
```

**Verification:**

```bash
docker compose -f docker-compose.trading.yml up --build
curl http://localhost:8000/health
curl http://localhost:8000/runtime/status
curl http://localhost:8000/coinbase/balances
```

Expected:

- API healthy.
- UI reachable.
- Worker starts in paper mode.
- Coinbase balance endpoint works if secret is mounted.

---

# Phase 6: Worker and Strategy Runtime

## Task 20: Add StrategyRunner abstraction

**Objective:** Normalize strategy execution so every strategy emits signals/order intents instead of placing orders directly.

**Files:**

- Create: `trading_system/core/strategy/runner.py`
- Test: `trading_system/tests/unit/test_strategy_runner.py`

**Requirements:**

- Run strategy once with market state.
- Return normalized signal list.
- Catch exceptions and emit `strategy_error` events.

---

## Task 21: Add OrderManager in paper mode

**Objective:** Route order intents through risk and paper execution.

**Files:**

- Create: `trading_system/core/execution/order_manager.py`
- Test: `trading_system/tests/unit/test_order_manager.py`

**Requirements:**

- Accept `OrderIntent`.
- Evaluate basic risk policy.
- In paper mode, create paper order/fill record.
- Record events for accepted/rejected orders.
- Never live-submit in this task.

---

## Task 22: Wire worker to StrategyRunner and OrderManager

**Objective:** Replace synthetic worker behavior with observable strategy ticks in paper mode.

**Files:**

- Modify: `trading_system/apps/worker/main.py`

**Requirements:**

- Load enabled strategies.
- Fetch watched product prices.
- Evaluate strategies.
- Submit intents to paper OrderManager.
- Record events.
- Expose heartbeat event.

**Verification:**

Run worker locally and inspect events:

```bash
python3 -m trading_system.apps.worker.main
python3 scripts/trading/status.py --json
curl http://localhost:8000/events?limit=20
```

---

# Phase 7: Risk Controls and Live Execution Gate

## Task 23: Add kill switch API

**Objective:** Provide global stop control visible in UI and enforced by OrderManager.

**Files:**

- Modify: `trading_system/apps/api/main.py`
- Modify: `trading_system/core/execution/order_manager.py`
- Test: `trading_system/tests/integration/test_risk_controls_api.py`

**Endpoints:**

```text
GET /risk/status
POST /risk/kill-switch
POST /risk/resume-paper
```

**Requirements:**

- Kill switch prevents new orders.
- Resume returns to paper mode only.
- Live mode cannot be resumed from kill switch without explicit separate enable flow.

---

## Task 24: Add live Coinbase executor behind hard gate

**Objective:** Add live execution capability but keep it unreachable unless all gates are explicit.

**Files:**

- Create: `trading_system/core/execution/coinbase_executor.py`
- Test: `trading_system/tests/unit/test_coinbase_executor.py`

**Live execution requirements:**

- `LIVE_TRADING_ENABLED=true`
- strategy explicitly set to live
- global kill switch off
- order passes risk
- Coinbase preview succeeds
- order has idempotent `client_order_id`
- event log records preview and submission

**Important:**

- Unit tests use fake Coinbase connector.
- Do not place real live order during automated tests.

---

## Task 25: Add UI controls for live-gated operations

**Objective:** Make risk and live state visible before enabling controls.

**UI controls:**

- kill switch
- resume paper mode
- strategy enable/disable
- strategy paper/live toggle, disabled unless `LIVE_TRADING_ENABLED=true`
- manual order preview form
- live order submission disabled by default

**Verification:**

Manual browser check:

- UI clearly shows paper mode.
- Kill switch changes status.
- Live controls remain disabled unless configured.

---

# Phase 8: Postgres/Redis Persistence Upgrade

## Task 26: Add database schema for events and execution state

**Objective:** Move from JSONL event fallback to durable database-backed state.

**Tables:**

- `trading_events`
- `strategy_status`
- `account_snapshots`
- `balance_snapshots`
- `price_snapshots`
- `order_intents`
- `order_previews`
- `orders`
- `fills`
- `risk_decisions`

**Files:**

- Create migration under existing storage/alembic pattern if present.
- If no alembic pattern is usable, create `trading_system/storage/schema.py` and document migration command.

---

## Task 27: Add Redis pub/sub for live UI updates

**Objective:** Provide low-latency event propagation from worker to API/UI.

**Files:**

- Create: `trading_system/core/runtime/pubsub.py`
- Modify: worker to publish events
- Modify: API to expose WebSocket stream

**Endpoint:**

```text
WS /ws/events
```

**Verification:**

- Start compose stack.
- Trigger a strategy tick.
- UI event log updates without full page refresh.

---

# Definition of Done

The first production-ready milestone is complete when:

1. `python3 scripts/trading/status.py --json` works on host.
2. `python3 scripts/trading/list_strategies.py --json` works on host.
3. API exposes:
   - `/runtime/status`
   - `/coinbase/status`
   - `/coinbase/balances`
   - `/coinbase/prices/BTC-USD`
   - `/strategies/catalog`
   - `/strategies/status`
   - `/events`
4. UI shows:
   - Coinbase connection status
   - balances
   - strategy catalog
   - worker status
   - recent events
   - paper/live/kill-switch state
5. Docker Compose stack starts successfully with:
   - API
   - UI
   - worker
   - Redis
   - Postgres
6. Containerized API can read Coinbase balances using Docker secret.
7. Worker runs in paper mode and emits events.
8. Live order placement remains disabled by default.
9. Tests for runtime models, API endpoint shapes, and event recorder pass.

---

# Commands for First Milestone Verification

```bash
cd /home/scott/git/portfolio-management

# Host-level checks
python3 check_balance.py
python3 scripts/trading/status.py --json
python3 scripts/trading/list_strategies.py --json

# Unit/integration tests
pytest trading_system/tests/unit/test_runtime_models.py -v
pytest trading_system/tests/unit/test_runtime_events.py -v
pytest trading_system/tests/integration/test_runtime_api.py -v
pytest trading_system/tests/integration/test_coinbase_api.py -v
pytest trading_system/tests/integration/test_strategy_api.py -v
pytest trading_system/tests/integration/test_events_api.py -v

# API local run
uvicorn trading_system.apps.api.main:app --host 0.0.0.0 --port 8000
curl http://localhost:8000/health
curl http://localhost:8000/runtime/status
curl http://localhost:8000/coinbase/balances
curl http://localhost:8000/strategies/catalog

# Container run
docker compose -f docker-compose.trading.yml up --build
curl http://localhost:8000/runtime/status
curl http://localhost:8501
```

---

# Implementation Notes

- Prefer additive changes over rewrites.
- Keep existing verified Coinbase files intact unless tests prove they need changes.
- Avoid deleting old strategy code until registry/runtime confirms which modules are used.
- Keep UI read-only until API/event state is reliable.
- Keep live trading disabled until manual approval after the first milestone.
- If Coinbase CLI behavior differs inside container, fix entrypoint/credential handling before touching trading logic.
