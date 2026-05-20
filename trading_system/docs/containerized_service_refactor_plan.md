# Containerized Trading Service Refactor Plan

## Objectives
- Turn the imported `trading_system` module set into a reliable, containerized multi-service runtime.
- Preserve strict safety controls (approval gates, trust scoring, mode constraints) while enabling autonomous/agentic strategy orchestration.
- Make the stack operable in local dev, CI, and staged production with explicit promotion criteria.

## Current State Review

### Runtime entrypoints and service boundaries
- API service exists with control endpoints, strategy catalog, risk toggles, onchain analysis, and ops router integration (`apps/api/main.py`).
- Worker service is currently a heartbeat loop and not yet wired to queues, strategy runners, approvals, or execution adapters (`apps/worker/main.py`).
- Additional executables exist for backtesting, replay, and paper exchange and should be modeled as job-style services rather than always-on daemons (`apps/backtester/runner.py`, `apps/replay_engine/runner.py`, `apps/paper_exchange/runner.py`).

### Configuration and safety controls
- Environment-driven typed settings already enforce critical safety constraints for live/canary mode (`core/config/settings.py`).
- Config presets are extensive (`configs/*.yaml`) but lack a canonical environment mapping table and startup validation pass that confirms config+env coherence before trading runtime.

### Containerization baseline
- Existing compose includes `postgres`, `redis`, and `api`, but no worker container, no migration/init job, and no profiles for backtest/replay (`docker-compose.yml`).
- Build/run assumptions are dev-oriented (`volumes: .:/app`) and need production-hardened image strategy and health/readiness semantics.

### Data and persistence
- README flags partial alembic scaffolding (`alembic/env.py` missing), creating deployment risk for persistent state evolution (`README.md`, `alembic/versions/0001_initial.py`).
- Ops-layer state is noted as in-memory only, which blocks resilient control-plane behavior across restarts (`README.md`, `apps/api/ops_layer.py`).

### Test and operability posture
- Broad test suites exist across unit/integration/replay/perf smoke (`tests/`), and `make ci` path is documented (`Makefile`, `docs/testing/TEST_PLAN.md`).
- No repository CI workflow is currently defined in the imported module docs, so promotion controls are likely manual and inconsistent (`README.md`).

## Target Architecture (Containerized Service Model)

### Core long-running services
1. **control-api**
   - FastAPI control plane for mode management, approvals, telemetry, strategy lifecycle API.
2. **strategy-worker**
   - Queue-driven executor handling signals, risk checks, approval workflow state machine, and order dispatch.
3. **marketdata-ingestor**
   - Optional service for websocket/rest normalization into Redis/Postgres topics/tables.
4. **scheduler-orchestrator**
   - Triggers periodic tasks (rebalance windows, reconciliation checks, health probes, report generation).

### Supporting services
- `postgres` for durable state.
- `redis` for queue/event bus/caching.
- `migration` one-shot job for Alembic migrations before API/worker startup.
- `replay-runner` and `backtest-runner` as profile-based ephemeral jobs.

### Control-plane and agentic layer
- Introduce an **Agent Controller** abstraction that sits above strategy execution:
  - policy-constrained action planning,
  - explainability payload generation,
  - approval escalation/auto-approval gates,
  - rollback/kill-switch semantics.
- Represent agent actions as typed intents (enable strategy, adjust allocation, widen spread, pause symbol, unwind, etc.) with required safety evidence fields.

## Refactor Workstreams

### 1) Packaging and process model
- Add production Dockerfile with multi-stage build (builder + slim runtime).
- Make each app entrypoint runnable via explicit commands:
  - `python -m apps.api.main`
  - `python -m apps.worker.main`
  - `python -m apps.scheduler.main` (new)
- Add graceful shutdown hooks and signal handling across long-running loops.

### 2) Compose and deployment topology
- Expand compose with:
  - `api`, `worker`, `scheduler`, `postgres`, `redis`, `migrate` services,
  - healthchecks for API and dependencies,
  - startup ordering with readiness gates,
  - profiles: `dev`, `paper`, `replay`, `backtest`.
- Remove dev-bind mount requirement for non-dev profiles.

### 3) Configuration hardening
- Create config contract doc: env var + yaml preset mapping + allowed mode matrix.
- Add startup validator that fails fast when:
  - live mode lacks explicit approvals/credentials,
  - queue model mismatches runtime module availability,
  - required symbols/venues are missing.
- Add `.env.example` and per-profile env files.

### 4) State and workflow persistence
- Complete Alembic runtime (`alembic/env.py`) and baseline schema for:
  - strategy state,
  - approvals,
  - execution intents,
  - incidents/events,
  - reconciliation snapshots.
- Move in-memory ops state to durable store with optional Redis cache.

### 5) Worker and queue execution model
- Replace heartbeat worker with queue consumers:
  - signal intake,
  - pre-trade risk checks,
  - approval transitions,
  - execution routing (paper/live adapters),
  - post-trade attribution emit.
- Introduce idempotency keys and exactly-once-ish handling via dedup tables/locks.

### 6) Agentic strategy control
- Build an `agent_control` module with:
  - **Intent schema** (Pydantic models),
  - **Policy engine** mapping intents to allowed actions by mode,
  - **Plan evaluator** that simulates expected risk/impact,
  - **Execution adapter** that calls existing registry/risk/execution hooks.
- Require explainability envelope for every autonomous action:
  - hypothesis,
  - expected edge,
  - bounded downside,
  - fallback path,
  - confidence score.
- Add human-in-the-loop thresholds for notional, volatility regime, or trust degradation.

### 7) Observability and incident response
- Standardize structured logs across API/worker with correlation IDs.
- Add metrics endpoints/counters for:
  - risk blocks,
  - approval latencies,
  - fill quality,
  - strategy PnL by regime,
  - agent override frequency.
- Emit incident bundles compatible with existing examples in `artifacts/onchain/`.

### 8) CI/CD and quality gates
- Add CI workflow with staged checks:
  1. lint/type/unit,
  2. integration (api health + ops api),
  3. replay regression,
  4. container build smoke.
- Enforce branch protection on CI status + required test matrix subset.

## Phased Delivery Plan

### Phase 0 — Discovery + contracts (1 week)
- Finalize service boundaries, queue topology, and state schema.
- Define agent intent/action matrix and safety rules by trading mode.

### Phase 1 — Container/runtime foundation (1–2 weeks)
- Dockerfile + compose refactor + healthchecks + migration job.
- Worker process skeleton with queue plumbing and graceful lifecycle.

### Phase 2 — Persistence + control plane (1–2 weeks)
- Alembic completion + ops state persistence.
- API endpoints for strategy lifecycle and approval status backed by DB.

### Phase 3 — Agentic control MVP (2 weeks)
- Agent intent schema, policy checks, plan/evaluate/execute pipeline.
- Paper-mode-only autonomous actions with strict guardrails.

### Phase 4 — Hardening + promotion (ongoing)
- Replay/backtest calibration loop, alerting, incident drills.
- Canary rollout controls and progressive notional limits.

## Immediate Enhancements Recommended Before Enabling Agentic Execution
- Implement durable audit trail for all state transitions and decisions.
- Add deterministic replay harness for every agent action type.
- Add explicit kill-switch endpoint and out-of-band operator command path.
- Enforce strategy-level position/notional caps independent of agent logic.
- Add trust-score decay response automation (auto-pause on sustained untrusted state).

## Definition of Done (Containerized Agent-Ready Service)
- One-command profile-based bring-up for local paper trading.
- API + worker + scheduler healthy with migrations applied automatically.
- Agent actions constrained by policy and fully auditable.
- Replay suite demonstrates no regression for risk/approval invariants.
- Canary live mode available behind explicit environment/approval flags.
