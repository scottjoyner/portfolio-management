# Paper Trading Status Report

**Last Updated:** June 2026  
**System:** Unified Execution Engine + Broker Adapters + Coinbase Bridge

---

## Current State Summary

Paper trading is fully operational through the unified execution engine. The system uses a state machine (draft → approved → submitted → filled|cancelled|failed) with confidence-scored gating, approval workflow, and multiple broker adapters.

### Core Components

| Component | Status | Notes |
|-----------|--------|-------|
| ExecutionEngine | ✅ Complete | State machine, confidence gating, retry logic, event emission |
| ConfidenceScorer | ✅ Complete | 5-dimension scoring (strategyConviction, marketCondition, riskAssessment, historicalPerformance, dataFreshness) |
| PaperBrokerAdapter | ✅ Complete | In-memory paper trading with cash/position tracking, fee/slippage simulation |
| CoinbaseBrokerAdapter | ✅ Complete | Bridges to Python via bridge_execution.py; paper (dry-run) and live modes |
| KalshiBrokerAdapter | ✅ Complete | Kalshi REST client with demo auth (readonly without live keys) |
| PolymarketBrokerAdapter | ✅ Complete | Polymarket CLOB integration (readonly; orders require on-chain sigs) |
| GraphAlphaBotAdapter | ✅ Complete | Consumes Neo4j graph signals as OrderIntents |
| ExecutionReconciler | ✅ Complete | Fill-mismatch detection, settlement tracking, audit event generation |
| SettlementTracker | ✅ Complete | Pending/settled/failed fill lifecycle with retry support |
| AdapterRegistry | ✅ Complete | Singleton registry mapping venues to adapters |

## API Routes

All execution routes live under `apps/api/src/operatorRouter.mjs` and require `Authorization: Bearer op-token-001`:

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/execution/plan | Plan execution without committing |
| POST | /api/execution/execute | Submit execution (confidence-gated, auto-approve or draft) |
| POST | /api/execution/:id/approve | Approve a draft execution |
| POST | /api/execution/:id/reject | Reject a draft execution |
| POST | /api/execution/:id/cancel | Cancel an execution |
| POST | /api/execution/:id/reconcile | Run reconciliation (fill-mismatch check, audit event) |
| POST | /api/execution/:id/settle/:fillId | Settle a specific fill |
| POST | /api/execution/:id/retry-settlement | Retry pending/failed settlements |
| GET | /api/executions | List all executions |
| GET | /api/executions/:id | Get execution details + events |
| GET | /api/executions/:id/events | Get events for an execution |
| GET | /api/execution/events | Get all system events |
| GET | /api/execution/adapters | List registered broker adapters |
| GET | /api/execution/graph-signals | Fetch raw graph-alpha-bot signals |
| POST | /api/execution/graph-signals/ingest | Ingest graph signals as opportunities |

## Confidence Scoring

The ConfidenceScorer (`packages/confidence/src/index.ts`) evaluates five dimensions:

1. **strategyConviction**: 0.0–1.0 — strength of the signal source
2. **marketCondition**: 0.0–1.0 — current market regime favorability
3. **riskAssessment**: 0.0–1.0 — risk-adjusted attractiveness
4. **historicalPerformance**: 0.0–1.0 — backtested win rate / Sharpe
5. **dataFreshness**: 0.0–1.0 — staleness of the underlying data

The default confidence threshold is **0.60**. Orders below threshold are rejected; orders at or above but below `approvalThreshold` (default 0.80) create a draft requiring human approval; orders at or above approval threshold auto-submit.

Conviction weight is computed as `score / totalScoreOfAllInputs` for portfolio-level allocation.

## Broker Adapters

### PaperBrokerAdapter
- In-memory cash/position tracking starting at $100,000 USD
- Fee simulation (configurable bps)
- Slippage simulation (percentage-based)
- Tracks positions with average price, quantity, market value

### CoinbaseBrokerAdapter
- Calls Python bridge at `coinbase/src/bridge_execution.py` as subprocess
- Supports actions: list_accounts, best_bid_ask, preview_order, submit_order, get_candles, health
- Paper mode: returns preview data but does not submit live orders
- Live mode: submits real orders via Coinbase Advanced Trade API V3
- Configurable via env vars: `COINBASE_PYTHON_PATH`, `COINBASE_BRIDGE_SCRIPT`, `COINBASE_DRY_RUN`

### KalshiBrokerAdapter
- REST client in `packages/kalshi/src/client.ts`
- Login with email/password, market list, balance, order CRUD
- Demo fallback when credentials not configured

### PolymarketBrokerAdapter
- CLOB client in `packages/polymarket/src/client.ts`
- Market list, orderbook, price quotes
- Readonly without on-chain signing capability

## Graph-Alpha-Bot Integration

The graph-alpha-bot adapter (`packages/adapters/src/graphAlphaBotAdapter.mjs`) consumes signals from the Neo4j-based graph analysis pipeline:

1. **Fetch signals** — tries signal cache at `~/.hermes/signals/graph-alpha-signals.json`, then Python subprocess, then demo fallback
2. **Convert to opportunities** — `POST /api/execution/graph-signals/ingest` calls `generateOpportunitiesFromGraphSignals()` which creates opportunities in the opportunity pipeline
3. **Flow through approval** — created opportunities appear in the UI opportunity feed, go through approve/reject/defer workflow
4. **Execution** — approved opportunities can be converted to execution orders via the execution engine

## Reconciliation & Settlement

### ExecutionReconciler
- `reconcile(execution)` — compares fills to orders, detects quantity mismatches
- `toAuditEvent(id, report)` — generates an audit-compatible event from a reconciliation report
- Report fields: totalFills, totalOrders, matched, mismatched, pendingSettlement, status

### SettlementTracker
- `trackFill(fill)` — registers a fill for settlement tracking
- `attemptSettle(fill, venueAdapter)` — checks order status on venue, marks settled
- `getSettlementSummary(execution)` — returns counts of total/settled/pending/failed/stale fills
- `retryPending(execution, getAdapter)` — retries all pending/failed fills

## UI Panels

The web UI (`apps/web/src/`) provides:

- **Execution dashboard** — KPI cards (total executions, filled, pending, failed, total fills, settled, pending settlement), execution table with approve/cancel/reconcile/retry buttons
- **Graph-Alpha-Bot panel** — signal table with ingest button
- **Broker adapter panel** — shows registered adapters with venue, mode, connected status
- **Audit panel** — shows all execution/reconciliation/settlement audit events
- **Capital policy editor** — dashboard-configurable reserve/core/opportunity split with conservative/balanced/aggressive presets

## CLI

The CLI (`apps/cli/src/execution.mjs`) provides:

```
list                          List all executions
get <id>                      Get execution details and events
events [id]                   Get events (all or for an execution)
execute '<json>'              Submit an execution request
approve <id>                  Approve a draft execution
reject <id> <reason>          Reject a draft execution
cancel <id>                   Cancel a draft execution
plan '<json>'                 Plan an execution without committing
adapters                      List registered broker adapters
graph-signals                 Fetch and display graph-alpha-bot signals
graph-ingest                  Ingest graph-alpha-bot signals as opportunities
```

## Test Coverage

39 test cases across 3 test suites:
- `tests/execution-engine.test.mjs` — 17 tests (engine lifecycle, confidence gating, approval, events)
- `tests/execution-adapters.test.mjs` — 11 tests (adapter registry, paper broker)
- `tests/execution-recon-settle.test.mjs` — 9 tests (reconciliation, settlement tracker)

All tests pass with clean lint.

## Configuration

```
# Confidence threshold (default 0.60)
CONFIDENCE_THRESHOLD=0.60

# Authorization token
API_TOKEN=op-token-001

# Coinbase bridge (optional, for live Coinbase orders)
COINBASE_PYTHON_PATH=python3
COINBASE_BRIDGE_SCRIPT=coinbase/src/bridge_execution.py
COINBASE_DRY_RUN=true

# Kalshi credentials (optional, demo mode without)
KALSHI_EMAIL=
KALSHI_PASSWORD=

# Polymarket credentials (optional, readonly without)
POLYMARKET_API_KEY=
POLYMARKET_WALLET_ADDRESS=
POLYMARKET_PRIVATE_KEY=

# Capital policy (optional, dashboard-managed)
# reserve/core/opportunity and bucket presets are persisted in optimizer_state.db + operator-state.json
```

## Next Steps

1. Wire the execution engine's reconciliation/settlement into the operator audit trail and UI (done)
2. Connect CoinbaseBrokerAdapter to the live bridge_execution.py and test with real API key
3. Add execution engine metrics (Prometheus counters for submitted/filled/failed) to /metrics.prom
4. Wire graph-alpha-bot signals into the opportunity pipeline so they flow through approval → execution (done)
5. Decide whether to keep the dashboard capital preset defaults as Balanced or switch the starting preset to Conservative/Aggressive
