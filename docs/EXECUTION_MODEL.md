# Execution Model

## Architecture

The execution system uses a three-layer architecture:

```
[API / CLI / UI]  →  ExecutionEngine  →  BrokerAdapter  →  [Exchange / Bridge]
                          │
                    ConfidenceScorer
                          │
                 (gates execution)
                          │
                    Reconciliation
                    SettlementTracker
```

### ExecutionEngine (`packages/execution/src/executionEngine.mjs`)

The core state machine with the following lifecycle:

```
draft ──approve──→ approved ──submit──→ submitted ──filled──→ filled
  │                    │                     ├──cancelled──→ cancelled
  ├──reject──→ rejected │                     └──failed────→ failed
  └──cancel──→ cancelled│
                         └──reject──→ rejected
```

- **draft**: Created when confidence is above threshold but below approval threshold, or when `requireApproval` is set. Awaiting human approval.
- **approved**: Human approved via `POST /api/execution/:id/approve`. Engine submits to broker.
- **submitted**: Sent to broker adapter. Waiting for fill/cancel/fail.
- **partially_filled**: Some fills received, still active.
- **filled**: All fills complete.
- **cancelled**: Cancelled by operator or system.
- **failed**: Broker rejected or execution error.
- **rejected**: Confidence check failed or human rejected.

### ConfidenceScorer (`packages/confidence/src/index.ts`)

Five-dimension scoring engine:

```
totalScore = w1*s1 + w2*s2 + w3*s3 + w4*s4 + w5*s5

where s1..s5 are:
  strategyConviction    — strength of signal source (default 0.30 weight)
  marketCondition       — current market regime (default 0.20 weight)
  riskAssessment        — risk-adjusted attractiveness (default 0.20 weight)
  historicalPerformance — backtested track record (default 0.15 weight)
  dataFreshness         — data staleness penalty (default 0.15 weight)
```

Decision logic:
- `score < minConfidence` (0.60): REJECTED, no execution
- `minConfidence <= score < approvalThreshold` (0.80): CREATES DRAFT, requires human approve
- `score >= approvalThreshold`: AUTO-SUBMITTED to broker

### Broker Adapters (`packages/adapters/src/`)

Each adapter implements a common interface:

```
health()                      → { ok, venue, latencyMs, mode, authenticated }
getAccounts()                 → [{ id, name, balance, currency }]
getBalances()                 → [{ currency, available, hold }]
discoverMarkets()             → [{ symbol, baseAsset, quoteAsset, status }]
getOrderBook(symbol)          → { bids, asks }
getQuote(symbol)              → { bid, ask, spread }
previewOrder(order)           → { estimatedFill, fees, slippage }
submitOrder(order)            → { orderId, status, fills }
cancelOrder(orderId)          → { ok }
getOrderStatus(orderId)       → { status, filled, remaining }
getHistoricalRates(symbol)    → [{ timestamp, open, high, low, close, volume }]
```

Venues:
- **paper**: In-memory trading with $100K starting cash
- **coinbase**: Python bridge subprocess to `coinbase/src/bridge_execution.py`
- **kalshi**: REST client to kalshi.com API (demo/live)
- **polymarket**: CLOB client to polymarket.io API (readonly)

Current operational note:
- The active paper trader is still crypto-spot oriented and behaves long-biased unless a separate bearish-regime policy suppresses longs.
- Future adapters must make intent type explicit (`LONG`, `SHORT`, `HEDGE`, `FLAT`) so downtrends in BTC / SPX can be expressed as shorts or defensive allocation instead of only sell-to-flat behavior.

## Reconciliation & Settlement

### ExecutionReconciler (`packages/execution/src/reconciliation.mjs`)

Detects discrepancies between expected orders and received fills:

```
reconcile(execution) → {
  executionId,
  totalOrders,
  totalFills,
  matched,
  mismatched,
  pendingSettlement,
  status: 'clean' | 'mismatch_detected' | 'pending_settlement'
}
```

Audit event format:
```
{
  action: 'execution_reconciled',
  details: { executionId, status, matched, mismatched, pendingSettlement }
}
```

### SettlementTracker (`packages/execution/src/settlement.mjs`)

Manages the lifecycle of fill settlement:

```
trackFill(fill) → registers fill with status 'pending'
attemptSettle(fill, adapter) → checks order status on venue
settleFill(fillId) → manually marks as settled
getSettlementSummary(execution) → { totalFills, trackedFills, settled, pending, failed, stale }
retryPending(execution, getAdapter) → retries all pending/failed fills
```

Settlement timeout: 7 days (`SETTLEMENT_TIMEOUT_MS = 7 * 24 * 60 * 60 * 1000`)

## Graph-Alpha-Bot Signal Pipeline

Graph-alpha-bot signals flow through the system as follows:

```
Neo4j / Python pipeline
        │
        ▼
GraphAlphaBotAdapter.fetchSignals()
   (cache → subprocess → demo fallback)
        │
        ▼
POST /api/execution/graph-signals/ingest
        │
        ▼
generateOpportunitiesFromGraphSignals()
   (creates opportunities via createOpportunity pipeline)
        │
        ▼
Opportunity review feed (UI)
        │
        ▼
Approve → ExecutionEngine.execute()
```

## Persistence

Executions are persisted via the operator store:

- **File store**: Executions serialized as JSON in `data/operator-state.json`
- **Postgres store**: Executions stored as JSON blob in `operator_flags` table under key `executions`
- **Memory store**: Executions held in-memory (lost on restart)

The execution engine singleton lives in the operator router (`handleOperatorRoute._execEngine`) — state persists across HTTP requests until server restart.

## API Reference

All execution endpoints under `/api/execution*` and `/api/executions*`:

| Method | Path | Handler | Audit Event |
|--------|------|---------|-------------|
| POST | /api/execution/plan | `engine.plan()` | none |
| POST | /api/execution/execute | `engine.execute()` + mutate store | `execution_submitted` |
| POST | /api/execution/:id/approve | `engine.approve()` + mutate store | `execution_approved` |
| POST | /api/execution/:id/reject | `engine.reject()` + mutate store | `execution_rejected` |
| POST | /api/execution/:id/cancel | `engine.cancel()` + mutate store | `execution_cancelled` |
| POST | /api/execution/:id/reconcile | reconciler + mutate store | `execution_reconciled` |
| POST | /api/execution/:id/settle/:fillId | tracker + mutate store | `fill_settled` |
| POST | /api/execution/:id/retry-settlement | tracker + mutate store | `settlement_retry_completed` |
| GET | /api/executions | `engine.listExecutions()` | none |
| GET | /api/executions/:id | `engine.getExecution()` | none |
| GET | /api/executions/:id/events | `engine.getEvents()` | none |
| GET | /api/execution/events | `engine.getAllEvents()` | none |
| GET | /api/execution/adapters | registry.listAdapters() | none |
| GET | /api/execution/graph-signals | adapter.fetchSignals() | none |
| POST | /api/execution/graph-signals/ingest | generate + mutate store | `graph_signals_ingested` |

## Test Coverage

39 test cases across 3 suites:

- **execution-engine.test.mjs** (17): engine lifecycle, confidence gating, approval flow, event emission, retry logic, fee calculation
- **execution-adapters.test.mjs** (11): adapter registry registration/lookup, paper broker cash/position tracking, order preview/submit
- **execution-recon-settle.test.mjs** (9): reconciler mismatch detection, settlement tracker lifecycle, audit event generation
