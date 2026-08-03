# API Contract — Portfolio OS Paper/Guarded Operator Product

This contract covers the current daily operations, competition, research, approval, and paper-execution surface. It does **not** certify live trading.

## Conventions

- JSON responses use `application/json; charset=utf-8`.
- Successful collection routes normally return an object containing the named collection rather than a bare array.
- Protected responses include a request ID when routed through `server.p1.mjs`.
- Unknown or unavailable source evidence remains `null`/`unknown`; it is not converted to zero.
- Runtime timestamps are ISO 8601 unless a source contract explicitly uses Unix seconds.

## Authentication and authorization

Authentication is required when either condition is true:

- `OPERATOR_AUTH_REQUIRED=true`
- `MODE=live`

Configure one or more tokens:

```bash
OPERATOR_AUTH_TOKEN=<operator-token>
OPERATOR_ADMIN_TOKEN=<admin-token>
OPERATOR_PAPER_TOKEN=<paper-token>
OPERATOR_READONLY_TOKEN=<readonly-token>
```

Use:

```http
Authorization: Bearer <token>
X-Request-Id: optional-request-id
```

Roles:

| Role | Access |
|---|---|
| `admin` | All operator routes permitted by the application safety layer. |
| `paper` | All reads plus explicitly allowed paper/backtest/approval workflows. |
| `readonly` | `GET`, `HEAD`, and `OPTIONS` only. |

`/health`, `/ready`, and `/ready/production-paper` remain available without authentication for deployment probes.

## Health, readiness, and observability

| Method | Route | Purpose |
|---|---|---|
| GET | `/health` | Service health and storage status. |
| GET | `/ready` | Fail-closed live-production readiness report. |
| GET | `/ready/production-paper` | Paper-production readiness gate with live trading disabled. |
| GET | `/metrics` | Existing JSON operational counters. |
| GET | `/metrics.prom` | Prometheus-style request/process metrics. |
| GET | `/api/operator/summary` | Counts, storage state, kill switch, runtime summary, and feature flags. |
| GET | `/api/audit/verify` | Verify audit hash-chain integrity. |

## Daily operations read model

These routes power the default operator console.

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/system-truth` | Source-labelled mode, feed, cache, service, paper-book, execution, and terminal truth. |
| GET | `/api/competition` | Validated shared-epoch bot-versus-agent scorecard. |
| GET | `/api/positions` | Operator-store positions, accounts, capital in play, and source label. |
| GET | `/api/executions` | Execution records and lifecycle status. |
| GET | `/api/execution/events` | All execution-engine events. |
| GET | `/api/executions/:id` | One execution plus its events. |
| GET | `/api/executions/:id/events` | Events for one execution. |
| GET | `/api/opportunities` | Pre-trade opportunity/decision records. |
| GET | `/api/activity-feed` | Aggregated audit, execution, and opportunity activity. |
| GET | `/api/agents/costs` | Agent cost ledger and aggregate cost summary. |
| GET | `/api/agents/budgets` | Agent budget policies. |
| GET | `/api/agents/budget-approvals` | Explicit paid-research spend approvals. |
| GET | `/api/market-data/live-quotes` | Current observed quotes for supported crypto symbols. |

## System Truth

### `GET /api/system-truth`

System Truth is source-labelled and fail-closed. The response shape includes:

```json
{
  "generated_at": "2026-07-29T16:00:00.000Z",
  "trading_mode": {
    "value": "paper",
    "source": "system_health_snapshot",
    "status": "ok"
  },
  "feed": {
    "heartbeat": {
      "freshness": "fresh"
    }
  },
  "cache": {
    "status": "ok"
  },
  "services": {},
  "paper_book": {
    "gross_exposure_usd": 230,
    "open_positions": 2,
    "capital_in_play_usd": 123.46,
    "cash_usd": 9876.54,
    "realized_pnl_usd": -12.34,
    "fees_paid_usd": 1.23,
    "state_age_sec": 4.5,
    "status": "ok",
    "source": "paper_trader"
  },
  "execution_decision": {
    "value": "allowed",
    "status": "ok",
    "source": "paper_trader"
  },
  "terminal": {
    "url": "/dashboard",
    "source": "dashboard_default",
    "status": "ok"
  },
  "warnings": []
}
```

When the system-health snapshot is stale, invalid, or incomplete, canonical paper-book and execution-decision fields return source-labelled unknowns rather than operator-store estimates.

## Competition

### `GET /api/competition`

Returns the validated published competition snapshot.

Important fields:

```json
{
  "schema_version": 2,
  "generated_at": "2026-07-29T16:00:00.000Z",
  "status": "ok",
  "source": {
    "file": "competition_state.json",
    "freshness": "fresh",
    "age_seconds": 2.4
  },
  "competitors": {
    "agent": {
      "accounting_version": 2,
      "ranking_eligible": true,
      "starting_capital_usd": 10000,
      "gross_equity_usd": 10120,
      "operating_cost_usd": 25,
      "net_equity_usd": 10095,
      "net_pnl_usd": 95,
      "net_return_pct": 0.95
    },
    "bot": {
      "starting_capital_usd": 10000,
      "gross_equity_usd": 10110,
      "operating_cost_usd": 0,
      "net_equity_usd": 10110,
      "net_pnl_usd": 110,
      "net_return_pct": 1.1
    }
  },
  "standings": {
    "valid_for_ranking": true,
    "leader": "bot",
    "edge_usd": 15,
    "agent_minus_bot_usd": -15,
    "agent_cost_coverage_ratio": 4.8,
    "agent_break_even_gap_usd": 0,
    "agent_alpha_after_cost_pct_points": -0.15,
    "ranking_basis": "net_equity_after_agent_operating_costs",
    "required_agent_accounting_version": 2
  },
  "warnings": []
}
```

The route independently blocks ranking when:

- the snapshot is missing or stale;
- starting capital is not comparable;
- agent accounting version is not 2;
- the agent ledger is not ranking eligible;
- either competitor status/equity is unavailable.

The API does not replace shared-epoch agent cost with a current-day total.

## Positions

### `GET /api/positions`

Returns the operator-store position view:

```json
{
  "ok": true,
  "positions": [
    {
      "symbol": "BTC-USD",
      "venue": "coinbase",
      "quantity": 0.25,
      "averagePrice": 0,
      "markPrice": 0,
      "unrealizedPnl": 0,
      "status": "open"
    }
  ],
  "accounts": [],
  "capitalInPlayUsd": 2500,
  "source": "operator_store"
}
```

Contract notes:

- This route does not claim that operator-store positions are the canonical EventTrader or Hermes books.
- The response preserves missing/zero cost basis and mark values; the server does not invent a price.
- The web client may enrich a recognized symbol using `/api/market-data/live-quotes`.
- Client-side P&L calculation is allowed only when quantity, valid cost basis, and valid mark are all available.

## Market quotes

### `GET /api/market-data/live-quotes`

Returns supported current quotes when the Coinbase execution bridge is available:

```json
{
  "ok": true,
  "quotes": {
    "BTC-USD": {
      "bid": 100000,
      "ask": 100010,
      "mid": 100005,
      "spreadBps": 1
    }
  },
  "ts": 1785340800000
}
```

A bridge failure returns an error response. Consumers must not treat an absent quote as a zero-price mark.

## Product primitives

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/accounts` | List paper/operator account records. |
| GET | `/api/instruments` | List normalized instruments. |
| GET | `/api/strategy-templates` | List strategy templates and parameter schemas. |

## Strategy lifecycle

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/strategies` | List strategies. |
| POST | `/api/strategies` | Create a free-form draft strategy. |
| POST | `/api/strategies/from-template` | Create and validate a strategy from a template. |
| POST | `/api/strategies/:id/clone` | Clone a strategy to a new draft version. |
| POST | `/api/strategies/:id/status` | Set `draft`, `active`, `archived`, or `blocked`. |

## Backtesting and approvals

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/backtests` | List backtest runs. |
| POST | `/api/backtests` | Legacy deterministic demo backtest. |
| POST | `/api/backtests/run` | P1 deterministic backtest with report artifact. |
| GET | `/api/backtests/:id/report` | Metrics, assumptions, trades, curve, and report metadata. |
| GET | `/api/approvals` | List approval requests. |
| POST | `/api/approvals` | Legacy approval request. |
| POST | `/api/approvals/request` | Request approval using evidence. |
| POST | `/api/approvals/:id/decision` | Approve or reject a request. |

Approval decision body:

```json
{
  "status": "approved",
  "reviewer": "operator",
  "reason": "Evidence reviewed"
}
```

## Opportunities, research, and agent costs

These routes create review and paper-workflow records. They do not certify a live order.

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/opportunity-dashboard` | Aggregated opportunity, risk, research, budget, cost, and market state. |
| GET | `/api/opportunities` | List opportunities/decisions. |
| POST | `/api/opportunities` | Create an opportunity and linked risk breakdown. |
| GET | `/api/opportunities/:id` | Opportunity detail and risk breakdown. |
| POST | `/api/opportunities/:id/approve` | Approve for guarded review/paper workflow. |
| POST | `/api/opportunities/:id/reject` | Reject. |
| POST | `/api/opportunities/:id/defer` | Defer. |
| POST | `/api/opportunities/:id/request-research` | Create bounded-cost follow-up research. |
| GET | `/api/risk-breakdowns` | List risk breakdowns. |
| GET/POST | `/api/agents/jobs` | List or create agent research jobs. |
| GET | `/api/agents/budgets` | List budget limits. |
| GET/POST | `/api/agents/budget-approvals` | List or request explicit spend approvals. |
| POST | `/api/agents/budget-approvals/:id/decision` | Approve or reject spend. |
| GET | `/api/agents/costs` | List agent costs and aggregate summary. |
| GET | `/api/market-data/snapshots` | List normalized market snapshots. |
| POST | `/api/connectors/market-data/ingest` | Ingest configured paper/watch snapshots. |
| POST | `/api/opportunities/generate-from-connectors` | Generate opportunities from connector snapshots. |
| POST | `/api/opportunities/generate-from-strategies` | Generate opportunities from strategy signals. |
| POST | `/api/opportunities/generate-from-prediction-markets` | Generate prediction-market opportunities. |
| POST | `/api/opportunities/generate-from-arbitrage` | Generate arbitrage opportunities. |
| GET | `/api/polymarket/opportunities` | List Polymarket/prediction opportunities. |

### Net expected value

```text
netExpectedValue = grossExpectedValue
                   - estimatedFees
                   - estimatedSlippage
                   - estimatedGas
                   - agentResearchCost
                   - modelInferenceCost
```

A positive value is a pre-trade hypothesis, not realized profit.

### Budget approval request

```json
{
  "agentId": "market-research-agent",
  "marketScope": "BTC-USD",
  "projectedCost": 5,
  "projectedTokens": 25000,
  "requestedBy": "operator",
  "reason": "Bounded follow-up research"
}
```

### Budget decision

```json
{
  "status": "approved",
  "reviewer": "risk-manager",
  "approvedCostLimit": 5,
  "approvedTokenLimit": 25000,
  "reason": "Bounded one-off research approved"
}
```

## Execution engine

### Read routes

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/executions` | List execution records. |
| GET | `/api/executions/:id` | Execution plus events. |
| GET | `/api/executions/:id/events` | Execution-specific events. |
| GET | `/api/execution/events` | All execution events. |
| GET | `/api/execution/adapters` | Registered execution adapters and capabilities. |

### Mutation routes

| Method | Route | Purpose |
|---|---|---|
| POST | `/api/execution/plan` | Validate and create an execution plan. |
| POST | `/api/execution/execute` | Submit through the configured guarded execution engine. |
| POST | `/api/execution/:id/approve` | Approve a pending execution. |
| POST | `/api/execution/:id/reject` | Reject with a reason. |
| POST | `/api/execution/:id/cancel` | Cancel an execution. |
| POST | `/api/execution/:id/reconcile` | Reconcile execution state. |
| POST | `/api/execution/:id/settle` | Settle eligible fills. |
| POST | `/api/execution/:id/retry-settlement` | Retry settlement handling. |

A normalized trade plan requires positive finite:

- entry price;
- take-profit price;
- stop-loss price.

Execution records can contain:

- `id`, `status`, `strategyId`, and source/competitor tags;
- symbol, side, confidence, and notional/risk;
- normalized trade plan;
- orders and fills;
- preview data;
- timestamps, errors, and settlement state.

The UI derives lifecycle display from persisted execution status, fills, and execution events. It does not guarantee that every legacy record contains every field.

## Paper execution sessions

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/paper-executions` | List paper sessions. |
| POST | `/api/paper-executions` | Start paper execution for an approved strategy. |
| POST | `/api/paper-executions/:id/stop` | Stop a session. |
| POST | `/api/paper-executions/:id/signal` | Paper signal through preview, fill, account/position update, and reconciliation. |
| POST | `/api/kill-switch/stop-paper` | Stop all paper sessions without changing the global switch. |

## Activity feed

### `GET /api/activity-feed`

Combines recent:

- audit events;
- execution-engine events;
- opportunity creation/approval/rejection activity.

Entries are sorted newest first and include normalized `type`, `action`, `timestamp`, `actor`, and `details` fields when available.

The feed is a navigation aid, not the authoritative source for execution or accounting state.

## Risk, audit, and safety

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/risk-breakdowns` | Opportunity risk records. |
| GET | `/api/audit` | Audit events. |
| GET | `/api/audit/verify` | Audit integrity result. |
| POST | `/api/kill-switch` | Enable/disable global kill switch. Enabling stops paper sessions. |
| POST | `/api/kill-switch/stop-paper` | Stop all paper sessions. |
| Any | `/api/execution/live/*` | Blocked with `live_execution_disabled`. |

## Error behavior

Common error responses:

```json
{
  "ok": false,
  "error": "operator_auth_required",
  "requestId": "req-..."
}
```

or:

```json
{
  "ok": false,
  "errors": ["entry_price_required"]
}
```

Status conventions:

- `400`: invalid request or workflow state.
- `401`: missing/invalid token.
- `403`: role forbidden.
- `404`: referenced record not found.
- `409`: integrity or state conflict when applicable.
- `503`: operator store, adapter, or required runtime evidence unavailable.

## Current limitations

- Live trading remains uncertified and blocked.
- Operator positions are not yet automatically reconciled with both competition ledgers and venue truth.
- Coinbase account sync may provide quantity without cost basis.
- Competition snapshots must be published by the scoreboard process; the API is a validator/reader, not the ledger writer.
- Legacy execution and opportunity records may lack correlation IDs or complete rationale.
- Browser clients with bearer auth require a trusted same-origin auth mechanism or proxy; the static UI does not persist raw credentials.
