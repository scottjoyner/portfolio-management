# API Contract — P0/P1/P2 Operator Product

This contract covers the current mock/paper operator surface. It is intentionally not a live-trading API.

## Auth

Dev/mock mode allows local access by default. Protected operator routes require a bearer token when either condition is true:

- `OPERATOR_AUTH_REQUIRED=true`
- `MODE=live`

Configure:

```bash
OPERATOR_AUTH_REQUIRED=true
OPERATOR_AUTH_TOKEN=<strong-token>
```

Use:

```http
Authorization: Bearer <strong-token>
X-Request-Id: optional-request-id
```

`/health`, `/ready`, and `/ready/production-paper` remain available without auth so deployment tooling can check service status.

## Health, readiness, and observability

| Method | Route | Purpose |
|---|---|---|
| GET | `/health` | Service health and storage status. |
| GET | `/ready` | Fail-closed live-production readiness report. |
| GET | `/ready/production-paper` | Paper-production readiness gate for deployments that remain live-disabled. |
| GET | `/metrics` | JSON operational counters for existing API consumers. |
| GET | `/metrics.prom` | Prometheus-style process/request metrics. |
| GET | `/api/operator/summary` | UI summary, counts, storage status, kill-switch state, redacted runtime config, and feature flags. |

## Product primitives

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/accounts` | List local paper/Plaid-sandbox account records. |
| GET | `/api/instruments` | List normalized instruments. |
| GET | `/api/strategy-templates` | List strategy templates and parameter schemas. |

## Strategy lifecycle

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/strategies` | List strategies. |
| POST | `/api/strategies` | Create a free-form draft strategy. |
| POST | `/api/strategies/from-template` | Create strategy from a template and validate parameters. |
| POST | `/api/strategies/:id/clone` | Clone a strategy into a new draft version. |
| POST | `/api/strategies/:id/status` | Update lifecycle status: `draft`, `active`, `archived`, or `blocked`. |

## Backtesting

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/backtests` | List backtest runs. |
| POST | `/api/backtests` | Legacy deterministic demo backtest route. |
| POST | `/api/backtests/run` | P1 deterministic backtest route with report artifact. |
| GET | `/api/backtests/:id/report` | Retrieve metrics, assumptions, trades, equity curve, and report metadata. |

## Approvals

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/approvals` | List approval requests. |
| POST | `/api/approvals` | Legacy approval request route. |
| POST | `/api/approvals/request` | Request approval using backtest evidence. |
| POST | `/api/approvals/:id/decision` | Approve or reject a request. |

Decision body:

```json
{
  "status": "approved",
  "reviewer": "operator",
  "reason": "Backtest evidence reviewed"
}
```

## Opportunity review, research agents, connectors, and cost accounting

These routes power the operator opportunity feed. They are review/paper workflow routes only and do not submit live orders.

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/opportunity-dashboard` | Aggregated opportunity, risk, research, market snapshot, and agent cost state for the UI. |
| GET | `/api/opportunities` | List review opportunities. |
| POST | `/api/opportunities` | Create a review opportunity and linked risk breakdown. |
| GET | `/api/opportunities/:id` | Retrieve opportunity detail with linked risk breakdown. |
| POST | `/api/opportunities/:id/approve` | Mark opportunity as approved for review/paper workflow. |
| POST | `/api/opportunities/:id/reject` | Reject opportunity. |
| POST | `/api/opportunities/:id/defer` | Defer opportunity. |
| POST | `/api/opportunities/:id/request-research` | Create a bounded-cost follow-up research job for an opportunity. |
| GET | `/api/risk-breakdowns` | List risk breakdowns. |
| GET | `/api/agents/jobs` | List research jobs. |
| POST | `/api/agents/jobs` | Create a research job and agent cost ledger row. |
| GET | `/api/agents/budgets` | List agent budget limits. |
| GET | `/api/agents/costs` | List agent cost ledger and aggregate cost summary. |
| GET | `/api/market-data/snapshots` | List normalized market data snapshots. |
| POST | `/api/connectors/market-data/ingest` | Ingest normalized market snapshots from configured paper/watch adapters. |
| POST | `/api/opportunities/generate-from-connectors` | Generate review opportunities from connector snapshots with risk and research records. |
| GET | `/api/polymarket/opportunities` | List prediction-market opportunities. |

Connector generation is idempotent for active candidates: a second run should not duplicate active opportunities for the same symbol and venue.

Opportunity creation validates:

- title/venue/market type are present
- optional strategy/backtest/research job references exist
- numeric risk/cost fields are finite and non-negative
- max loss cannot exceed total money risked when risked capital is positive
- win/loss probabilities must sum to approximately 1
- status must be a known opportunity review status

Research-job creation validates:

- token counts are non-negative
- `totalTokens` is not below prompt + completion tokens
- local/remote mode is valid
- status is valid
- enabled agent budget exists when configured
- per-job token limit
- daily cost limit
- per-market cost limit
- approval threshold for expensive research

Research jobs can pass `approvedBudgetOverride: true` only when an operator has explicitly approved additional spend.

Opportunity body example:

```json
{
  "researchJobId": "job-001",
  "marketType": "prediction_market",
  "venue": "polymarket-watch",
  "symbol": "PREDICTION:DEMO",
  "title": "Demo prediction opportunity",
  "recommendation": "review_yes",
  "confidenceScore": 0.68,
  "winProbability": 0.57,
  "lossProbability": 0.43,
  "grossExpectedValue": 68.4,
  "totalMoneyRisked": 500,
  "maxLoss": 500,
  "potentialUpside": 420,
  "estimatedFees": 5,
  "estimatedSlippage": 10,
  "agentResearchCost": 9.35,
  "modelInferenceCost": 2.9
}
```

Net expected value is computed as:

```text
netExpectedValue = grossExpectedValue - estimatedFees - estimatedSlippage - estimatedGas - agentResearchCost - modelInferenceCost
```

Local model cost is estimated as:

```text
runtime_hours * estimated_watts / 1000 * electricity_rate_per_kwh + hardware_depreciation_per_hour * runtime_hours
```

## Paper execution

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/paper-executions` | List paper execution sessions. |
| POST | `/api/paper-executions` | Start paper execution for an approved strategy. |
| POST | `/api/paper-executions/:id/stop` | Stop a running paper execution. |
| POST | `/api/paper-executions/:id/signal` | Paper-only signal-to-preview-to-fill execution with account/position/reconciliation updates. |
| POST | `/api/kill-switch/stop-paper` | Stop all running paper executions without changing global kill-switch state. |

Paper signal body:

```json
{
  "signal": {
    "symbol": "BTC-USD",
    "side": "buy",
    "quantity": 0.1,
    "price": 50000,
    "feeBps": 5,
    "slippageBps": 10
  }
}
```

## Risk and audit

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/positions` | List positions. |
| GET | `/api/audit` | List audit events. |
| GET | `/api/audit/verify` | Verify audit hash-chain integrity. |
| POST | `/api/kill-switch` | Toggle kill switch. Enabling it stops running paper execution sessions. |
| Any | `/api/execution/live/*` | Always blocked with `live_execution_disabled`. |

## Current limitations

- Backtests are deterministic product simulations, not production-grade market replay.
- Paper execution includes preview/fill/account/position/reconciliation mechanics, but it is still a paper-only simulator.
- Product-layer Postgres state has schema targets for opportunities/research/costs, but the broader store should still be moved toward targeted row-level mutations.
- Market data connector adapters are paper/static watch adapters until real venue adapters are implemented.
- Polymarket opportunities are review records only; live order submission remains blocked.
- Live trading remains explicitly uncertified and blocked.
