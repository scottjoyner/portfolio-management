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

`/health` and `/ready` remain available without auth so deployment tooling can check service status.

## Health and readiness

| Method | Route | Purpose |
|---|---|---|
| GET | `/health` | Service health and storage status. |
| GET | `/ready` | Fail-closed readiness report. |
| GET | `/metrics` | Basic operational counters. |
| GET | `/api/operator/summary` | UI summary, counts, storage status, kill-switch state, and feature flags. |
| GET | `/api/release/status` | First-prod paper-only release posture, blockers, audit status, runtime posture, and enabled capabilities. |

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
| GET | `/api/audit/verify` | Verify hash-chained audit events and return chain issues, count, and last hash. |
| POST | `/api/kill-switch` | Toggle kill switch. Enabling it stops running paper execution sessions. |
| Any | `/api/execution/live/*` | Always blocked with `live_execution_disabled`. |

## Current limitations

- Backtests are deterministic product simulations, not production-grade market replay.
- Paper execution now includes preview/fill/account/position/reconciliation mechanics, but it is still a paper-only simulator.
- Postgres product-layer persistence uses row tables, while broader core state still has additional row-level hardening remaining.
- Live trading remains explicitly uncertified and blocked.
