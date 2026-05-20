# Asset Management + Liquidity Optimization Layer

## Overview

This pass adds an operator-focused Coinbase-centric portfolio operations layer with:

- FastAPI endpoint surface for dashboard, portfolios, treasury preview/execute, liquidity map/recommendations, order preview/submit/cancel, and oversight pages.
- Deterministic liquidity optimization scoring for capital mobility recommendations.
- Read-model contract definitions intended for websocket/event-driven UI hydration.
- Seeded institutional-like data for portfolios, sleeves, allocations, liquidity, approvals, alerts, incidents, and strategy outcomes.
- Dashboard hardening for anticipated feed and data-quality failures.

## API Surface

All endpoints are mounted under `/ops`.

Core groups:

- Dashboard: `/ops/dashboard/snapshot`, `/ops/dashboard/delta`
- Realtime feed health: `/ops/feeds/health`
- Portfolios: `/ops/portfolios`, `/ops/portfolios/{id}`
- Treasury workflow: `/ops/treasury/preview`, `/ops/treasury/execute`
- Liquidity: `/ops/liquidity/map`, `/ops/liquidity/recommendations`
- Orders and execution: `/ops/orders/preview`, `/ops/orders/submit`, `/ops/orders/open`, `/ops/orders/{id}/cancel`, `/ops/fills`
- Strategy operations: `/ops/strategies/backtest/start`, `/ops/strategies/{id}/start`, `/ops/strategies/outcomes/realtime`
- UI ergonomics: `/ops/ui/theme`, `/ops/ui/labels`
- Oversight: `/ops/risk/summary`, `/ops/approvals`, `/ops/alerts`, `/ops/incidents`, `/ops/audit`

## Dashboard Reliability + Error Visibility

The dashboard snapshot now includes:

- `feed_health` entries with freshness, update rate, dropped messages, and failover state.
- `active_issues` generated from degraded/stale feeds and capital threshold checks.
- `quick_actions` that map directly to safe, preview-first operational actions.
- Liquidity utilization diagnostics (`liquidity_availability_score`, `idle_capital_score`, `working_capital_score`).

This supports stress-time scanning and rapid operator triage.

## Realtime Feed Optimization Standards

For low-latency production operation:

1. Feed-level freshness and drop counters should be materialized from websocket ingest workers.
2. Dashboard should hydrate from denormalized read models, not from fan-out joins.
3. Delta endpoint (`/ops/dashboard/delta`) should be polled or pushed at short intervals to avoid full snapshot churn.
4. UI rendering should keep dense tables compact and chart rendering in canvas mode for lower overhead.

## Treasury Safety Contract

Treasury actions follow preview-first semantics:

1. Submit transfer intent to preview endpoint.
2. Receive approval requirements and impact estimates.
3. Execute with preview id lineage.
4. Emit immutable audit event with source, destination, amount and timestamp.

## Strategy Backtest + Runtime Actions

Operators can now:

- Queue a backtest run from dashboard actions.
- Start a strategy run.
- Inspect realtime strategy outcomes (PnL, fill quality, consumed capital, and latest decisions).

All action launches emit audit events.

## Dark Mode + Lightweight Theme

The UI contract explicitly provides a performance-oriented default theme:

- `mode=dark`
- `lightweight=true`
- reduced animations
- compact tables
- canvas chart rendering

This reduces visual overhead and helps preserve interaction latency during high update rates.

## Liquidity Optimization Methodology

`LiquidityOptimizer` computes three operator-facing scores:

- `usefulness`: suitability of an asset for deployment.
- `productivity`: expected productive deployment quality after accounting for idle ratio and friction.
- `transfer_necessity`: urgency to move idle capital.

Inputs include idle/working balances, depth score, spread opportunity, friction, hedgeability, and available risk budget.

## Known Limitations

- Current implementation is in-memory; production should persist in read stores and event logs.
- Websocket stream and partial update buses are represented through feed-health contracts and delta semantics but not fully wired in this pass.
- Market microstructure metrics are synthetic seed values.
