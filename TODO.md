# TODO Roadmap

This roadmap tracks the gap between the current production-paper backend scaffold and the target trading-bot operator product.

The current system has meaningful backend scaffolding, paper-mode controls, audit hardening, certification gates, and some UI/API surface. It is **not** a complete trading dashboard, not a complete research-agent system, and not certified for live real-money execution.

Live trading remains blocked until a separate live certification release.

## Current product reality

Completed or partially completed:

- Paper-first operator API
- Strategy templates and strategy lifecycle scaffolding
- Deterministic backtest scaffold
- Approval workflow scaffold
- Paper execution scaffold
- Kill switch
- Audit logging and audit-chain verification
- Production-paper readiness and smoke commands
- First-production certification validation scaffolding
- Deployment docs and runbooks
- Unified execution engine with state machine, confidence scoring, approval workflow
- Broker adapters (Coinbase bridge via Python, Kalshi REST, Polymarket CLOB, Paper)
- Graph-alpha-bot signal integration into opportunity pipeline
- Reconciliation and settlement tracking
- Postgres store support for execution persistence
- CLI for execution management
- UI execution dashboard with approve/cancel/reconcile/retry
- Dashboard-configurable capital policy with reserve/core/opportunity presets
- Operator shell overview with capital policy summary and collapsible opportunity risk breakdowns

Still missing for the product we actually want:

- Real trading dashboard UX
- Broader Settings tab beyond capital policy
- Opportunity review feed
- Polymarket-focused research and opportunity tab
- Live P/L and liquidity dashboard
- Market data adapter layer in the UI
- Daily research-agent workflow
- Agent token/model cost accounting
- Net expected value after model/research costs
- Real historical replay backtesting
- Collapsible risk breakdowns for every opportunity
- Human approval feed for every trade candidate

## P0 — Make the existing product honest and operator-usable

### P0.1 CI failure triage

- [ ] Keep failing GitHub Actions visible.
- [ ] Categorize failures into lint/test/build/deploy-validation groups.
- [ ] Add `docs/CI_FAILURE_TRIAGE.md` with current failures, likely causes, and next actions.
- [ ] Do not mark broken checks as success just to make the repo look clean.

### P0.2 UI route shell

- [ ] Add or refactor the UI into a real operator shell with these tabs:
  - Overview
  - Portfolio
  - Live Markets
  - Strategies
  - Backtesting
  - Opportunities
  - Polymarket
  - Agents
  - Risk
  - Approvals
  - Executions
  - Audit
  - Settings
- [ ] Keep unimplemented tabs visible with clear empty states and TODO links.
- [ ] Make paper-only/live-disabled status visible globally.

### P0.3 Overview tab

- [ ] Use `/api/operator/summary` as the first data source.
- [ ] Show total NAV or placeholder if unavailable.
- [ ] Show daily P/L placeholder until real P/L is implemented.
- [ ] Show realized/unrealized P/L placeholders.
- [ ] Show account cash, locked capital, and liquidity placeholders.
- [ ] Show counts for strategies, backtests, approvals, paper executions, and audit events.
- [ ] Show kill-switch status.
- [ ] Show production-paper readiness.
- [ ] Show audit integrity status.
- [ ] Show agent spend today/month placeholders.

### P0.4 Strategies tab

- [ ] List strategies with version, status, risk level, and parameter summary.
- [ ] Create strategy from template.
- [ ] Clone strategy version.
- [ ] Update lifecycle status.
- [ ] Link strategies to backtests, approvals, and paper executions.

### P0.5 Backtesting tab

- [ ] List backtest runs.
- [ ] Run deterministic P1 backtest.
- [ ] Show metrics, assumptions, trades, and equity curve data.
- [ ] Add clear label that current backtesting is deterministic/scaffolded, not production-grade historical replay.

### P0.6 Approvals tab

- [ ] Present approvals as a feed, not just a table.
- [ ] Show strategy, backtest evidence, requested action, risk level, reason, reviewer, and status.
- [ ] Add approve/reject controls.
- [ ] Prepare layout for future opportunity approvals.

### P0.7 Executions tab

- [ ] List paper executions.
- [ ] Start approved paper strategy.
- [ ] Stop paper execution.
- [ ] Send paper signal.
- [ ] Show simulated fills, slippage, fees, and stop reason.
- [ ] Show blocked live execution attempts.

### P0.8 Audit tab

- [ ] List audit events.
- [ ] Add audit verification panel using `/api/audit/verify`.
- [ ] Show audit chain status.
- [ ] Filter by actor, action, strategy, market, and date.

## P1 — Build the opportunity-review product

### P1.1 Opportunity domain object

- [ ] Add `Opportunity` model/table/store with:
  - id
  - sourceAgentId
  - strategyId/version
  - marketType
  - venue
  - symbol or marketSlug
  - recommendation
  - confidenceScore
  - winProbability
  - lossProbability
  - expectedValue
  - netExpectedValue
  - capitalRequired
  - totalMoneyRisked
  - maxLoss
  - potentialUpside
  - rewardRiskRatio
  - liquidityScore
  - dataFreshnessScore
  - backtestId
  - riskBreakdownId
  - status
  - expiresAt
  - createdAt
  - updatedAt

### P1.2 Opportunity API

- [ ] `GET /api/opportunities`
- [ ] `GET /api/opportunities/:id`
- [ ] `POST /api/opportunities/:id/approve`
- [ ] `POST /api/opportunities/:id/reject`
- [ ] `POST /api/opportunities/:id/defer`
- [ ] `POST /api/opportunities/:id/request-research`
- [ ] Write audit events for approve/reject/defer/research-request.

### P1.3 Opportunity approval feed UI

- [ ] Build card-based opportunity feed.
- [ ] Card must show:
  - market/venue
  - recommendation
  - confidence score
  - win probability
  - loss probability
  - total money risked
  - max loss
  - potential upside
  - gross EV
  - net EV after model/research costs
  - backtest status
  - risk score
  - approval status
- [ ] Add approve/reject/defer/request-more-research controls.

### P1.4 Collapsible opportunity details

- [ ] Risk breakdown panel.
- [ ] Backtest summary panel.
- [ ] Evidence/research notes panel.
- [ ] Agent/model cost panel.
- [ ] Execution preview panel.

Risk breakdown must include:

- capital at risk
- max loss
- expected loss
- expected upside
- win/loss probability
- liquidity score
- slippage estimate
- data freshness score
- strategy certification state
- agent cost impact

### P1.5 Polymarket tab v1

- [ ] Add Polymarket market discovery placeholder or read-only ingest.
- [ ] Show market question, category, outcome token, yes/no price, spread, depth, and expiry.
- [ ] Show fair value estimate and edge estimate.
- [ ] Show win/loss probability.
- [ ] Show max loss, total money risked, and potential upside.
- [ ] Show liquidity risk, resolution ambiguity risk, time-to-resolution risk, and slippage risk.
- [ ] Add approve/reject/defer/request-more-research controls.
- [ ] Keep live Polymarket order submission blocked.

### P1.7b Execution engine
- [x] Build runtime execution engine with state machine (draft→approved→submitted→filled|cancelled|failed)
- [x] Implement confidence scoring with configurable threshold (default 0.60)
- [x] Add approval workflow (draft → approve/reject/cancel)
- [x] Create paper broker adapter with cash/position tracking and fee/slippage simulation
- [x] Create Coinbase broker adapter via Python bridge subprocess
- [x] Create Kalshi broker adapter (REST client, demo fallback)
- [x] Create Polymarket broker adapter (CLOB integration, readonly)
- [x] Build adapter registry for lazy-initialized venue adapters
- [x] Add execution event emission and retrieval
- [x] Add plan endpoint (preview without execution)
- [x] Support execution retry logic
- [x] Persist executions in operator store (postgres+file)
- [x] Add execution metrics to operator summary (filled/pending/failed counts)

### P1.7c Graph-alpha-bot signal pipeline
- [x] Create GraphAlphaBotAdapter consuming Neo4j signals
- [x] Implement signal-to-OrderIntent conversion
- [x] Add `POST /api/execution/graph-signals/ingest` route
- [x] Add `GET /api/execution/graph-signals` route
- [x] Wire signals into opportunity pipeline (creates opportunities via createOpportunity)
- [x] Add UI Graph Signals panel with signal table and ingest button
- [x] Add CLI graph-signals and graph-ingest commands

### P1.7d Reconciliation and settlement
- [x] Build ExecutionReconciler for fill-mismatch detection and audit events
- [x] Build SettlementTracker for pending/settled/failed fill lifecycle
- [x] Add reconciliation routes (reconcile, settle fill, retry settlement)
- [x] Emit audit events for reconciliation runs and settlement outcomes
- [x] Show settlement status in UI execution table
- [x] Add settlement KPI cards (total fills, settled, pending)

### P1.7e Broker adapter UI
- [x] Add broker adapter registry panel in UI
- [x] Show adapter name, venue, mode, connected status
- [x] Add CLI adapters command
- [x] Add `GET /api/execution/adapters` API route

### P1.6 Risk breakdown model

- [ ] Add `RiskBreakdown` model/table/store with:
  - scope: portfolio, strategy, opportunity, venue, agent
  - aggregate score
  - capital-at-risk score
  - liquidity score
  - slippage score
  - drawdown score
  - volatility score
  - correlation score
  - model confidence score
  - data freshness score
  - agent cost score
  - explanation
  - generatedAt

### P1.7 Backtest linkage for opportunities

- [ ] Every opportunity should either link to a backtest/replay result or explicitly show `backtest_missing`.
- [ ] Approval should be blocked or loudly warned when no backtest exists.
- [ ] Show backtest summary directly inside the opportunity card.

## P2 — Daily research-agent workflow and cost accounting

### P2.1 Research job ledger

- [ ] Add `ResearchJob` model/table/store with:
  - id
  - agentId
  - triggerType
  - marketScope
  - symbolScope
  - provider
  - model
  - status
  - startedAt
  - completedAt
  - promptTokens
  - completionTokens
  - totalTokens
  - estimatedRemoteCost
  - estimatedLocalCost
  - opportunityIdsCreated
  - failureReason

### P2.2 Agent budget model

- [ ] Add `AgentBudget` model/table/store with:
  - agentId
  - dailyTokenLimit
  - dailyCostLimit
  - perJobTokenLimit
  - perMarketCostLimit
  - requireApprovalAboveCost
  - enabled

### P2.3 Agent cost ledger

- [ ] Add `AgentCostLedger` model/table/store with:
  - id
  - agentId
  - jobId
  - model
  - provider
  - localOrRemote
  - promptTokens
  - completionTokens
  - totalTokens
  - remoteApiCost
  - localComputeCost
  - allocatedOpportunityId
  - createdAt

### P2.4 Local model token cost calculation

- [ ] Implement cost estimates for local models, even when there is no API bill.
- [ ] Estimate local cost from tokens/sec, watts, runtime, hardware depreciation, and electricity cost.
- [ ] Store local model cost as estimated operational cost.
- [ ] Include local model cost in opportunity net EV calculations.
- [ ] Display local-vs-remote model cost in Agents tab.

Minimum formula:

```text
local_model_cost = runtime_hours * estimated_watts / 1000 * electricity_rate_per_kwh + hardware_depreciation_per_hour * runtime_hours
```

### P2.5 Net expected value after all costs

- [ ] Compute gross EV.
- [ ] Subtract fees.
- [ ] Subtract slippage.
- [ ] Subtract gas/settlement costs.
- [ ] Subtract agent research cost.
- [ ] Subtract remote model API cost.
- [ ] Subtract local model compute cost.
- [ ] Show both gross EV and net EV on opportunity cards.

Formula:

```text
net_expected_value = gross_expected_value - estimated_fees - estimated_slippage - estimated_gas - agent_research_cost - model_inference_cost
```

### P2.6 Agent spend as loss metric

- [ ] Treat research spend as operational loss until linked to profitable outcomes.
- [ ] Track daily research spend.
- [ ] Track weekly research spend.
- [ ] Track cost per opportunity generated.
- [ ] Track cost per approved opportunity.
- [ ] Track cost per rejected opportunity.
- [ ] Track cost per profitable trade.
- [ ] Track cost per losing trade.
- [ ] Track research cost as percent of expected upside.
- [ ] Track research cost as percent of realized P/L.

### P2.7 Expensive research approval gate

- [ ] Agents must request approval before exceeding configured per-job or daily budgets.
- [ ] Operator can approve/reject additional research spend.
- [ ] Budget approvals must be audited.

## P3 — Live market and portfolio dashboard

### P3.1 Live P/L and liquidity view

- [ ] Total NAV.
- [ ] Daily P/L.
- [ ] Realized P/L.
- [ ] Unrealized P/L.
- [ ] Cash.
- [ ] Locked/reserved capital.
- [ ] Liquidity by venue.
- [ ] Exposure by asset class.
- [ ] Exposure by strategy.
- [ ] Open risk by opportunity.

### P3.2 Market data adapters

- [ ] Add normalized `MarketDataSnapshot` model with bid, ask, spread, last, volume, depth, volatility, timestamp, and source.
- [ ] Add live quote board.
- [ ] Add historical data coverage indicators.

### P3.3 Streaming updates

- [ ] SSE or WebSocket stream for market data.
- [ ] Stream account/liquidity changes.
- [ ] Stream opportunity status.
- [ ] Stream backtest completion.
- [ ] Stream approval changes.
- [ ] Stream execution state.
- [ ] Stream agent job state.

### P3.4 Polymarket liquidity and order book UI

- [ ] Show yes/no prices.
- [ ] Show spread and depth.
- [ ] Show liquidity available at target size.
- [ ] Show slippage at proposed order size.
- [ ] Show resolution date and ambiguity score.

## P4 — Live certification later

### P4.1 Live execution remains blocked

- [ ] Do not expose live order buttons until a separate certification release.
- [ ] Keep all live controls disabled or hidden behind certification state.
- [ ] Continue surfacing blocked live attempts in audit.

### P4.2 Broker/venue adapter contract tests

- [ ] Add adapter-specific contract tests before any live execution.
- [ ] Require paper certification before live certification.
- [ ] Require kill-switch and reconciliation tests.

### P4.3 External/WORM audit sink

- [ ] Add append-only audit write path.
- [ ] Add external immutable audit sink.
- [ ] Add export and verification process.

## Immediate next branch recommendation

```text
codex/ui-opportunity-dashboard-design
```

First implementation target:

- UI route shell
- Overview tab
- Opportunities tab scaffold
- Polymarket tab scaffold
- Agents tab scaffold
- Audit verification panel
- Risk and cost metric placeholders

Do not start live execution work yet.
