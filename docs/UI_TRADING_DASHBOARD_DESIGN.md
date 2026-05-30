# UI Trading Dashboard Product Design

This document defines the target operator interface for the portfolio-management system. The current backend is still paper-first and live-disabled, but the UI should be designed around the eventual full workflow: research agents identify opportunities, backtests and risk checks run automatically, the operator approves/rejects, and only certified execution paths can place or simulate orders.

## Product principle

The UI is not just a chart screen. It is the command center for a semi-autonomous trading operation.

Every screen should answer four questions:

1. What is happening now?
2. What capital is at risk?
3. Why did the system recommend this action?
4. What approval or intervention is required from the operator?

## Primary navigation

Recommended top-level tabs:

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

The first implementation does not need to complete every tab, but the route structure and data model should anticipate these surfaces.

## Overview tab

Purpose: single-page operator cockpit.

Required widgets:

- Total NAV
- Daily P/L
- Realized P/L
- Unrealized P/L
- Available cash
- Locked/risk-reserved cash
- Available liquidity by venue
- Open exposure by asset class
- Open exposure by venue
- Open execution count
- Pending approval count
- Active strategy count
- Agent spend today
- Agent spend this month
- Loss limit usage
- Kill-switch status
- Production-paper readiness status
- Audit-chain integrity status

Operator actions:

- Toggle global kill switch
- Stop all paper executions
- Open pending approvals
- Open risk dashboard
- Run production-paper smoke check

## Portfolio tab

Purpose: show account, position, liquidity, and capital mobility state.

Required sections:

- Accounts table
- Holdings table
- Position P/L table
- Liquidity distribution
- Capital allocation by strategy
- Capital allocation by venue
- Locked funds / reserved margin
- Cash drag
- Concentration warnings
- Exposure limits

Position row fields:

- Symbol
- Venue
- Asset class
- Quantity
- Average cost
- Mark price
- Market value
- Unrealized P/L
- Realized P/L
- Day P/L
- Strategy source
- Risk tier
- Stop loss
- Take profit
- Last updated

Future integrations:

- Plaid account and holdings ingest
- Coinbase spot balances
- Broker account holdings
- On-chain wallet balances
- Polymarket open positions

## Live Markets tab

Purpose: quote board and market data explorer.

Required sections:

- Ticker watchlist
- Bid/ask spread
- Last price
- Volume
- Liquidity depth
- Volatility
- Market regime indicator
- News/research signal status
- Strategy eligibility status

Supported market groups:

- Equities
- Crypto
- Prediction markets
- On-chain liquidity pools
- Macro tickers
- Custom watchlists

Required interactions:

- Add/remove watchlist ticker
- Drill into symbol
- Show recent strategy signals
- Show available historical data coverage
- Show whether symbol is eligible for backtesting
- Show whether symbol is eligible for live execution

## Strategies tab

Purpose: configure, version, compare, and certify strategies.

Required sections:

- Strategy library
- Strategy templates
- Active strategies
- Draft strategies
- Archived/blocked strategies
- Parameter editor
- Version history
- Certification state
- Linked backtests
- Linked approvals
- Linked executions

Strategy row fields:

- Strategy name
- Version
- Status
- Asset class
- Venue
- Risk level
- Capital limit
- Max daily loss
- Max drawdown
- Win probability estimate
- Expected value
- Last backtest score
- Last approved by
- Last execution state

Required actions:

- Create from template
- Clone strategy version
- Edit parameters
- Run backtest
- Request approval
- Promote to paper
- Archive
- Block

## Backtesting tab

Purpose: explain whether a strategy deserves operator attention.

Required sections:

- Backtest run list
- Backtest detail page
- Equity curve
- Drawdown curve
- Trade list
- Assumptions
- Data coverage
- Slippage/fee model
- Out-of-sample split
- Walk-forward results
- Stress scenarios
- Replay diagnostics

Backtest result fields:

- Strategy ID/version
- Symbol/market
- Time range
- Data source
- Start capital
- Ending capital
- Total return
- Sharpe/Sortino
- Max drawdown
- Win rate
- Profit factor
- Average win
- Average loss
- Expected value
- Tail loss estimate
- Slippage estimate
- Fee estimate
- Token/research cost estimate
- Net expected value after agent/model cost

Required actions:

- Re-run backtest
- Compare against previous version
- Promote to approval request
- Reject as not actionable
- Export report

## Opportunities tab

Purpose: operator review feed for all candidate trades.

This should be the highest-value product surface. It is where research agents, market scanners, and strategy engines present candidate trades to the operator.

Opportunity card fields:

- Opportunity ID
- Source agent
- Strategy template/version
- Market/venue
- Symbol or market slug
- Recommendation: buy/sell/hold/market-make/arbitrage/provide-liquidity
- Confidence score
- Win probability
- Loss probability
- Expected value
- Total money risked
- Maximum loss
- Potential upside
- Reward/risk ratio
- Liquidity available
- Required capital
- Time horizon
- Expiration time
- Backtest status
- Approval status
- Execution eligibility
- Agent cost incurred
- Net expected value after research/model cost

Opportunity card layout:

- Header: market, side, confidence, expiry
- Main metrics: risked capital, max loss, upside, EV, win probability
- Status chips: backtested, risk-checked, needs approval, blocked, paper-only
- Expandable risk breakdown
- Expandable backtest summary
- Expandable evidence and research notes
- Expandable token/model cost details
- Approve / Reject / Defer / Request more research buttons

Approval feed behavior:

- Every opportunity must be reviewable before execution.
- Approval must be explicit for any non-trivial capital allocation.
- Rejections should require or allow a reason.
- Deferrals should set a revisit time or price trigger.
- Request-more-research should spawn a bounded-cost agent task.

## Polymarket tab

Purpose: prediction-market-specific opportunity management.

Required sections:

- Market discovery
- Market liquidity board
- Agent-researched opportunities
- Open positions
- Candidate trades
- Resolution calendar
- Event clusters
- Risk by event category
- Market maker opportunities
- Arbitrage opportunities
- Settlement/redeem status

Polymarket opportunity fields:

- Market question
- Event category
- Outcome token
- Current yes/no price
- Fair value estimate
- Edge estimate
- Bid/ask spread
- Available liquidity
- Order book depth
- Estimated slippage
- Expiration/resolution date
- Resolution source
- Dispute/ambiguity score
- Probability estimate
- Win/loss probability
- Capital risked
- Max loss
- Potential upside
- Fees/gas/settlement costs
- Agent research cost
- Net expected value after all costs

Polymarket-specific risk breakdown:

- Liquidity risk
- Resolution ambiguity risk
- Correlated event risk
- Time-to-resolution risk
- Slippage risk
- Counterparty/venue risk
- Agent confidence risk
- Data freshness risk

Actions:

- Approve paper trade
- Reject
- Add to watchlist
- Request deeper research
- Run/re-run backtest or historical analog replay
- Simulate position sizing
- Lock capital reserve

Live order submission must remain blocked until a separate live certification release.

## Agents tab

Purpose: monitor and control daily analysis agents.

Required sections:

- Agent roster
- Running jobs
- Completed research
- Failed jobs
- Token/cost budget
- Cost by model
- Cost by agent
- Cost by market
- Cost by accepted opportunity
- Cost by rejected opportunity
- Net EV after model cost
- Daily loss from research spend

Agent job fields:

- Job ID
- Agent name
- Trigger type: schedule, operator request, market trigger, backtest trigger
- Market/symbol scope
- Model used
- Local/remote provider
- Prompt tokens
- Completion tokens
- Estimated local compute cost
- Remote API cost
- Wall-clock runtime
- Result status
- Opportunity IDs created
- Cost allocated per opportunity

Required controls:

- Pause agent
- Resume agent
- Set daily budget
- Set per-job token cap
- Set per-market research cap
- Require approval before expensive research
- Kill runaway job
- Mark research result as low quality

## Agent cost and loss accounting

Agent work can lose money even before any trade is placed. Token usage, API calls, local GPU/CPU time, and human review time must be treated as operational spend.

The system should track:

- Daily token spend
- Daily remote model cost
- Daily local model estimated cost
- Cost per research job
- Cost per opportunity generated
- Cost per approved trade
- Cost per rejected trade
- Cost per profitable trade
- Cost per unprofitable trade
- Research cost as a percentage of expected upside
- Research cost as a percentage of realized P/L

Local model token cost TODO:

- Add a token calculation model for local inference.
- Estimate local cost from tokens/sec, watts, runtime, hardware depreciation, and electricity cost.
- Store local model cost as estimated operational cost even when no external API fee exists.
- Include local model cost in net EV calculations.
- Display local-vs-remote model cost in the Agents tab.

Minimum local cost formula:

```text
local_model_cost = runtime_hours * estimated_watts / 1000 * electricity_rate_per_kwh + hardware_depreciation_per_hour * runtime_hours
```

Minimum opportunity net EV formula:

```text
net_expected_value = gross_expected_value - estimated_fees - estimated_slippage - estimated_gas - agent_research_cost - model_inference_cost
```

## Risk tab

Purpose: explain portfolio, strategy, venue, and opportunity risk.

Required risk surfaces:

- Aggregate portfolio risk
- Daily loss limit usage
- Weekly loss limit usage
- Drawdown state
- Venue exposure
- Strategy exposure
- Correlated exposure
- Liquidity risk
- Slippage risk
- Model/research confidence risk
- Agent cost risk
- Tail risk

Opportunity risk breakdown should be collapsible and include:

- Capital at risk
- Max loss
- Expected loss
- Expected upside
- Win probability
- Loss probability
- Confidence interval
- Liquidity score
- Slippage estimate
- Backtest quality score
- Data freshness score
- Strategy certification status
- Agent research cost
- Net EV after costs

## Approvals tab

Purpose: queue of decisions requiring human action.

Approval types:

- Strategy approval
- Backtest approval
- Paper execution approval
- Opportunity trade approval
- Agent budget approval
- Expensive research approval
- Kill-switch override approval

Approval card fields:

- Requested action
- Requesting agent/strategy
- Capital required
- Max loss
- Potential upside
- Net EV after costs
- Win probability
- Risk score
- Evidence summary
- Backtest link
- Audit link
- Approve/reject/defer controls

## Executions tab

Purpose: paper/live-disabled execution monitoring.

Required sections:

- Running paper executions
- Stopped executions
- Open orders/simulated orders
- Fills
- Reconciliation status
- Failed execution attempts
- Blocked live execution attempts
- Kill-switch events

Execution fields:

- Execution ID
- Strategy ID
- Approval ID
- Venue
- Symbol/market
- Side
- Quantity
- Preview price
- Fill price
- Slippage
- Fees
- Status
- Last heartbeat
- Stop reason

## Audit tab

Purpose: trust and forensic review.

Required sections:

- Audit event timeline
- Audit hash-chain verification
- Approval history
- Rejected opportunities
- Blocked live attempts
- Agent actions
- Risk-limit events
- Config changes

Required actions:

- Verify audit chain
- Export audit window
- Filter by actor/strategy/market/action
- Open linked opportunity/backtest/execution

## Required backend/API implications

The UI requires backend resources that are not fully implemented yet.

Needed domain objects:

- Opportunity
- MarketDataSnapshot
- ResearchJob
- AgentBudget
- AgentCostLedger
- RiskBreakdown
- ApprovalDecision
- ExecutionPreview
- ExecutionFill
- StrategyCertification
- ModelCostProfile

Needed API groups:

- `/api/opportunities`
- `/api/opportunities/:id`
- `/api/opportunities/:id/approve`
- `/api/opportunities/:id/reject`
- `/api/opportunities/:id/defer`
- `/api/opportunities/:id/request-research`
- `/api/polymarket/markets`
- `/api/polymarket/opportunities`
- `/api/market-data/snapshots`
- `/api/agents/jobs`
- `/api/agents/budgets`
- `/api/agents/costs`
- `/api/risk/breakdowns/:scope/:id`
- `/api/portfolio/liquidity`
- `/api/portfolio/pnl`

## P0 UI implementation sequence

1. Add static route shell and navigation.
2. Add Overview tab using current summary endpoints.
3. Add Strategies and Backtesting tabs using existing endpoints.
4. Add Approvals tab using existing approval endpoints.
5. Add Executions tab using existing paper execution endpoints.
6. Add Audit tab using `/api/audit` and `/api/audit/verify`.
7. Add placeholder Opportunities and Polymarket tabs with empty-state explanations and TODO links.
8. Add Agents tab with budget/cost placeholders.

## P1 UI implementation sequence

1. Implement Opportunity domain object and API.
2. Implement opportunity approval/reject/defer workflow.
3. Implement Polymarket market/opportunity ingest stubs.
4. Implement risk breakdown API.
5. Implement agent research job ledger.
6. Implement token/model cost ledger.
7. Implement net EV after cost calculations.
8. Add opportunity cards with collapsible risk/backtest/evidence/cost panels.

## P2 UI implementation sequence

1. Add live market data adapters.
2. Add streaming updates via SSE or WebSockets.
3. Add portfolio P/L and liquidity dashboards.
4. Add Polymarket order book/liquidity views.
5. Add daily agent budget enforcement.
6. Add research spend loss accounting.
7. Add strategy certification gating in the UI.
8. Add execution replay and audit export.

## Non-negotiable safety constraints

- UI must not expose live execution controls until live certification exists.
- Any live-looking control must clearly say paper-only or blocked.
- Every opportunity must show max loss and net EV after model/research costs.
- Any expensive agent action must be budgeted and auditable.
- Approval/rejection decisions must be written to audit.
- Kill switch must remain visible from primary operator surfaces.
