# Agentic Evaluation Mechanism Plan

This document extends the trading system into an agentic research, valuation, strategy-evaluation, and approval pipeline. The goal is to let the system continuously evaluate positions, estimate fair market buy/sell/hold levels, backtest proposed strategies, and route profitable strategies through human approval before any trade execution.

## Core principle

The system must separate **recommendation**, **approval**, and **execution**.

```text
data ingestion -> research/evaluation agents -> fair-value model -> strategy hypothesis -> backtest/walk-forward validation -> paper/shadow run -> approval packet -> gated execution adapter
```

No agent is allowed to bypass risk, audit, approval, reconciliation, or mode gates.

## Product boundary: Plaid vs brokers/exchanges

Plaid is account-data infrastructure for banking, investment holdings, securities, balances, and investment transactions. It should be used to link financial institutions, normalize holdings, reconcile cost basis, and observe historical account activity. Plaid should **not** be modeled as the primary stock-trade execution rail.

Actual trade execution requires broker/exchange adapters, such as:

- equities/ETF/options broker adapters: Alpaca, Interactive Brokers, Tradier, Schwab/TDA successor APIs, or other approved broker APIs;
- crypto exchange adapters: Coinbase, Kraken, Gemini, Binance.US where legally/operationally appropriate;
- onchain execution adapters: DEX/router/wallet modules already scoped in `onchain/`.

## Target capabilities

1. Ingest account, holding, transaction, market, fundamental, macro, sentiment, and onchain data.
2. Evaluate every current and candidate position.
3. Produce buy/sell/hold recommendations with fair-value range and confidence.
4. Estimate expected holding period and invalidation conditions.
5. Attach an investment philosophy and strategy taxonomy to every recommendation.
6. Backtest each proposed strategy over relevant historical regimes.
7. Run walk-forward and out-of-sample validation.
8. Convert profitable, robust strategies into approval packets.
9. Require human approval before live/canary execution.
10. Audit every model input, decision, strategy version, approval, and execution.

## Phase A — Account and portfolio data foundation

### A.1 Plaid account linking and normalization

**Goal**: ingest external banking and brokerage account data into the portfolio graph/schema.

**Work**:
- Add Plaid Link token flow for dev/sandbox and later production.
- Store Plaid Items, accounts, institution metadata, consent state, and webhook status.
- Pull holdings, securities, balances, and investment transactions.
- Normalize external symbols/security IDs to internal instrument IDs.
- Reconcile institution value, cost basis, quantity, and transaction history.

**Acceptance criteria**:
- sandbox Plaid Item can be linked and refreshed;
- holdings and transactions map to internal `Portfolio`, `Position`, `Instrument`, and `Account` models;
- revoked consent disables refresh and downstream evaluation for that Item;
- no Plaid access tokens are logged or exposed in API responses.

### A.2 Multi-account portfolio ledger

**Goal**: create a canonical view across cash, brokerage, retirement, crypto, and onchain accounts.

**Work**:
- Add account ledger models: institution, account, balance snapshot, position snapshot, transaction, tax lot, corporate action, transfer.
- Track cash availability separately from margin/borrowing capacity.
- Add reconciliation jobs for stale holdings and missing transactions.

**Acceptance criteria**:
- one endpoint returns consolidated NAV, cash, exposures, unrealized P&L, realized P&L, and allocation drift;
- each external account can be traced back to source connector and refresh timestamp.

## Phase B — Market data and valuation foundation

### B.1 Instrument master

**Goal**: unify stocks, ETFs, options, crypto, stablecoins, LP positions, and onchain tokens.

**Work**:
- Add `Instrument` model with asset class, symbol, venue, CUSIP/ISIN where available, chain/address where applicable, currency, multiplier, and trading hours.
- Add symbol mapping between Plaid securities, broker symbols, Coinbase products, and onchain tokens.
- Add delisting/suspension/corporate-action awareness.

**Acceptance criteria**:
- every position resolves to a canonical instrument;
- unresolved instruments are quarantined from automated recommendations.

### B.2 Fair-market-price engine

**Goal**: estimate buy, sell, and hold ranges for each instrument/position.

**Valuation modules**:
- **Market microstructure**: bid/ask, spread, depth, volatility, slippage, order-book imbalance.
- **Technical/statistical**: trend, momentum, mean reversion, realized volatility, z-score, support/resistance.
- **Fundamental**: earnings, cash flow, balance sheet quality, revenue growth, valuation multiples, sector comps.
- **Macro/regime**: rates, inflation, liquidity, dollar strength, risk-on/risk-off regime.
- **Sentiment/news**: event risk, news classification, social/news momentum with source quality scoring.
- **Onchain**: liquidity, TVL, token flow, wallet concentration, contract risk, bridge/DEX risk.

**Outputs**:
- `fair_value_low`, `fair_value_mid`, `fair_value_high`;
- `buy_below`, `sell_above`, `hold_range`;
- confidence score and model agreement/disagreement;
- expected holding period and invalidation trigger;
- recommended max position size and stop/hedge policy.

**Acceptance criteria**:
- every valuation output includes source inputs, timestamp, model version, and rationale;
- recommendations are blocked if market data is stale or instrument mapping is unresolved.

## Phase C — Agentic research and position evaluation

### C.1 Evaluation agent roles

**Goal**: break evaluation into auditable specialist agents.

**Agents**:
- **Position Auditor**: reconciles actual holdings, cost basis, exposure, and concentration.
- **Market Analyst**: evaluates price action, liquidity, spread, depth, volatility, and regime.
- **Fundamental Analyst**: builds/updates equity valuation and quality scores.
- **Crypto/Onchain Analyst**: evaluates exchange, custody, liquidity, token, and contract risk.
- **Risk Analyst**: computes drawdown, VaR/CVaR, correlation, exposure, and stress-test impact.
- **Strategy Researcher**: proposes strategy hypotheses and parameter grids.
- **Backtest Critic**: checks overfitting, survivorship bias, look-ahead bias, fee/slippage realism, and regime fragility.
- **Approval Drafter**: converts validated opportunity into human-readable approval packets.

**Acceptance criteria**:
- each agent writes structured outputs to the database;
- agent disagreements are surfaced instead of hidden;
- final recommendations include rationale, evidence links, and dissenting signals.

### C.2 Position recommendation schema

Every evaluation should produce:

```json
{
  "instrument_id": "AAPL",
  "portfolio_id": "core",
  "current_position": 100,
  "recommendation": "BUY|SELL|HOLD|REDUCE|EXIT|WATCH",
  "fair_value_low": 0,
  "fair_value_mid": 0,
  "fair_value_high": 0,
  "buy_below": 0,
  "sell_above": 0,
  "hold_range": [0, 0],
  "expected_holding_period": "days|weeks|months|years",
  "investment_philosophy": "value|growth|momentum|quality|income|market_making|arbitrage|hedged|macro|onchain_yield",
  "confidence": 0.0,
  "risk_score": 0.0,
  "max_position_pct_nav": 0.0,
  "stop_or_invalidation": "text",
  "hedge_plan": "text",
  "evidence": [],
  "requires_human_approval": true
}
```

## Phase D — Strategy research, backtesting, and certification

### D.1 Strategy hypothesis registry

**Goal**: all strategies are versioned, parameterized, and reviewable before testing.

**Work**:
- Add `StrategyHypothesis` records with philosophy, target instruments, timeframe, holding period, signal rules, exit rules, risk constraints, and expected edge.
- Require deterministic config hashes for each parameter set.
- Link hypotheses to backtest runs and approval packets.

**Acceptance criteria**:
- no strategy can be backtested or approved without a registered hypothesis;
- every strategy version is immutable once evaluated.

### D.2 Backtest pipeline

**Goal**: reject weak strategies before paper/live routing.

**Required tests**:
- historical backtest across multiple market regimes;
- walk-forward validation;
- out-of-sample validation;
- transaction-cost and slippage model;
- liquidity capacity model;
- drawdown and tail-risk tests;
- sensitivity analysis over parameter ranges;
- benchmark comparison;
- stress replay scenarios.

**Minimum certification gates**:
- positive net return after fees/slippage;
- acceptable maximum drawdown for declared risk tier;
- Sharpe/Sortino/profit factor thresholds by strategy type;
- no evidence of severe overfitting;
- stable performance across at least two regimes or explicit regime detector;
- capacity estimate supports proposed allocation.

### D.3 Paper/shadow incubation

**Goal**: prove strategies in runtime without risking capital.

**Work**:
- Run certified strategies in paper mode first.
- Run shadow mode against live market data and broker/exchange payload generation.
- Compare expected fills to paper fills and live quotes.
- Track drift from backtest assumptions.

**Acceptance criteria**:
- strategy cannot request live approval until paper/shadow incubation completes;
- incubation report includes realized paper P&L, slippage estimate, missed fills, latency, and risk events.

## Phase E — Approval pipeline

### E.1 Strategy approval packet

A strategy approval packet must include:

- strategy hypothesis and config hash;
- investment philosophy;
- instruments/venues/accounts touched;
- fair-value logic and signal rules;
- backtest, walk-forward, and paper/shadow evidence;
- expected return/risk range;
- max allocation and capital at risk;
- holding period and exit criteria;
- stop-loss, hedge, and kill-switch policy;
- compliance/regulatory constraints;
- live/canary rollout plan;
- exact broker/exchange execution adapter to use;
- required human approver and expiry time.

**Acceptance criteria**:
- no strategy approval packet can execute directly;
- approval expiry requires re-evaluation;
- any code/config/data change invalidates prior approval unless explicitly grandfathered.

### E.2 Trade approval packet

Each proposed trade should include:

- account and venue;
- instrument and side;
- order type, limit/market bounds, and fair-value range;
- expected slippage, fees, spread, liquidity, and fill risk;
- position impact and portfolio-level exposure impact;
- holding period and exit plan;
- risk score and kill-switch triggers;
- source strategy approval reference.

**Acceptance criteria**:
- live execution checks both strategy approval and trade approval;
- rejected trades are logged with reasons;
- partial fills and cancellations update the audit trail.

## Phase F — Execution adapters

### F.1 Equities/brokerage execution layer

**Goal**: add stocks/ETFs/options execution through approved broker APIs, not Plaid.

**Work**:
- Define broker adapter interface: account sync, asset metadata, buying power, order preview, submit, cancel, positions, fills, activities, and reconciliation.
- Implement paper broker adapter first.
- Add one real broker adapter behind shadow/live gates.
- Require account suitability and asset-class permissions before enabling options/margin/shorting.

**Acceptance criteria**:
- stock/ETF trade execution is impossible through Plaid connector code;
- broker adapter must support preview and reconciliation before submit is enabled.

### F.2 Crypto execution layer

**Goal**: unify centralized exchange and onchain execution approvals.

**Work**:
- Extend Coinbase adapter to share the same approval and valuation contracts.
- Add exchange capability matrix for spot, advanced orders, fees, products, limits, custody constraints, and region restrictions.
- Keep onchain wallet signing behind route approval and key-management gates.

**Acceptance criteria**:
- crypto trades and onchain actions use the same strategy/trade approval pipeline;
- exchange trust degradation blocks new orders.

## Phase G — Governance, audit, and compliance

### G.1 Audit and explainability

**Goal**: make every recommendation and trade reconstructable.

**Work**:
- Store data snapshot IDs, model versions, prompt versions, agent outputs, and risk decisions.
- Add rationale summaries and machine-readable evidence references.
- Add reviewer/approver identity and approval expiry.

**Acceptance criteria**:
- any executed trade can be traced back to data, model, strategy config, approval, and execution adapter.

### G.2 Policy controls

**Work**:
- Global kill switch.
- Per-account and per-asset allocation limits.
- Regime-specific risk limits.
- Cooling-off period after drawdown or model drift.
- Human re-approval after strategy code/config changes.

**Acceptance criteria**:
- policy violations prevent approval packet generation or execution;
- all violations create audit events.

## Phase H — Operator UI and reporting

**Work**:
- Add dashboard for current holdings, fair-value bands, recommendations, pending approvals, active strategies, and live/paper performance.
- Add strategy research notebook/report export.
- Add approval review UI with evidence, dissenting agent outputs, and one-click approve/reject with comments.
- Add daily portfolio briefing generated from audited data.

**Acceptance criteria**:
- operator can see why each position is buy/sell/hold;
- operator can approve strategy and trade separately;
- reports distinguish backtested, paper, shadow, canary, and live performance.

## Build order

1. Plaid sandbox ingestion and canonical account/position ledger.
2. Instrument master and symbol/security mapping.
3. Fair-market-price model contract and persistence.
4. Agentic position-evaluation schema and specialist agents.
5. Strategy hypothesis registry and backtest certification gates.
6. Paper/shadow incubation reports.
7. Strategy approval packets.
8. Trade approval packets.
9. Broker adapter interface and paper broker adapter.
10. First real equities broker adapter in read-only/shadow mode.
11. Crypto adapter alignment with the same approval contracts.
12. Operator UI for recommendations and approvals.
13. Live/canary execution only after all gates pass.
