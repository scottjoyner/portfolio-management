# Daily Trading Operations Guide

This guide explains how to use the Portfolio OS console during normal paper/guarded operation. It is written for the operator who wants to understand what the deterministic bot, paid agent, and execution engine are doing without reading raw ledgers.

## Safety rules

1. **Unknown is not healthy.** Missing or stale evidence remains visibly unknown and should not be interpreted as zero risk.
2. **A decision is not an execution.** Opportunities and signals can exist without an order ever reaching the execution engine.
3. **A fill is not always settled.** Review the execution lifecycle through settlement.
4. **Competition results are conditional.** No winner is valid without a shared epoch, fresh books, valid accounting, and current marks.
5. **Live orders remain blocked.** The console is a paper/guarded operator surface and is labelled `Local operator execution (non-canonical)`.

## Recommended daily sequence

### 1. Open **Today**

Start with the plain-language brief and safety strip.

Confirm:

- Trading mode is the mode you expect.
- Execution evidence is `ok` or explicitly allowed for paper operation.
- Market feed freshness is acceptable.
- The competition is either valid or clearly blocked.
- The connection indicator is green and the last-refresh time is recent.

Do not proceed from a green-looking metric alone. Review any warning banner first.

### 2. Review **What needs attention**

The attention queue prioritizes:

- failed, rejected, or cancelled executions;
- blocked competition accounting;
- paid-agent inference costs that are not yet covered by gross P&L;
- research budget requests awaiting approval;
- stale, missing, or internally inconsistent system evidence.

Each item links to the page where the underlying evidence can be inspected.

### 3. Review the decision pipeline

The Today funnel shows the number of records at each stage:

```text
Signals → Approved → Executions → Filled → Settled
```

The counts are not expected to be equal. Differences help identify where work is stopping:

- Many signals but few approvals: strategy or risk filters are rejecting candidates.
- Many approvals but few executions: execution planning or operator approval may be incomplete.
- Many executions but few fills: submission, venue, price, or order-shape problems may exist.
- Many fills but few settlements: reconciliation or settlement handling needs attention.

### 4. Inspect open exposure

The Today position preview is intentionally compact. Open **Positions** for the full ledger.

### 5. Follow recent activity

The activity timeline combines recent audit entries, opportunities, and execution events. Use it to determine what changed since the last operator review, then open the relevant detailed page.

## Today page fields

### Safety strip

| Field | Meaning |
|---|---|
| Trading mode | Source-labelled runtime mode from System Truth. |
| Execution | Whether canonical execution evidence is available and permitted. |
| Market feed | Freshness of the observed feed heartbeat. |
| Competition | Current leader only when ranking is valid; otherwise `Ranking blocked`. |
| Ranking badge | Explicit valid/blocked state. |

### Daily KPIs

| KPI | Meaning |
|---|---|
| Open positions | Maximum observed exposure count across operator positions and competition books. |
| Executions in progress | Draft, pending, approved, submitted, open, partial-fill, or reconciliation records. |
| Agent cost this epoch | Paid model/API and compute cost attributable after the shared epoch baseline. |
| Race standing | Agent, bot, tie, or blocked. |

Counts can come from different source-labelled books. They are an operating summary, not a replacement for ledger reconciliation.

## Execution page

The Execution page is the authoritative operator view for order lifecycle understanding.

### Filters

- **Status:** all, in progress, filled, or failed/rejected.
- **Owner:** paid agent, deterministic bot, or other/unknown.
- **Search:** execution ID, symbol, strategy, source agent, side, or status.

### Execution header

Each expandable record shows:

- symbol and side;
- owner and strategy/source identifier;
- execution ID and relative update time;
- current status;
- notional and confidence when available.

### Lifecycle stages

```text
Decision → Approved → Submitted → Filled → Settled
```

A failed or rejected record does not imply that an order was submitted. Expand the record and inspect its last completed stage.

### Execution details

The expanded record includes:

- created and last-updated timestamps;
- order and fill counts;
- entry, target, and stop evidence;
- persisted error or rejection reason;
- execution-engine event history.

When an event history is empty, treat the execution store as incomplete rather than assuming nothing happened.

## Positions page

The Positions page reads `GET /api/positions` and enriches known symbols with `GET /api/market-data/live-quotes` when available.

Fields:

| Field | Meaning |
|---|---|
| Symbol | Product or asset identifier. |
| Source | Venue/provider/operator-state source. |
| Quantity | Reported open quantity. |
| Average | Cost basis or entry price when supplied. |
| Mark | Current mark or observed quote when supplied. |
| Market value | Quantity multiplied by mark when both are valid, or a persisted value. |
| Unrealized P&L | Persisted value, or price difference times quantity when valid inputs exist. |
| Status | Open/closed/source status. |

Coinbase balance sync can return quantity without cost basis. In that case Average and Unrealized P&L remain unavailable. Do not infer profitability from market value alone.

The competition books may report positions that are not represented in the operator position store. The page displays both counts separately in the summary chips.

## Decisions page

The Decisions page shows normalized opportunities before they become executions.

Review:

- Source: paid agent, deterministic bot, or another strategy/source.
- Recommendation and confidence.
- Persisted rationale or thesis.
- Gross expected value.
- Attributable research/model cost.
- Net expected value.
- Approval status.

The core economic relationship is:

```text
net_expected_value = gross_expected_value
                     - fees
                     - slippage
                     - gas
                     - agent_research_cost
                     - model_inference_cost
```

A positive expected value is a hypothesis, not realized profit. Use the Execution and Competition pages to evaluate actual outcomes.

## Competition page

The Competition page compares normalized performance from one shared epoch.

### Validity requirements

A winner is allowed only when:

- a competition epoch manifest exists;
- agent and bot records identify the same epoch;
- books and snapshot are fresh;
- the agent ledger uses accounting version 2 and is ranking eligible;
- both normalized starting values are valid;
- open positions have valid marks;
- agent operating cost is attributable after the epoch baseline;
- no integrity warning blocks the result.

### Score interpretation

```text
agent_net_equity = normalized_agent_gross_equity - post_epoch_operating_cost
bot_net_equity   = normalized_bot_gross_equity
```

The `Agent alpha after cost` number is the difference between normalized net return percentages, not a guarantee of future performance.

### Cost coverage

```text
agent_cost_coverage = agent_gross_pnl / attributable_agent_cost
```

- Less than `1.00×`: gross P&L has not covered paid inference/compute cost.
- Equal to `1.00×`: gross P&L is approximately at operating-cost break-even.
- Greater than `1.00×`: gross P&L exceeds attributable operating cost, but the agent must still beat the bot after cost and remain inside risk limits.

## Agent page

The Agent page separates economics from learning claims.

### Economics

Review:

- attributed epoch cost;
- remote model cost;
- local compute cost;
- cost per opportunity;
- daily budget;
- net P&L after cost;
- break-even gap.

### Budget approvals

Paid research should be tied to:

- an explicit opportunity;
- projected token and dollar cost;
- a market scope;
- an approval decision;
- an expected-value or evidence objective.

The UI does not expose an unrestricted spend button. Budget creation remains guarded through the API workflow.

### Learning and promotion

A challenger should not be promoted merely because a recent trade won. A promotion record needs:

- version and hypothesis;
- training and evaluation windows;
- minimum sample size and regime diversity;
- risk and drawdown comparison;
- out-of-sample result;
- canary deployment evidence;
- approval state;
- rollback pointer.

## Risk & System page

This page answers whether the displayed evidence can be trusted.

Review:

- trading mode;
- feed and service freshness;
- cache status;
- canonical paper-book cash and open-position count;
- execution decision;
- terminal link;
- agent and bot drawdown;
- known operator exposure;
- unresolved warnings.

System Truth does not fall back to operator-state guesses when the canonical health snapshot is stale or invalid.

## Refresh behavior

The console polls every five seconds while the browser tab is visible. It also refreshes when the tab becomes visible again.

The Refresh button prevents overlapping requests. A partial API failure leaves the last known successful state visible and adds a warning containing the failed endpoint.

## Starting a new competition epoch

Stop EventTrader and Hermes before running:

```bash
python scripts/start_competition_epoch.py --yes
python scripts/competition_scoreboard.py --print-json
```

The command refuses to proceed when:

- the bot writer lock is held;
- the bot book is missing, stale, or invalid;
- the bot has open positions;
- bot equity is unavailable;
- the agent-cost baseline cannot be read.

It archives the previous Hermes ledger and epoch, creates a clean accounting-v2 agent ledger, captures baselines, writes the epoch manifest, and removes the stale competition snapshot.

## Incident workflow

### Execution failure

1. Open **Execution** and filter `Failed / rejected`.
2. Expand the newest record.
3. Identify the last completed lifecycle stage.
4. Read the persisted error and event history.
5. Confirm no duplicate execution already filled before retrying.
6. Review System Truth and the kill-switch state.

### Stale or unknown system evidence

1. Open **Risk & System**.
2. Identify the exact source-labelled warning.
3. Check the system-health publisher and feed heartbeat.
4. Do not clear the warning by substituting operator-state values.
5. Restore the authoritative source and wait for a fresh snapshot.

### Competition ranking blocked

1. Open **Competition** and read the validity list.
2. Confirm a shared epoch exists.
3. Confirm both books are fresh and correctly marked.
4. Confirm agent accounting version 2 and ranking eligibility.
5. Republish the scoreboard snapshot.

### Paid-agent cost spike

1. Open **Agent**.
2. Compare remote/local cost, cost per opportunity, and daily budget.
3. Review pending budget approvals and recent opportunities.
4. Confirm each paid request has decision lineage.
5. Reduce or block new research before changing trading risk limits.
