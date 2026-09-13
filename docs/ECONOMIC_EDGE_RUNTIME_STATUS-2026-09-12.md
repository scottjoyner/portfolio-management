# Economic Edge / Runtime Boundary — 2026-09-12

Status: active paper/shadow architecture hardening. This document does not authorize live trading.

## Why this slice exists

The highest-value defects found in the current trading stack were not missing statistical sophistication. They were mismatches between what the research system proves and what the runtime actually uses, plus economic values with incompatible units.

The near-term goal is therefore to make the decision chain economically and behaviorally coherent:

```text
research screen
  -> certified candidate identity
  -> payoff-based expected P&L in USD
  -> execution/research costs in USD
  -> positive net executable edge for automated ENTRY admission
  -> allocator / hard risk
  -> overseer
  -> paper/shadow execution
  -> realized attribution
```

Screening, certification, sizing, and execution admission are separate authorities.

## Implemented on `agent/economic-edge-coherence`

### 1. Strategy expected value uses USD payoff economics

The old strategy opportunity path treated a score like:

```text
weighted_confidence * win_rate * 100
```

as `expectedValue/grossExpectedValue`, then subtracted dollar-denominated fees and slippage from it.

Strategy ENTRY opportunities now compute:

```text
quantity = requested_notional_usd / entry_price
upside_usd = quantity * abs(take_profit_price - entry_price)
downside_usd = quantity * abs(entry_price - stop_loss_price)
gross_expected_pnl_usd = p_win * upside_usd - p_loss * downside_usd
net_expected_value_usd = gross_expected_pnl_usd - fees - slippage - gas - research/model costs
```

Risk-reduction EXIT intents receive no fabricated entry edge. Their method is explicitly recorded as `risk_reduction_not_edge_scored_v1`.

### 2. Same-window strategy scans are labeled as screens, not validation

Strategy opportunities created from the current 30-day scanner now carry:

```text
backtestStatus = same_window_30d_screen_uncertified
validationScope = same_window_in_sample_screen
tournamentCertified = false
```

Their expected-value evidence also records the unit (`USD`), method, upside USD, max-loss USD, and gross expected P&L USD.

### 3. Opportunity creation no longer acts as the portfolio allocator

The previous path applied a context-free Kelly fraction and could inflate requested notional by up to 25% even though opportunity construction does not know total portfolio equity, cross-position correlation, allocator budget, or competing opportunities.

Now the opportunity layer preserves requested notional subject only to the configured hard max-position cap. Kelly remains diagnostic. `capitalAtRisk` is stop-defined max loss rather than gross notional.

### 4. Execution sizing fails closed

Approval no longer fabricates a `$1000` fallback execution size when recommended/requested size is missing. A finite positive execution size is required before an execution draft can be created.

An explicit `maxPositionSizeUsd = 0` remains zero; it is not replaced by the default cap.

### 5. Screening is now separated from automated execution admission

The strategy scanner previously created an opportunity labeled uncertified and then immediately called `decideOpportunity(... reviewer='system:auto-draft')`, producing a paper execution draft from the same uncertified screen.

That contradiction is removed.

Same-window strategy scans now produce reviewable opportunities only. They carry an explicit admission artifact:

```text
policy = same_window_screen_requires_certification
status = screen_only
autoDraftEligible = false
researchCertification = uncertified
reason = same_window_30d_screen_uncertified
```

The scanner path does not auto-approve those opportunities and does not create execution drafts.

### 6. Automated approval now has a fail-closed admission gate

`decideOpportunity()` persists the execution-admission artifact and treats reviewers whose identity starts with `system:auto` as automated approval.

For automated approval:

- `executionAdmission.autoDraftEligible` must be exactly `true`;
- ENTRY/open-position intents must have finite positive `netExpectedValue` in USD;
- finite positive execution size is still required.

An uncertified screen therefore cannot become an automated execution draft by merely raising confidence or win rate.

Human/operator review remains a distinct path. This gives the future tournament-certified runtime integration an explicit contract instead of another implicit bypass.

## Current research certification status

The preceding dependence-aware research-selection slice is certified at exact head `59090c39b1559a2be94665ffe540347e65a19c13`:

- Trading Strategies CI #691: passed;
- Full Python Test Inventory #181: passed;
- native compiled Rust canonical replay certification: passed.

The dependence-aware gate remains intentionally small: fold-local non-overlapping block direction votes supplement, rather than replace, the marginal exact sign test.

## Next highest-value work

The next implementation should **not** add another statistical test first. It should connect the certified research identity to the actual runtime admission artifact.

A certified strategy ENTRY should eventually carry one immutable identity bundle containing at least:

- candidate ID;
- strategy name;
- exact strategy config + config hash;
- candidate source SHA;
- alpha-validation evidence hash;
- tournament experiment ID/hash;
- selected multiple-testing/dependence assessment;
- terminal-holdout evidence hash/status when required;
- canonical dataset/replay identity;
- admission policy version.

The runtime must verify that bundle rather than trusting a boolean such as `tournamentCertified=true` supplied by an upstream caller.

After that binding exists, the next economic gate is `netExecutableEdgeUsd`: expected P&L must be reduced by measured execution costs, model/research cost, uncertainty reserve, and latency decay before automated ENTRY admission. EXIT/risk-reduction semantics must remain able to reduce exposure even when entry-style expected edge is negative.

## Important remaining gaps

1. `ChallengerRegistry.promote()` writes `data/agent_runtime_config.json`, but repository search has not yet shown a production signal path that consumes and verifies the exact active challenger identity/config.
2. The current scanner still performs same-window strategy/product search. It is now demoted to screening, but the runtime does not yet source entries from certified tournament candidates.
3. The execution overseer carries `netExecutableEdgeUsd` but does not yet independently verify research-certification identity or require positive executable edge for strategy-driven ENTRY intents.
4. Backtest win rate is still not a calibrated forward probability. Shadow outcomes need to drive probability calibration before sizing relies on it strongly.
5. Shadow execution needs closed-loop attribution: predicted edge vs realized P&L, slippage, latency, turnover, drawdown, calibration, and opportunity cost.

## Decision rule for further work

A proposed feature should move up the roadmap only if it improves at least one of these:

- correctness of expected P&L or costs;
- probability calibration;
- certified strategy identity actually used by runtime;
- capital allocation under hard risk limits;
- execution realism;
- realized attribution / learning;
- prevention of a concrete research or runtime bypass.

Features that merely make the research report look more sophisticated should remain behind those items.

## Safety invariant

No change in this slice enables live trading, changes broker credentials, enables withdrawals, or increases leverage/risk limits. Automated admission discussed here is limited to the existing paper/shadow execution architecture. Real-capital entry remains separately blocked and requires explicit operational and human authorization.
