# Strategy Concentration Policy

## Scope

This policy applies to the supervised production-paper competition runtime. It does not authorize live order submission or define a certified live-capital limit.

## Default

`max_strategy_pnl_share` defaults to `0.60` for the paper competition.

The guard blocks new entries for a strategy when that strategy's tracked live paper PnL exceeds 60% of current paper equity. Existing positions remain eligible for normal risk-reducing exits.

## Rationale

The competition is intended to discover whether a strategy can repeatedly outperform after fees and model costs. A lower default such as 30% can prematurely suppress a genuinely dominant strategy before enough live-paper observations accumulate. A 60% cap allows continued evidence collection while still preventing one strategy from approaching total-book dominance.

This is a PnL-concentration guard, not a position-size or gross-exposure limit. Other controls continue to constrain position size, portfolio drawdown, order notional, leverage, open-position count, cash reserves, and kill-switch state.

## Operator controls

The value remains runtime-tunable within the declared range. Operators may lower it for conservative rehearsals, but every override should record:

- the old and new value;
- the operator and timestamp;
- the reason for the change;
- the affected competition epoch;
- the planned rollback condition.

A value of zero disables this specific guard and must not be used for a release-certified rehearsal.

## Promotion requirements

Before any future live-trading certification, the concentration limit must be re-derived from replay and paper evidence and approved alongside:

- per-strategy capital exposure limits;
- correlated-strategy and asset-cluster limits;
- drawdown and loss-streak breakers;
- minimum sample depth and confidence intervals;
- rollback and kill-switch procedures.

The paper default must not be copied into live configuration without that review.
