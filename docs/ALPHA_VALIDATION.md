# Alpha Validation Evidence Contract

## Purpose

`Portfolio OS` previously had a strong challenger gate but accepted caller-supplied metrics such as `walk_forward_passed`, `profit_factor`, and `out_of_sample_trades`. That made the promotion gate structurally vulnerable to fabricated, stale, or accidentally miscomputed evidence.

This slice introduces a canonical evidence boundary. A challenger can no longer become promotion-eligible through metric booleans alone.

## Safety boundary

This work does **not** enable live trading or add broker authority.

It only changes research/challenger certification. The intended flow is:

```text
candidate strategy/config
  -> historical replay/backtest runner
  -> leakage-safe walk-forward folds
  -> realized OOS return observations
  -> alpha_validation.py
  -> immutable AlphaValidationEvidence
  -> challenger_manager.py
  -> approved/rejected challenger
  -> canary metadata only
```

## Walk-forward split contract

`scripts.alpha_validation.generate_walk_forward_splits()` emits half-open integer boundaries:

```text
[ train ][ purge ][ embargo ][ test ]
```

- `train`: data available to fit/tune the candidate.
- `purge`: observations removed because overlapping labels/holding horizons could leak future information back into training.
- `embargo`: additional no-train/no-test spacing before the OOS test period.
- `test`: out-of-sample observations only.

`expanding=True` creates anchored expanding windows. `expanding=False` keeps a fixed rolling training window.

The split generator does not itself run a strategy. Upstream replay/backtest code must obey these boundaries exactly and pass only realized OOS trade returns into evidence generation.

## Evidence integrity

Evidence is canonicalized with sorted, compact JSON and SHA-256. NaN and Infinity are rejected.

The artifact contains provenance for:

- candidate ID;
- candidate source SHA;
- candidate config hash;
- dataset ID;
- dataset hash;
- creation timestamp;
- validation method;
- fold metrics;
- policy used for pass/fail;
- bootstrap parameters/results;
- transaction-cost stress results;
- final evidence hash.

Changing any hashed field without rebuilding the artifact causes `verify_alpha_validation_evidence()` to fail.

## Initial generated metrics

The first slice derives finite JSON-safe metrics from OOS return observations:

- trade count;
- total and annualized return;
- mean return;
- Sharpe and Sortino ratios;
- profit factor;
- maximum drawdown;
- Calmar ratio;
- worst trade;
- 5% expected shortfall;
- win rate;
- positive walk-forward fold fraction.

It also runs deterministic trade-sequence bootstrap resampling and reports:

- fraction of resamples ending profitable;
- probability of crossing the configured ruin threshold;
- median terminal return;
- 5th-percentile terminal return.

## Cost stress

`cost_stress_results()` subtracts incremental round-trip cost from every observed OOS return. The default evidence policy requires the candidate to remain profitable at the first configured scenario at or above 25 bps additional cost.

The purpose is not to claim that 25 bps is the final correct execution model. It is a conservative first gate until shadow/live execution calibration provides venue- and strategy-specific empirical slippage distributions.

## Parameter stability

This slice records a bounded `parameter_stability_score` and enforces a minimum threshold. The score is intentionally an input hook for now; the next validation slice should generate it from explicit parameter-neighborhood experiments rather than accepting it from orchestration code.

## Promotion contract

`scripts.challenger_manager.ChallengerRegistry.evaluate()` now fails closed when `validation_evidence` is missing.

Legacy `challenger_metrics` remain accepted as an argument for compatibility/audit visibility, but they cannot authorize promotion.

Before metric evaluation:

1. evidence schema is checked;
2. evidence hash is recomputed;
3. the evidence `candidate_id` must match the proposed challenger ID;
4. canonical evidence is mapped to the existing metric-gate contract;
5. detailed validation failure reasons are preserved;
6. the evidence hash is persisted into challenger evaluation state.

`promote()` additionally requires the challenger to carry a verified alpha-validation evidence hash and copies that hash into canary configuration and promotion lineage.

## Default evidence policy

Current defaults in `ValidationPolicy`:

```text
minimum folds                     3
minimum OOS trades               10
minimum profit factor            1.10
maximum drawdown                 20%
minimum profitable-fold fraction 50%
minimum profitable bootstrap     55%
maximum estimated ruin prob       5%
required extra-cost stress       25 bps
minimum parameter stability      0.60
```

The existing `challenger_manager.DEFAULT_THRESHOLDS` remains a second independent gate and is stricter in some dimensions, including 30 total trades, minimum regime diversity, after-cost P&L improvement, cost coverage, and allowed drawdown regression versus the incumbent.

Passing alpha evidence therefore does not automatically mean a challenger beats the incumbent.

## Deliberate limitations of this first slice

Still required before this becomes a complete scientific-validation system:

1. Wire the split contract into a canonical historical replay runner.
2. Generate parameter stability from actual neighborhood sweeps.
3. Add multiple-testing / selection-bias correction across candidate searches.
4. Add block/bootstrap methods for serially correlated returns.
5. Add explicit asset/session/regime concentration metrics.
6. Add empirical slippage/latency distributions from shadow/live executions.
7. Add immutable storage/indexing for evidence artifacts, rather than only carrying the evidence hash in challenger state.
8. Add source/dataset snapshot tooling that produces the supplied provenance hashes.
9. Add regime-labelled fold construction where appropriate.
10. Add explicit final holdout rules so agents cannot repeatedly optimize against the terminal test set.

Until those are implemented, this engine should be treated as a major improvement in evidence integrity and leakage discipline—not a claim that any strategy is certified for meaningful real capital.
