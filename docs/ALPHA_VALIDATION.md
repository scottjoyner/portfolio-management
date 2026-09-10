# Alpha Validation Evidence Contract

## Purpose

`Portfolio OS` previously had a strong challenger gate but accepted caller-supplied metrics such as `walk_forward_passed`, `profit_factor`, and `out_of_sample_trades`. That made the promotion gate structurally vulnerable to fabricated, stale, or accidentally miscomputed evidence.

This slice introduces a canonical evidence boundary. A challenger can no longer become promotion-eligible through metric booleans alone.

It also repairs an existing walk-forward correctness defect: `scripts/backtest_framework/walk_forward.py` previously returned `n_folds` copies of the same final holdout. That meant a report such as `oos_passed=4` could represent the same OOS interval evaluated four times. The framework now uses distinct chronological expanding OOS intervals.

## Safety boundary

This work does **not** enable live trading or add broker authority.

It only changes research/challenger certification. The intended flow is:

```text
candidate strategy/config
  -> canonical feed-cache snapshot
  -> compiled Rust historical replay
  -> leakage-safe walk-forward folds
  -> realized OOS return observations
  -> alpha_validation.py
  -> replay-bound AlphaValidationEvidence
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

The existing backtest framework now delegates its fold boundaries to this canonical splitter. With 1,000 observations, four folds, and no gap, the framework creates:

```text
train 0:200 -> test 200:400
train 0:400 -> test 400:600
train 0:600 -> test 600:800
train 0:800 -> test 800:1000
```

Earlier OOS intervals can legitimately become training history for a later fold because they are in the past at that later evaluation time. A fold may never train on its own test interval or future observations.

## Evidence integrity — schema v2

Evidence is canonicalized with sorted, compact JSON and SHA-256. NaN and Infinity are rejected.

Schema v2 embeds the data required to recompute the statistical claims, including:

- candidate ID and source SHA;
- complete candidate config plus its hash;
- dataset ID and dataset hash;
- raw OOS return observations grouped by fold;
- annualization convention;
- bootstrap sample count, seed, and ruin threshold;
- cost-stress scenarios;
- validation policy;
- regime labels;
- fold metrics and aggregate metrics;
- final evidence hash.

`verify_alpha_validation_evidence()` does more than recompute the artifact hash. It rebuilds the candidate-config hash, fold metrics, aggregate metrics, cost-stress results, bootstrap results, pass/fail decision, and failure reasons from the embedded OOS observations. Therefore, changing a reported `profit_factor`, `max_drawdown_pct`, or `walk_forward_passed` value and simply re-hashing the JSON still fails verification.

For an approved challenger, the registry persists both the complete validation evidence artifact and its hash. Promotion re-verifies the complete artifact, candidate binding, evidence hash, and evaluation-to-evidence hash binding before writing canary configuration. A registry edit that alters previously approved evidence therefore fails closed at promotion time.

## Canonical replay provenance

`scripts/backtest_framework/canonical_replay.py` closes the local data-origin boundary that existed in the first version of this work.

The canonical replay path now:

1. loads OHLCV rows from `data.feed_cache`;
2. normalizes and validates timestamp, price, and volume invariants;
3. hashes the exact logical OHLCV rows that will be replayed;
4. derives `dataset_id` and `dataset_hash` from those rows and replay metadata rather than accepting caller labels;
5. creates leakage-safe walk-forward boundaries;
6. regenerates each OOS fold through the compiled Rust strategy evaluator;
7. records test-row hashes, realized-return hashes, replay controls, and the canonical replay runner source hash;
8. binds that attestation into the alpha-evidence hash;
9. on challenger evaluation and again on promotion, reloads the same feed-cache time range and reruns the Rust replay fail-closed.

Missing feed data, missing required compiled Rust symbols, changed in-range candles, changed replay controls, changed return observations, or changed canonical replay source invalidate provenance.

The dedicated `canonical-replay-rust` CI job builds the checked-out mixed Python/Rust package, proves the required native exports exist, and runs replay/fee parity tests against that compiled extension. Portable Python tests do not pretend that the source compatibility wrapper is a native backend.

### Remaining provenance boundary

Canonical replay now proves local dataset/replay consistency relative to the checked-out replay implementation. It is **not** yet a remote hardware signature, compiled-binary signature, or append-only evidence-store signature.

More importantly, `candidate_config` is currently integrity-bound metadata but is not a universally executable strategy parameter contract. Many Rust strategies still contain hard-coded lookbacks and thresholds, and the canonical Rust evaluator currently receives a strategy name plus market arrays rather than a typed parameter map. Therefore evidence can prove that `rsi_revert` ran, for example, but it cannot yet prove that arbitrary claimed challenger parameters were the parameters the Rust implementation executed.

The next implementation slice must close that execution-identity boundary before parameter-neighborhood optimization is treated as scientific evidence. A strategy's typed executable configuration, implementation identity, and replay attestation must all describe the same thing.

## Backtest semantic parity

This slice also repairs Python/Rust metric drift uncovered by native replay certification.

The historical engines now share these semantics:

- round-trip fees use basis points correctly (`10 bps` per side is `0.20` percentage points per completed trade);
- portfolio return is compounded from the equity curve rather than summing percentage returns;
- zero-variance samples report zero Sharpe instead of using an artificial volatility floor;
- drawdown is chronological peak-to-trough drawdown;
- no-loss profit-factor handling is aligned;
- insufficient trade count blocks approval without erasing otherwise observable one-trade metrics;
- `min_trades` is propagated through both Python and Rust backtest interfaces;
- the Python historical loop clears indicator cache state between growing bar windows so temporary list identity cannot leak stale cached indicator values.

The native parity lane is deliberately separate from the portable Python lane: native-only assertions are skipped when a compiled extension is absent and are exercised after CI builds the exact checked-out Rust package.

## Initial generated metrics

The engine derives finite JSON-safe metrics from OOS return observations:

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

This slice records a bounded `parameter_stability_score` and enforces a minimum threshold. The score remains an input hook for now.

It must **not** be treated as strong validation until strategies expose typed executable tunables to canonical replay. The next strategy-capability slice should first parameterize a small pilot strategy (for example `rsi_revert` with period/oversold/overbought), attest the exact executed parameter map, and only then derive stability from explicit neighboring-parameter replays.

## Promotion contract

`scripts.challenger_manager.ChallengerRegistry.evaluate()` fails closed when `validation_evidence` is missing or is not bound to canonical replay provenance.

Legacy `challenger_metrics` remain accepted as an argument for compatibility/audit visibility, but they cannot authorize promotion.

Before metric evaluation:

1. evidence schema and canonical hash are checked;
2. candidate config hash is recomputed;
3. raw OOS returns are re-evaluated into fold/aggregate/bootstrap/stress metrics;
4. reported derived metrics and pass/fail state must match recomputation;
5. evidence `candidate_id` must match the proposed challenger ID;
6. evidence `candidate_config` must match the challenger's proposed parameters;
7. replay provenance must revalidate against the canonical feed cache and compiled Rust replay;
8. approved evidence and its hash are persisted with the challenger.

Before canary promotion:

1. challenger and recorded evaluation must still be approved;
2. persisted evidence is re-verified from its full contents;
3. candidate ID and candidate config must still match the challenger;
4. persisted evidence hash must equal the artifact hash;
5. recorded evaluation hash must equal the same evidence hash;
6. canonical replay provenance is independently rerun and revalidated;
7. only then is canary configuration written.

The canary configuration and promotion lineage carry the verified evidence hash.

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

## Deliberate remaining limitations

Still required before this becomes a complete scientific-validation system:

1. Add typed executable strategy configuration and bind the exact executed parameter map plus implementation identity to replay evidence.
2. Generate parameter stability from actual neighboring-parameter replay sweeps rather than accepting a score from orchestration.
3. Add trusted runner/build/binary attestation and move complete evidence artifacts to an append-only or externally immutable evidence store.
4. Add multiple-testing / selection-bias correction across candidate searches.
5. Add block/bootstrap methods for serially correlated returns.
6. Add explicit asset/session/regime concentration metrics and regime-labelled fold construction where appropriate.
7. Add empirical slippage/latency distributions from shadow/live executions.
8. Add an untouched terminal holdout policy so agents cannot repeatedly optimize against the final test set.
9. Bind candidate source commit/tree identity to the executable implementation used by replay, not only to evidence metadata.
10. Expand configured replay beyond the pilot strategy without creating a generic untyped parameter bag.

Until those are implemented, this engine should be treated as a major improvement in evidence integrity, replay provenance, and leakage discipline—not a claim that any strategy is certified for meaningful real capital.
