# Alpha Validation Evidence Contract

## Purpose

`Portfolio OS` previously had a strong challenger gate but accepted caller-supplied metrics such as `walk_forward_passed`, `profit_factor`, and `out_of_sample_trades`. That made promotion structurally vulnerable to fabricated, stale, accidentally miscomputed, or repeatedly optimized evidence.

The canonical research boundary now has two stages:

1. replay-bound alpha validation over a committed search dataset;
2. a one-shot untouched terminal holdout for the deterministically selected candidate.

Metric booleans alone cannot authorize challenger promotion.

This work also repaired an earlier walk-forward correctness defect: the old framework could return `n_folds` copies of the same final holdout. Distinct chronological OOS intervals are now mandatory.

## Safety boundary

This research stack does **not** enable live trading or add broker authority.

The intended flow is:

```text
candidate strategy/config
  -> canonical feed-cache search snapshot
  -> compiled Rust historical replay
  -> leakage-safe walk-forward folds
  -> replay-bound AlphaValidationEvidence
  -> research tournament records every trial
  -> deterministic pre-terminal selection
  -> one-shot committed terminal holdout
  -> TerminalHoldoutEvidence
  -> ChallengerRegistry promotion gate
  -> supervised canary metadata only
```

Live trading remains blocked by the execution boundary.

## Walk-forward split contract

`scripts.alpha_validation.generate_walk_forward_splits()` emits half-open integer boundaries:

```text
[ train ][ purge ][ embargo ][ test ]
```

- `train`: data available to fit/tune the candidate;
- `purge`: observations removed because overlapping labels/holding horizons could leak future information into training;
- `embargo`: additional no-train/no-test spacing before OOS test;
- `test`: out-of-sample observations only.

`expanding=True` creates anchored expanding windows. `expanding=False` keeps a fixed rolling training window.

With 1,000 observations, four folds, and no gap, the repaired framework creates:

```text
train 0:200 -> test 200:400
train 0:400 -> test 400:600
train 0:600 -> test 600:800
train 0:800 -> test 800:1000
```

Earlier OOS intervals can become training history for a later fold because they are then historical. A fold may never train on its own test interval or future observations.

## Alpha evidence integrity — schema v2

Evidence is canonicalized with sorted compact JSON and SHA-256. NaN and Infinity are rejected.

Schema v2 embeds enough data to recompute statistical claims, including:

- candidate ID and source SHA;
- complete candidate config and config hash;
- dataset ID/hash;
- raw OOS return observations grouped by fold;
- annualization convention;
- bootstrap sample count, seed and ruin threshold;
- cost-stress scenarios;
- validation policy;
- regime labels;
- fold metrics and aggregate metrics;
- final evidence hash.

`verify_alpha_validation_evidence()` rebuilds candidate-config hash, fold metrics, aggregate metrics, cost stress, bootstrap results, pass/fail decision and failure reasons from embedded OOS observations. Editing a derived `profit_factor`, `max_drawdown_pct`, or `walk_forward_passed` and simply rehashing does not pass verification.

## Canonical replay provenance

`scripts/backtest_framework/canonical_replay.py` binds alpha evidence to the exact local data/replay path.

The canonical replay path:

1. loads OHLCV rows from `data.feed_cache`;
2. validates and normalizes timestamp/price/volume invariants;
3. hashes exact logical OHLCV rows;
4. derives dataset identity from those rows and replay metadata rather than trusting caller labels;
5. creates leakage-safe walk-forward boundaries;
6. regenerates OOS folds through compiled Rust strategy evaluation;
7. records test-row hashes, return hashes, replay controls and runner-source hash;
8. binds that attestation into alpha evidence;
9. reloads source and reruns replay during challenger evaluation and again at promotion.

Missing feed data, required native symbols, changed in-range candles, changed replay controls, changed return observations, or changed canonical replay source invalidate provenance.

The dedicated native CI lane builds the checked-out mixed Python/Rust package, proves native exports, and exercises canonical replay/fee parity against that compiled extension.

### Executable configuration identity

Configured `rsi_revert` is no longer merely metadata-bound. The canonical configured path binds a typed RSI parameter map to the compiled replay call and independently regenerates its parameter neighborhood.

Not every strategy family has an equally complete typed configuration contract yet. Expansion must be strategy-specific and typed rather than introducing an unvalidated generic parameter bag.

Canonical replay proves local data/execution consistency relative to checked-out source. It is not yet a remote hardware signature, compiled-binary signature, or externally immutable evidence-store signature.

## Backtest semantic parity

Historical Python/Rust semantics were hardened during replay certification:

- round-trip fees use basis points correctly (`10 bps` per side is `0.20` percentage points per completed trade);
- portfolio return compounds from the equity curve instead of summing percentage returns;
- zero-variance samples report zero Sharpe rather than using an artificial volatility floor;
- drawdown is chronological peak-to-trough drawdown;
- no-loss profit-factor handling is aligned;
- insufficient trade count blocks approval without erasing otherwise observable one-trade metrics;
- `min_trades` is propagated through Python and Rust backtest interfaces;
- Python historical loops clear indicator cache state between growing bar windows.

Native-only assertions are skipped in portable Python environments and exercised in CI after building the exact checked-out Rust package.

## Generated metrics and robustness

The alpha engine derives finite JSON-safe OOS metrics including:

- trade count;
- total and annualized return;
- mean return;
- Sharpe and Sortino;
- profit factor;
- maximum drawdown;
- Calmar ratio;
- worst trade;
- 5% expected shortfall;
- win rate;
- positive-fold fraction.

It also runs deterministic trade-sequence bootstrap resampling and reports profitable-resample fraction, estimated ruin probability, median terminal return and 5th-percentile terminal return.

## Cost stress

`cost_stress_results()` subtracts incremental round-trip cost from each observed OOS return. Default alpha policy requires profitability at the first configured scenario at or above 25 bps additional cost.

This is a conservative research stress, not a substitute for empirical shadow/live slippage distributions.

## Parameter stability

The configured `rsi_revert` path derives parameter stability from neighboring compiled replays rather than orchestration claims. Deterministic one-at-a-time neighbors vary `period ±2`, `oversold ±5`, and `overbought ±5` where valid, using the same OOS boundaries, fees, warmup and holding controls.

Each neighbor records exact typed config/hash, raw fold returns/hash, trade count, log growth, profitability and retained-growth ratio. The bounded score is the minimum of profitable-neighbor fraction and mean retained log-growth versus the candidate, preventing a narrow center-point spike from receiving an artificially high score.

The low-level alpha builder still accepts `parameter_stability_score` for synthetic/unit compatibility, but the canonical configured builder derives it and rejects a conflicting override.

## Untouched terminal holdout

`scripts/research_tournament.py` closes the repeated-final-test gap for the canonical tournament path.

Before search begins, trusted infrastructure commits one chronological partition:

```text
[ search ][ embargo ][ terminal holdout ]
```

The experiment plan exposes the terminal manifest/hash but not terminal OHLCV rows. Every submitted candidate trial—eligible or rejected—is recorded in append-only lineage. Candidate intake closes permanently when deterministic selection is sealed.

Only the selected candidate can open the terminal window. The terminal runner reloads the exact committed feed-cache range and uses the exact selected strategy/config, warmup, fees and holding controls from alpha replay evidence. It records raw terminal returns, recomputed metrics, terminal policy, exact dataset manifest/hash, selection/experiment bindings and lineage event hashes.

A terminal pass is required for promotion. A terminal failure is final for that experiment; choosing another candidate or changing terminal thresholds requires a new experiment and newly committed holdout.

See `docs/RESEARCH_TOURNAMENT.md` for the complete state-machine and verification contract.

## Promotion contract

`ChallengerRegistry.evaluate()` first preserves the historical alpha/replay diagnostics. Missing/invalid alpha evidence, candidate mismatch, executable-config mismatch or replay-provenance failure are rejected before terminal evidence is considered.

If alpha/replay would otherwise approve, terminal evidence becomes mandatory.

Before a challenger can enter approved state:

1. alpha schema/hash and recomputed metrics must verify;
2. candidate ID/config must match the proposal;
3. canonical replay must revalidate against source and compiled replay;
4. terminal evidence must verify against exact source and append-only lineage;
5. terminal evidence must have passed its policy;
6. terminal candidate ID/config/source SHA must match alpha/challenger identity;
7. terminal evidence must bind the exact alpha-evidence hash;
8. both complete artifacts and hashes are persisted.

Missing terminal evidence fails closed with `terminal_holdout_evidence_required`.

Before canary promotion:

1. challenger and recorded evaluation must still be approved;
2. complete persisted alpha evidence is reverified;
3. canonical alpha replay is rerun from source;
4. complete persisted terminal evidence is reverified;
5. terminal source replay and lineage chain are independently revalidated;
6. candidate/config/source/alpha-hash bindings must still match;
7. persisted alpha and terminal hashes must match the evaluation;
8. evaluation must record successful terminal verification;
9. only then is canary configuration written.

Canary config and promotion lineage carry both `alpha_validation_evidence_hash` and `terminal_holdout_evidence_hash`.

Legacy `challenger_metrics` remain accepted only for compatibility/audit visibility and cannot authorize promotion.

## Default alpha evidence policy

Current `ValidationPolicy` defaults:

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

`challenger_manager.DEFAULT_THRESHOLDS` remains a second independent relative gate and is stricter in some dimensions, including 30 total trades, regime diversity, after-cost P&L improvement, cost coverage and allowed drawdown regression versus incumbent.

Passing alpha evidence therefore does not imply passing terminal holdout or beating the incumbent.

## Default terminal policy

Current `TerminalHoldoutPolicy` defaults:

```text
minimum completed trades   5
positive total return      required
minimum profit factor      1.00
maximum drawdown           25%
```

These are final-test admission floors, not optimization targets.

## Deliberate remaining limitations

Still required before this is a complete scientific-validation system:

1. Add formal multiple-testing / selection-bias correction across the now-recorded candidate search history.
2. Add block/bootstrap methods appropriate for serially correlated returns.
3. Add explicit asset/session/regime concentration metrics and regime-labelled fold construction where appropriate.
4. Expand typed executable configuration/replay and replay-derived stability beyond currently configured strategy families.
5. Bind candidate source commit/tree identity more strongly to the executable implementation used by replay.
6. Add trusted runner/build/binary attestation and externally immutable evidence storage.
7. Add access-control/trusted-runner separation so autonomous research agents cannot inspect committed terminal rows out-of-band.
8. Add empirical slippage/latency distributions from shadow/live executions.
9. Quarantine or harden legacy research config-generation helpers before treating any of them as runtime promotion paths.

Until those are implemented, this stack should be treated as a major improvement in leakage discipline, replay provenance, experiment lineage and final-test integrity—not a claim that any strategy is certified for meaningful real capital.