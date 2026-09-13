# Certified Spot Execution Lifecycle

Status: paper/demo execution contract for future freshly-certified spot research. This document does **not** authorize live trading and does not retroactively upgrade existing research evidence.

## Why this boundary exists

The historical canonical research replay is intentionally preserved. While flat, that replay treats either `BUY` or `SELL` as a position-opening direction, so `SELL` can open a synthetic short. The Coinbase cash/spot runtime has a different economic constraint: uncovered shorts are not permitted and the canonical capital-risk snapshot already rejects sells larger than owned inventory.

Calling those two lifecycles equivalent would make certification meaningless. The system therefore keeps historical research evidence intact and introduces a separate, versioned spot execution lifecycle.

## Canonical spot-long-only semantics

`spot_long_only_opposite_signal_or_max_hold_v1` is defined as:

```text
state: FLAT
  BUY  -> OPEN_LONG
  SELL -> NOOP
  HOLD -> NOOP

state: LONG
  BUY  -> NOOP
  SELL -> CLOSE_LONG
  HOLD -> remain LONG unless max hold expires

max-hold expiry -> CLOSE_LONG on the actual expiry bar
forced close    -> no re-entry on that same bar
historical window end -> close only for evidence accounting
```

There is no pyramiding, no uncovered short, and no same-bar reversal.

## Separate replay runner

`scripts/backtest_framework/spot_execution_replay.py` is separate from `canonical_replay.py` on purpose. Existing alpha/terminal evidence binds the historical replay source hash. Editing that runner merely to gain new execution semantics would invalidate or reinterpret old evidence. The spot runner instead has its own source hash, lifecycle hash, event trace, fold-return hash and attestation hash.

The spot runner still obtains every strategy signal from the compiled Rust evaluator. It changes position lifecycle semantics, not the strategy's signal calculation.

## Correct max-hold semantics

The historical replay only evaluates `max_hold_bars` on a bar that emitted a non-HOLD signal. That means a nominal maximum holding period can be exceeded when the strategy remains quiet.

The executable spot lifecycle closes at the actual expiry bar even when the evaluator emits HOLD/no signal. Max-hold evaluation occurs before the current signal and wins ties, so the forced-exit bar cannot also become a new entry.

## Certified scanner behavior

A verified canary is now scoped by both exact symbol and exact bar granularity. Once selected into the canary:

- only the exact promoted configured Rust evaluator emits the strategy signal;
- the legacy same-window default-parameter backtest is not run;
- impossible legacy `min_win_rate` / confidence screening thresholds cannot suppress the promoted signal;
- same-window backtest metrics are not manufactured as forward probabilities;
- heuristic TP/SL brackets are not given execution authority;
- BUY is represented as a candidate long entry and SELL as a candidate long close;
- runtime failure remains fail-closed rather than falling back to defaults.

Uncertified markets continue through the ordinary discovery screen and remain review-only.

## Execution authorization

`packages/execution/src/certifiedSpotLifecycle.mjs` derives a hash-bound lifecycle authorization from:

- exact runtime identity;
- fresh spot execution attestation hash;
- exact spot lifecycle hash;
- symbol;
- current certified signal;
- authoritative open quantity;
- position-open time and observation time.

The execution overseer now requires an `OPEN_LONG` lifecycle authorization before a no-human-approval strategy entry can pass. Merely setting `execution_lifecycle_certified=true` is insufficient. A forged or mutated authorization fails its hash and identity/lifecycle bindings.

Existing capital-risk, portfolio-allocation, positive executable-edge and shadow-calibration gates remain independent requirements.

## Graduation rule

Existing promoted candidates remain **shadow-only**. Their alpha and terminal evidence were produced under the historical long/short replay and cannot be relabeled as spot-executable after the fact.

A future candidate may receive spot execution authority only after the research process precommits this spot lifecycle before the terminal window is opened, produces source-reverifiable spot-long-only search and terminal evidence, and promotion/runtime certification carries the resulting attestation and lifecycle hashes. Until that artifact exists, the scanner explicitly publishes:

```text
execution_lifecycle_certified = false
execution_lifecycle_blocker = fresh_spot_long_only_attestation_required
```

## What remains before automated paper entry

The next research-control slice must wire the spot lifecycle commitment into new tournament experiments and promotion artifacts. It must not backfill the current candidate. Once a new candidate survives that path, the authoritative position state must be used to derive the lifecycle authorization before the existing allocator/risk/overseer pipeline can admit an automated paper entry.

Live mode remains independently blocked even after paper lifecycle certification.
