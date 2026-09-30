# Certified Shadow Attribution

Certified shadow attribution is the forward-measurement layer between research certification and any future automated execution authority.

It is intentionally non-live. A certified strategy signal can be measured in shadow mode while `execution_lifecycle_certified` remains false and while the overseer continues to reject automated strategy entry.

## Authority boundary

The runtime certification identity binds the selected candidate, source SHA, strategy config hash, alpha-validation evidence, terminal-holdout evidence, replay attestation, compiled evaluator binary, and canonical replay lifecycle.

The canonical lifecycle currently binds:

- exact replay dataset kind and symbol;
- bar granularity;
- warmup bars;
- fee basis points;
- maximum hold bars;
- exit on opposite certified signal;
- close-open-trade-at-observation-end semantics.

A certification produced for one replay symbol does not certify another market.

## Shadow position state machine

Shadow attribution follows canonical replay position semantics rather than the scanner's heuristic TP/SL trade plan.

For each `runtimeIdentityHash + symbol` pair:

1. While flat, a certified `BUY` opens one long shadow trial and a certified `SELL` opens one short shadow trial.
2. While a shadow position is open, repeated same-side decisions do not pyramid or open additional trials.
3. An opposite certified signal closes the current shadow position and does not reverse into a new position on the same signal bar.
4. If no opposite signal arrives first, `max_hold_bars` closes the trial on the first market snapshot at or after the canonical hold horizon.
5. The shadow ledger records predicted edge, simulated implementation shortfall, canonical replay fees, predicted-vs-shadow edge error, cost forecast error, latency error, sign accuracy, and edge capture.

These rules are regression-tested so forward shadow evidence remains comparable to the certified replay semantics.

## Economic measurement

Economic decisions are measured whether they were accepted or rejected. This avoids evaluating only selected positive-edge decisions and allows later analysis of false positives, false negatives, calibration, and selection quality.

A shadow result is not a live fill. Terms such as `canonicalShadowNetPnlUsd` and `simulatedExecutionCostsUsd` represent simulated forward attribution using observed market evidence and the certified replay assumptions.

## Graduation rule

This layer does **not** set `execution_lifecycle_certified=true` and does not grant capital authority. Any future graduation to automated execution must be based on sufficient forward evidence and a separately reviewed gate. Engineering CI establishes deterministic implementation consistency; it does not establish profitable alpha.
