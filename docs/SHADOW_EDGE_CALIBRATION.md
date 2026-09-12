# Certified Shadow Edge Calibration

Status: forward-evidence admission guard for certified paper/shadow strategy decisions. This document does not authorize live trading.

## Why this exists

Research certification answers a provenance question: **is the runtime executing the exact strategy/configuration that survived the canonical research process?**

Shadow attribution answers a measurement question: **what happened when that certified signal was followed forward under the canonical lifecycle?**

Neither question, by itself, proves that the economic model's current `netExecutableEdgeUsd` is calibrated. A strategy can be real, reproducible, and statistically defensible while its expected dollar edge is still systematically overstated.

This layer closes that loop conservatively. Forward shadow evidence is allowed to reduce or block a claimed executable edge. It is never allowed to increase the model's edge.

## Scope

Calibration is isolated by:

- exact `runtimeIdentityHash`;
- exact market symbol;
- positive predicted net-edge decisions only.

Evidence from one promoted binary/configuration or one market does not calibrate another.

## Independence guard

Economic recomputation can create more than one decision/trial around a single certified signal. Counting every recomputation as a new observation would manufacture sample size.

Calibration therefore counts one sample per certified signal episode. Shadow trials now persist `certifiedShadowSignalObservationId`; older rows can fall back to the exact runtime identity, symbol, side and signal timestamp as the episode key.

## Minimum forward evidence

The initial policy is deliberately fixed rather than caller-tunable:

```text
policy version          certified_shadow_edge_discount_v1
minimum signal episodes 20
minimum distinct days   5
positive predictions    only
upward adjustment       forbidden
```

Before both minimums are met, a certified strategy economic decision may still open a shadow trial, but automated economic admission is fail-closed with explicit calibration blockers. This allows the system to collect the evidence needed to graduate without using missing evidence as permission to trade.

## Edge discount

For the exact runtime identity + symbol, the calibration set computes:

```text
aggregate capture = sum(canonical shadow net P&L) / sum(predicted net executable edge)
sign accuracy      = fraction of positive-edge predictions with positive shadow net P&L
```

Aggregate capture is bounded to `[0, 1]` for admission. The executable-edge multiplier is:

```text
edge multiplier = min(bounded aggregate capture, sign accuracy)
```

The resulting decision edge is:

```text
shadow calibrated edge = raw net executable edge * edge multiplier
```

for positive raw edge. Negative raw edge is never made less negative by calibration.

Consequences:

- realized forward underperformance discounts future edge;
- poor sign accuracy constrains a strategy even if a few large winners make aggregate dollars look strong;
- forward outperformance cannot make the model claim more edge than it originally predicted;
- a calibrated edge at or below the existing minimum edge threshold remains blocked.

## Avoiding feedback contamination

A new shadow trial records the **raw, pre-calibration** economic forecast even when admission uses the discounted edge. Otherwise the calibration loop would compare future outcomes against its own already-discounted prediction and gradually hide the original model's overstatement.

The economic decision therefore retains:

- `rawNetExecutableEdgeUsd`;
- `shadowCalibratedNetExecutableEdgeUsd`;
- `shadowCalibration` + hash;
- final admission blockers.

The shadow trial continues to bind the raw prediction.

## What this does not prove

This is an admission guard, not a claim of statistical independence or future profitability.

The 20-episode / 5-day floor prevents tiny-sample activation and obvious same-signal pseudoreplication, but clustered market regimes can still correlate outcomes. The first policy intentionally avoids a complex fitted calibration model while the forward dataset is small.

When materially more forward evidence exists, the next scientific upgrade should evaluate regime-aware or block-aware confidence bounds using the actual shadow distribution. That future method must preserve the one-way safety property: uncertainty can reduce capital authority, never manufacture expected return.

## Safety invariant

This slice does not change the current execution-lifecycle certification boundary. PR #55 still records shadow operation with `execution_lifecycle_certified=false`, and the overseer continues to block automated strategy entry until the complete execution lifecycle is separately proven. Live execution remains blocked.
