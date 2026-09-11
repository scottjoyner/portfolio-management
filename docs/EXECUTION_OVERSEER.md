# Execution Overseer Authorization Boundary

Status: canonical paper/demo admission control; live entry remains uncertified and blocked.

## Purpose

The execution boundary guarantees that an agent, strategy, API caller, or stale draft cannot self-authorize capital use or choose its own final position size.

The active path separates four independently hash-bound objects:

1. the requested material `TradeIntent`;
2. a deterministic `PortfolioAllocation` derived from authoritative portfolio state;
3. a fresh `CapitalRiskSnapshot` derived from the same authoritative state for the allocator-approved intent;
4. the `OverseerDecision` that binds the approved intent to that exact allocation and capital-risk verdict.

Caller-supplied `riskDecision` data is not an authority input to the final decision.

## Trust boundary

```text
agent / strategy / caller
        |
        | requested material trade intent
        v
ExecutionEngine
        |
        +--> requested TradeIntent envelope + hash
        |
        +--> ONE fresh riskStateProvider load
        |      + NAV / cash / high-water evidence
        |      + positions / pending executions
        |      + kill switch / reconciliation
        |      + instruments / market + liquidity state
        |      + optional covariance inputs
        |      + economic-decision lineage when required
        |
        +--> PortfolioAllocation v1
        |      + APPROVE / REDUCE / EXIT_ONLY / REJECT
        |      + approved material intent hash
        |      + allocation hash + decision hash + expiry
        |
        +--> approved TradeIntent
        |
        +--> CapitalRiskSnapshot v1
        |      + source hashes
        |      + deterministic derived risk inputs
        |      + expiry
        |      + snapshot hash
        |
        +--> canonical risk engine
        |      + deterministic risk verdict
        |      + allocation provenance
        |      + riskDecisionHash
        |
        +--> engine-owned OverseerDecision v3
        |      + approved intent hash
        |      + capital-risk snapshot hash + policy
        |      + risk-decision hash
        |      + decision hash + expiry
        v
 human approval when required
        |
        +--> stored authorization integrity check
        +--> fresh authoritative state reload
        +--> fresh PortfolioAllocation
        +--> fresh CapitalRiskSnapshot + risk verdict
        +--> new OverseerDecision for exact unchanged intent
        v
 submit()
        |
        +--> portfolio-allocation freshness check
        +--> capital-risk freshness check
        +--> authorization integrity check
        +--> freshness checks before each simulated fill
        v
 paper/demo execution
```

Allocation and capital-risk evaluation share the same authoritative load during one evaluation, avoiding a sizing/risk double-read race.

## Default behavior

`ExecutionEngine` defaults to:

- `minConfidence = 0.6`
- `requireApproval = true`
- `requireRiskCheck = true`
- time-bounded allocation, capital-risk and overseer authorization

With `requireRiskCheck=true`, absence of an authoritative state provider fails closed. Missing, stale, invalid or internally inconsistent required state rejects increasing-risk admission.

`requireRiskCheck=false` remains an explicit compatibility/testing escape hatch only. It must not be treated as a production live-enablement mechanism.

## Material intent binding

`TradeIntent` schema v2 binds material execution fields including:

- strategy, opportunity and source-agent identity;
- account and execution mode;
- venue, symbol, side, quantity and notional declarations;
- normalized order fields;
- entry, take-profit and stop-loss prices;
- trade plan / intent / purpose / position side;
- economic-decision, model-quote, forecast and cost-snapshot lineage IDs;
- caller-declared net executable edge, when present.

Derived allocation/risk decisions are deliberately not part of the material intent itself. Instead, each downstream artifact explicitly binds the exact material intent hash it evaluated.

Volatile lifecycle timestamps and execution status are excluded from the material trade hash.

A draft now persists:

- `requestedTradeIntentEnvelope`
- `requestedTradeIntentHash`
- `tradeIntentEnvelope`
- `tradeIntentHash`
- `portfolioAllocation`
- `portfolioAllocationHash`
- `portfolioAllocationDecisionHash`
- `capitalRiskSnapshot`
- `capitalRiskSnapshotHash`
- `riskDecision`
- `riskDecisionHash`
- `overseerDecision`

For manual paper/demo requests, the approved intent may be a deterministic reduction of the requested intent. For agent/economic requests, any required reduction fails closed and requires fresh economics at the suggested smaller notional.

## Portfolio allocation binding

The normalized canonical risk decision carries:

```text
portfolioAllocationHash
portfolioAllocationPolicyVersion
portfolioAllocationDecisionHash
```

Stored authorization independently verifies the allocation artifact hash, decision hash, approval state, exact approved-intent binding and expiry before verifying the capital-risk snapshot.

This makes allocation a real authorization layer rather than advisory sizing metadata.

See `docs/PORTFOLIO_ALLOCATOR.md` for policy and sizing details.

## Overseer decision v3

The v3 decision contains:

```text
schemaVersion
policyVersion
decision
approved
reasons[]
intentHash
capitalRiskSnapshotHash
capitalRiskPolicyVersion
riskDecisionHash
evaluatedAt
validUntil
requiresHumanApproval
mode
decisionHash
```

The allocator provenance is transitively bound through `riskDecisionHash`; the canonical risk decision itself carries the allocator hash, policy version and decision hash.

Current decision labels are:

- `PAPER`
- `DEMO`
- `LIVE_REQUIRES_HUMAN`
- `REJECT`

`LIVE_REQUIRES_HUMAN` is descriptive only. `live` remains rejected before submission and does not constitute live certification.

## Approval-time behavior

`approve(executionId)` never blindly submits a draft. It:

1. recomputes the current material intent hash;
2. verifies the persisted intent envelope;
3. verifies the persisted portfolio-allocation hash, decision hash, expiry and intent binding;
4. verifies the persisted capital-risk snapshot hash and intent binding;
5. verifies the persisted risk-decision hash;
6. verifies the overseer decision hash, bindings and expiry;
7. reloads fresh authoritative operator state;
8. excludes the current draft from its own pending-reservation accounting;
9. reruns portfolio allocation;
10. rebuilds the capital-risk snapshot on the freshly allocated intent;
11. reruns deterministic risk policy;
12. issues a new overseer decision only for the exact unchanged approved intent;
13. only then enters submission.

If current portfolio state would require a different size, approval fails with `portfolio_allocation_changed` / `portfolio_allocation_replan_required`. It does not silently mutate the reviewed draft.

`submit()` repeats authorization validation and independently verifies both allocation and capital-snapshot freshness. Simulated fills repeat freshness checks before proceeding.

## Risk-reducing posture

The allocator can place the portfolio into `EXIT_ONLY` when existing drawdown, leverage, concentration, covariance, concurrency or liquidity conditions make increasing risk unsafe.

A covered sell may still proceed through the downstream capital-risk gate so the system does not trap itself in a risky position merely because new entries are forbidden. Inventory checks remain authoritative; `EXIT_ONLY` does not permit uncovered shorts.

## Audit lineage

Targeted execution submit/approval audit events include:

- overseer decision label;
- trade-intent hash;
- portfolio-allocation hash;
- portfolio-allocation decision hash;
- capital-risk snapshot hash;
- risk-decision hash.

This gives operator/system-truth surfaces a content-addressed path from an execution back through portfolio sizing, exact material intent and admitted capital state.

## Migration behavior

Draft executions created before Overseer v3 do not have required PortfolioAllocation v1 evidence. After deployment they fail closed at approval/submission. Do not grandfather them; recreate any still-valid intent through the normal request path.

The content hashes prove deterministic integrity inside the supported lifecycle. They are not signatures against an actor with arbitrary in-process code/state mutation. A later trusted-runner/signing boundary can strengthen this.

## Deliberate remaining boundary

The execution boundary now includes canonical portfolio-wide allocation plus canonical per-intent capital admission, but it is still not an autonomous live-capital envelope.

Remaining work includes:

- production empirical correlation/volatility ingestion with strong freshness/provenance;
- multi-account/multi-currency and FX aggregation;
- richer liquidation/funding/borrow/tax/settlement risk where applicable;
- joint optimization across multiple simultaneous candidate trades;
- shadow/live execution calibration and canary controls;
- signed or externally attested state evidence for hostile-storage assumptions;
- explicit live execution certification and operator release authority.

## Safety invariant

This module must not be used as justification for enabling live trading. Real-capital entry remains blocked until live execution certification, supervised canary controls and independent operational acceptance are separately proven.
