# Execution Overseer Authorization Boundary

Status: canonical paper/demo admission control; live entry remains uncertified and blocked.

## Purpose

The execution boundary is responsible for one narrow but critical guarantee: an agent, strategy, API caller, or stale draft cannot self-authorize capital use.

The active path now separates three independently hash-bound objects:

1. the immutable material `TradeIntent`;
2. a fresh, engine-built `CapitalRiskSnapshot` derived from authoritative operator state;
3. the `OverseerDecision` that binds the trade intent to that exact capital-risk snapshot and derived risk verdict.

Caller-supplied `riskDecision` data is not an authority input to the final decision.

## Trust boundary

```text
agent / strategy / caller
        |
        | proposed material trade intent
        v
ExecutionEngine
        |
        +--> canonical TradeIntent envelope
        |      + SHA-256 tradeIntentHash
        |
        +--> fresh riskStateProvider load
        |      + account / positions / pending executions
        |      + kill switch / reconciliation
        |      + instruments / market snapshots
        |      + economic-decision lineage when required
        |
        +--> CapitalRiskSnapshot
        |      + source hashes
        |      + deterministic derived risk inputs
        |      + expiry
        |      + SHA-256 snapshotHash
        |
        +--> canonical risk engine
        |      + deterministic risk verdict
        |      + riskDecisionHash
        |
        +--> engine-owned OverseerDecision v2
        |      + intent hash
        |      + capital-risk snapshot hash + policy version
        |      + risk-decision hash
        |      + decision hash + expiry
        v
 human approval when required
        |
        +--> stored authorization integrity check
        +--> fresh authoritative state reload
        +--> new CapitalRiskSnapshot + risk verdict
        +--> new OverseerDecision
        v
 submit()
        |
        +--> snapshot freshness check
        +--> authorization integrity check
        +--> freshness check before each simulated fill
        v
 paper/demo execution
```

The final risk and overseer decisions are downstream of the caller. A request may reference economic lineage, but it cannot provide the capital snapshot or final authorization.

## Default behavior

`ExecutionEngine` defaults to:

- `minConfidence = 0.6`
- `requireApproval = true`
- `requireRiskCheck = true`
- time-bounded risk and overseer authorization

With `requireRiskCheck=true`, absence of an authoritative state provider fails closed. Missing, stale, invalid, or internally inconsistent required state rejects admission.

`requireRiskCheck=false` remains an explicit compatibility/testing escape hatch only. It must not be treated as a production live-enablement mechanism.

## Material intent binding

`TradeIntent` schema v2 binds material execution fields including:

- strategy, opportunity, and source-agent identity;
- account and execution mode;
- venue, symbol, side, quantity and notional declarations;
- normalized order fields;
- entry, take-profit and stop-loss prices;
- trade plan / intent / purpose / position side;
- economic-decision, model-quote, forecast and cost-snapshot lineage IDs;
- caller-declared net executable edge, when present.

The derived `riskDecision` is deliberately **not** part of the material trade-intent hash. This allows approval to refresh capital state and produce a new risk decision without falsely classifying an unchanged trade as mutated.

Volatile lifecycle timestamps and execution status are also excluded from the material trade hash.

A draft persists:

- `tradeIntentEnvelope`
- `tradeIntentHash`
- `capitalRiskSnapshot`
- `capitalRiskSnapshotHash`
- `riskDecision`
- `riskDecisionHash`
- `overseerDecision`

A material trade mutation after draft creation causes approval/submission to fail.

## Overseer decision v2

The v2 decision contains:

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

Current decision labels are:

- `PAPER`
- `DEMO`
- `LIVE_REQUIRES_HUMAN`
- `REJECT`

`LIVE_REQUIRES_HUMAN` is descriptive only in the current implementation. `live` is still rejected before submission and does not constitute live certification.

## Approval-time behavior

`approve(executionId)` never blindly submits a draft. It:

1. recomputes the current material intent hash;
2. verifies the persisted intent envelope;
3. verifies the persisted capital-risk snapshot hash and intent binding;
4. verifies the persisted risk-decision hash;
5. verifies the overseer decision hash, bindings, and expiry;
6. reloads fresh authoritative operator state;
7. rebuilds the capital-risk snapshot;
8. reruns deterministic risk policy;
9. issues a new overseer decision for the unchanged intent;
10. only then enters submission.

`submit()` repeats authorization validation and independently verifies capital-snapshot freshness. Simulated fills also check snapshot freshness before proceeding.

## Audit lineage

Targeted execution submit/approval audit events include:

- overseer decision label;
- trade-intent hash;
- capital-risk snapshot hash;
- risk-decision hash.

This gives operator/system-truth surfaces a content-addressed path from an execution back to the exact material intent and capital state admitted by the engine.

## Deliberate remaining boundary

This slice establishes authoritative per-execution capital admission. It does **not** yet replace the future canonical portfolio allocator.

Still outside this boundary are full portfolio-wide controls such as:

- equity high-water mark and canonical drawdown budget;
- gross/net leverage across every venue/account;
- correlation/cluster concentration across assets and strategies;
- portfolio-level marginal risk contribution;
- portfolio liquidity capacity and liquidation stress;
- strategy capital budgets allocated jointly across competing opportunities;
- signed/externally attested state snapshots for hostile-storage assumptions.

Those belong downstream in the canonical allocator/portfolio-risk layer before any autonomous live-capital envelope can exist.

## Safety invariant

This module must not be used as justification for enabling live trading. Real-capital entry remains blocked until portfolio-wide allocation, live execution certification, supervised canary controls, and independent operational acceptance are separately proven.