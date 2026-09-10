# Execution Overseer Authorization Boundary

Status: paper/demo admission control; live entry remains uncertified and blocked.

## Why this exists

The execution engine historically accepted a missing `riskDecision` as approved even though `requireRiskCheck` defaulted to true. A draft could then be operator-approved and submitted without proving that its material order intent still matched the object originally evaluated.

The overseer boundary makes execution admission explicit, fail-closed, content-addressed and revalidated.

## Trust boundary

```text
agent / strategy / caller
        |
        | proposed request + upstream risk evidence
        v
ExecutionEngine
        |
        +--> canonical TradeIntent envelope
        |      + SHA-256 intent hash
        |
        +--> engine-owned OverseerDecision
        |      + policy/schema version
        |      + expiry
        |      + decision hash
        |
        v
 human approval when required
        |
        +--> intent + decision revalidation
        v
 submit()
        |
        +--> authorization checked again
        v
 paper/demo execution
```

The caller may supply upstream risk evidence. The caller cannot supply the final `overseerDecision`; the engine creates it.

## Default behavior

`ExecutionEngine` defaults to:

- `minConfidence = 0.6`
- `requireApproval = true`
- `requireRiskCheck = true`
- a time-bounded overseer authorization

With `requireRiskCheck=true`:

- missing risk decision -> reject;
- malformed risk decision -> reject;
- explicit risk rejection -> reject;
- confidence below the configured floor -> reject.

`requireRiskCheck=false` exists only as an explicit compatibility/testing escape hatch. Missing risk is never silently approved under the production default.

## Material intent binding

The canonical intent envelope binds material execution fields including:

- strategy/opportunity/source-agent identity;
- account and execution mode;
- venue, symbol, side, quantity and notional;
- material order fields;
- entry, take-profit and stop-loss prices;
- trade plan / intent / purpose / position side;
- economic-decision, model-quote, forecast and cost-snapshot lineage IDs;
- net executable edge when supplied;
- normalized risk decision.

Volatile lifecycle timestamps and status are not part of the material intent hash.

A draft stores:

- `tradeIntentEnvelope`
- `tradeIntentHash`
- `overseerDecision`

Changing a material field after draft creation causes approval/submission to fail with an intent-integrity error.

## Overseer decision

The v1 decision contains:

```text
schemaVersion
policyVersion
decision
approved
reasons[]
intentHash
evaluatedAt
validUntil
requiresHumanApproval
mode
decisionHash
```

Current decisions are:

- `PAPER`
- `DEMO`
- `LIVE_REQUIRES_HUMAN`
- `REJECT`

`LIVE_REQUIRES_HUMAN` is descriptive only in this slice: `live` remains rejected and cannot reach submission.

## Approval-time behavior

`approve(executionId)` does not blindly submit the draft. It:

1. recomputes the material intent hash from the current stored execution;
2. checks the stored envelope hash;
3. validates the engine-owned decision hash;
4. checks expiry;
5. confirms the decision authorized the same intent hash;
6. reruns deterministic admission policy;
7. only then transitions to approved/submission.

`submit()` repeats stored authorization verification so direct callers cannot bypass the lifecycle gate.

## Audit lineage

Targeted execution submit/approval audit events include:

- overseer decision;
- trade-intent hash.

This allows operator/system-truth surfaces to connect a persisted execution to the exact material intent authorized by the engine.

## What this does not prove

This first boundary does **not** independently reconstruct portfolio/account state. The existing runtime risk engine can evaluate conditions such as kill switch, unresolved reconciliation, pair/compliance approval, market-data staleness, minimum edge, balance sufficiency and notional limits, but those inputs are not yet authoritatively assembled by the active execution route.

Therefore upstream `riskDecision` is explicit and integrity-bound, but not yet independently authenticated by this overseer.

The next capital-control slice should source authoritative:

- equity and high-water mark / drawdown;
- balances and open positions;
- pending orders;
- gross/net and correlation exposure;
- liquidity/spread/slippage state;
- market-data freshness;
- strategy certification/evidence state;
- kill switch and reconciliation state;

and calculate the final risk verdict downstream of the agent.

## Safety invariant

This module must not be used as justification for enabling live trading. Real-capital entry remains blocked until the independent portfolio allocator/overseer, live execution certification and supervised canary gates are separately proven.
