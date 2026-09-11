# Canonical Capital Risk Snapshot

Status: active paper/demo execution-admission artifact. Live trading remains blocked.

## Objective

`CapitalRiskSnapshot` is the deterministic bridge between persisted operator state and the existing risk engine. It exists so execution admission does not depend on a caller saying that risk is acceptable.

The snapshot is built inside the execution engine from a fresh state-provider read and is bound to one immutable material trade intent by SHA-256.

Current schema/policy:

- schema version: `1`
- policy version: `canonical-capital-risk-v1`
- default authoritative-state maximum age: 30 seconds
- default market-data maximum age: 120 seconds unless operator config supplies a stricter/explicit order-book threshold
- default minimum economic edge: 1 bp when economic lineage is required, unless configured otherwise

## Authoritative inputs

The active `/api/execution/plan`, `/api/execution/execute`, and `/api/execution/:id/approve` paths provide a fresh operator-store loader to the execution engine.

The snapshot derives facts from:

- requested account state and cash;
- open positions;
- pending executions;
- kill-switch state;
- execution/fill reconciliation plus reconciliation audit events;
- active instrument catalog;
- persisted market-data snapshots;
- configured position-size / order-book limits;
- persisted economic decision, forecast, execution-cost snapshot, and model-usage lineage when the intent is agent/economic in origin.

Each source represented in the snapshot receives a deterministic source hash. The final snapshot receives its own `snapshotHash`.

## Fail-closed state classes

The snapshot records three explicit state classes:

- `missingRequiredState[]` — required authority or provenance is absent;
- `staleRequiredState[]` — required state exists but is too old;
- `invalidRequiredState[]` — state exists but is contradictory, unusable, or fails policy.

The evaluator converts those into stable reasons:

```text
capital_risk_missing:<reason>
capital_risk_stale:<reason>
capital_risk_invalid:<reason>
```

The existing canonical risk-engine reasons are also preserved, such as `kill_switch_on`, `orderbook_stale`, `insufficient_balance`, `pair_not_approved`, `unresolved_reconciliation_discrepancy`, and `notional_limit_exceeded`.

## Timestamp semantics

A missing timestamp is unavailable. In particular, `null`, `undefined`, and an empty string are **not** interpreted as Unix epoch and cannot accidentally satisfy a freshness test.

The state provider timestamps its observation after the authoritative store read completes. The risk evaluation time is then captured after that provider returns, avoiding a race where a slow load could make newly observed state appear to come from the future.

Snapshot `validUntil` is the earliest relevant expiry among:

- the short capital-snapshot TTL;
- authoritative state freshness;
- market-data freshness;
- referenced forecast expiry;
- referenced execution-cost expiry;
- referenced model-quote age when applicable.

## Order-bundle semantics

Risk sizing is derived from the full normalized order bundle, not from a caller-provided aggregate and not from the first order alone.

The builder:

- sums every order quantity/notional;
- rejects mixed symbols in one intent;
- rejects mixed sides in one intent;
- rejects mixed venues in one intent;
- uses a conservative market/requested price for cash-increasing orders;
- fails closed when a required reference price cannot be established.

This prevents a multi-order request from being admitted using only the first leg's size.

## Cash and pending-execution semantics

For cash-consuming sides (`buy`, `yes`, `no`):

```text
available cash after pending
  = account cash
  - all same-account pending cash-consuming executions
```

Pending cash consumption is account-wide, not symbol-local. A pending ETH buy can therefore reduce cash available to a BTC request.

Any still-pending execution that lacks account ownership is a provenance blocker. It cannot silently disappear from capital accounting simply because the engine cannot determine which account owns it.

## Position and exposure semantics

Current position exposure is marked conservatively from position mark/current/average price with market price as fallback.

For exposure-increasing orders:

```text
projected symbol exposure
  = current symbol exposure
  + same-account/same-symbol pending increasing exposure
  + proposed order-bundle notional
```

For `sell`, current admission semantics treat the order as inventory-reducing. A sell must be covered by account-owned long inventory after accounting for pending sells.

```text
projected symbol exposure
  = max(
      0,
      current symbol exposure
      - pending sell exposure
      - proposed sell notional
    )
```

This is important: a risk-reducing sell must not be rejected merely because the old formula added the exit notional on top of the position. Opening an uncovered short is not admitted by this rule because the inventory sufficiency check fails.

## Account and venue validity

The current paper capital gate requires:

- the requested account to exist;
- account cash and timestamp to exist;
- account status to be `connected`;
- account currency to be USD until an authoritative conversion layer exists;
- the instrument to exist on a compatible venue and be `active`;
- market data to exist for the symbol/compatible venue and have a ready status;
- market data to satisfy the configured freshness threshold.

Paper/demo venue aliases are normalized for the existing `coinbase` / `coinbase-paper` style topology. A genuinely mismatched venue fails closed.

## Reconciliation semantics

Admission reconstructs reconciliation risk from current executions/fills plus the reconciliation audit trail.

Examples that block:

- pending settlement;
- a filled execution whose filled quantity does not match expected order quantity;
- a latest explicit `execution_recon_issues` audit result.

A later `execution_reconciled` event for the same execution can supersede an earlier issue event.

## Economic-edge lineage

Manual paper/demo requests without agent/economic lineage use the deterministic manual-paper edge policy and do not accept caller-provided positive edge as authority.

If the intent references an agent/economic source, the snapshot reconstructs the economic chain from persisted state. It requires, as applicable:

- an economic decision that exists and has `executionAllowed=true`;
- symbol and referenced lineage IDs to match;
- a valid, unexpired forecast;
- a valid, unexpired execution-cost snapshot;
- a reconciled model-usage quote when referenced;
- an economic decision regenerated after model-cost reconciliation;
- persisted `netExecutableEdgeUsd`;
- equality between any caller-declared edge and the persisted authoritative edge.

Edge basis points are then recomputed from persisted net executable edge divided by the full proposed order notional.

## Integrity and refresh behavior

The artifact is bound to `tradeIntentHash`, canonically serialized, and SHA-256 hashed.

The overseer v2 binds:

- `tradeIntentHash`;
- `capitalRiskSnapshotHash`;
- capital-risk policy version;
- `riskDecisionHash`.

A draft cannot carry an old favorable capital state through approval. Approval first verifies the stored artifact, then reloads state and builds a new snapshot/risk/overseer chain for the unchanged trade intent.

`submit()` checks capital-snapshot freshness again, and simulated fills check freshness before proceeding.

## Caller authority

The caller cannot authorize itself by sending:

```json
{"riskDecision":{"approved":true,"reasons":[]}}
```

That object is not used as the final capital verdict. Canonical operator state and deterministic policy decide admission.

Likewise, a caller-supplied rejection cannot replace the engine's derived result. This keeps the API request from becoming a hidden policy channel in either direction.

## Deliberate limitations / next layer

This artifact is per-intent capital admission, not the full canonical portfolio allocator. It currently does not provide the final authority for:

- portfolio high-water mark / drawdown budgeting;
- portfolio-wide gross and net leverage;
- cross-asset and cross-strategy correlation clusters;
- marginal portfolio risk contribution;
- joint allocation among multiple simultaneously attractive opportunities;
- global liquidity/liquidation stress;
- signed state attestation against a compromised persistence layer;
- autonomous live-capital envelopes.

Those remain explicit next-layer requirements. Until they are implemented and independently accepted, `live` stays blocked.

## Non-negotiable invariant

No agent, strategy, UI request, or caller-supplied risk object may bypass the canonical snapshot, deterministic risk engine, or overseer authorization boundary in production paper execution. Nothing in this artifact enables broker credentials, withdrawals, higher leverage, larger hard limits, or live trading.