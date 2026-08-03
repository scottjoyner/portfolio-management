# Production-Paper Gap Register

This register tracks the work required to move PR #30 from draft to reviewable for the first supervised production-paper deployment. It does not authorize live trading.

## Status definitions

- **Closed:** implementation and required automated evidence exist on the current branch.
- **Implemented — awaiting exact-head evidence:** code and documentation are committed, but a new exact-head green run is required.
- **In progress:** an implementation slice is actively defined or underway.
- **Blocked — manual:** requires the intended host, named owners, or external infrastructure.
- **Planned:** acceptance criteria are defined but implementation has not started.

## Current register

| ID | Gap | Status | Acceptance criteria | Evidence / next action |
|---|---|---|---|---|
| G-001 | Truthful maintained and generated test inventory | Closed | Maintained suite passes; every generated file is either active and passing or explicitly retired with a reason; no active timeouts. | Full inventory run #65 on head `cf7b552`; 714 maintained tests collected, 511 active generated files passed, 72 historical snapshots retired. |
| G-002 | Runner-normalized performance gate | Implemented — awaiting exact-head evidence | Checked-in runner profile; warmups and repeated samples; median, p95, and throughput limits; runner drift fails CI; job blocks `release-readiness`. | `config/release-performance-thresholds.json`, threshold tests, blocking `performance-gate`. Run exact-head CI and record the replacement artifact. |
| G-003 | Broad whole-state execution rewrites | In progress | Execution submit, approve, reject, cancel, order, and fill paths persist through normalized row repositories with expected versions and idempotency; no delete-and-reinsert operator-state save occurs on those routes; append-only audit evidence remains consistent. | Add a targeted execution-route persistence API and tests proving no broad save/synchronization is invoked. Retain compatibility synchronization only for import/hydration boundaries. |
| G-004 | Deterministic paid-agent counterfactual replay | Planned | Agent and bot receive identical immutable market window, decision timestamp, capital, fee/slippage model, risk limits, and instrument universe; replay produces reproducible action/PnL deltas; attribution records link provider cost, decision, execution, and counterfactual hash; missing evidence remains pending. | Build a replay envelope and attribution command around the existing replay engine, competition scoreboard, and economic attribution records. Add fixture-based determinism and cost-adjusted winner tests. |
| G-005 | Off-host backup retention | Planned | Logical dumps leave the PostgreSQL volume; destination, encryption, retention, checksum, and ownership are configured; scheduled restore verification exists; failed upload or verification is visible and blocks deployment certification. | Add signed backup manifests and a pluggable filesystem/S3-compatible uploader. Target destination must be supplied by the deployment owner. |
| G-006 | Immutable external audit anchoring | Planned | Periodic audit-chain roots are exported to an append-only or WORM-capable external destination; local and external roots can be verified; anchor gaps and mismatches alert and fail certification. | Define anchor record format and exporter; deployment owner selects the immutable destination. |
| G-007 | Target-host rehearsal | Blocked — manual | Real secrets, TLS/ingress, local inference nodes, monitoring, backup destination, kill switch, restart, restore, and rollback rehearsal pass on the intended host. | Requires host access and a recorded release session. |
| G-008 | Named operational ownership | Blocked — manual | Release operator, code reviewer, security reviewer, rollback owner, incident owner, monitoring destination, backup destination, image digests, and accepted residual risks are recorded. | Populate the release record before leaving draft. |
| G-009 | Human review | Blocked — manual | Code, architecture, security, and operational reviews are completed with no unresolved blocking threads. | No reviewer is currently assigned. |

## Execution order

1. Verify G-002 on the new exact head and retain the performance artifact.
2. Close G-003 by routing execution lifecycle writes through targeted optimistic persistence.
3. Implement G-004 with immutable replay envelopes and cost-adjusted attribution.
4. Implement the destination-neutral portions of G-005 and G-006; leave credentials and destination selection to host configuration.
5. Complete G-007 through G-009 during the supervised deployment review.

## Evidence rule

Every code or documentation commit invalidates prior exact-head certification. The PR body may cite a run only when the workflow and artifact records identify the current branch head. Historical green runs remain useful diagnostic evidence but cannot certify a newer commit.

## Scope rule

Closing this register certifies only the supervised production-paper path. Live order submission, live settlement, automatic broker certification, unsupervised promotion, and remote model execution by default remain outside scope.
