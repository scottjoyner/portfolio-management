# Production-Paper Gap Register

This register tracks the work required for the first supervised production-paper deployment. PR #30 merged to `main` on 2026-08-03; this register is maintained against the current `main` head rather than against a pull request. It does not authorize live trading.

**Current certified head:** `66397e672` ("Add canonical capital risk snapshot gate"), run `36749573705`, all twelve `release-readiness` jobs green. Any commit after this SHA invalidates the evidence below.

## Status definitions

- **Closed:** implementation and required automated evidence exist on the current branch.
- **Implemented — awaiting exact-head evidence:** code and documentation are committed, but a new exact-head green run is required.
- **In progress:** an implementation slice is actively defined or underway.
- **Blocked — manual:** requires the intended host, named owners, or external infrastructure.
- **Planned:** acceptance criteria are defined but implementation has not started.

## Current register

| ID | Gap | Status | Acceptance criteria | Evidence / next action |
|---|---|---|---|---|
| G-001 | Truthful maintained and generated test inventory | Closed | Maintained suite passes; every generated file is either active and passing or explicitly retired with a reason; no active timeouts. | Original evidence: full inventory run #65 on head `cf7b552`; 714 maintained tests collected, 511 active generated files passed, 72 historical snapshots retired. Re-confirmed at `66397e672` by `broad-python-suite` in run `36749573705`. |
| G-002 | Runner-normalized performance gate | Closed | Checked-in runner profile; warmups and repeated samples; median, p95, and throughput limits; runner drift fails CI; job blocks `release-readiness`. | `config/release-performance-thresholds.json`, threshold tests, blocking `performance-gate`. Exact-head evidence: run `36749573705` on `66397e672`, `performance-gate` success, artifact `performance-smoke-<sha>`. |
| G-003 | Broad whole-state execution rewrites | Closed | Execution submit, approve, reject, cancel, order, and fill paths persist through normalized row repositories with expected versions and idempotency; no delete-and-reinsert operator-state save occurs on those routes; append-only audit evidence remains consistent. | `apps/api/src/executionRoutePersistence.mjs` routes through `persistExecutionMutation`; `tests/targeted-execution-persistence.test.mjs` rejects `broad_mutate_must_not_run`, `broad_save_must_not_run`, and direct `DELETE FROM` replacement. `operational:validate` asserts all four invariants. |
| G-004 | Deterministic paid-agent counterfactual replay | Planned | Agent and bot receive identical immutable market window, decision timestamp, capital, fee/slippage model, risk limits, and instrument universe; replay produces reproducible action/PnL deltas; attribution records link provider cost, decision, execution, and counterfactual hash; missing evidence remains pending. | Build a replay envelope and attribution command around the existing replay engine, competition scoreboard, and economic attribution records. Add fixture-based determinism and cost-adjusted winner tests. |
| G-005 | Off-host backup retention | Planned | Logical dumps leave the PostgreSQL volume; destination, encryption, retention, checksum, and ownership are configured; scheduled restore verification exists; failed upload or verification is visible and blocks deployment certification. | Add signed backup manifests and a pluggable filesystem/S3-compatible uploader. Target destination must be supplied by the deployment owner. |
| G-006 | Immutable external audit anchoring | Planned | Periodic audit-chain roots are exported to an append-only or WORM-capable external destination; local and external roots can be verified; anchor gaps and mismatches alert and fail certification. | Define anchor record format and exporter; deployment owner selects the immutable destination. |
| G-007 | Target-host rehearsal | Blocked — manual | Real secrets, TLS/ingress, local inference nodes, monitoring, backup destination, kill switch, restart, restore, and rollback rehearsal pass on the intended host. | Requires host access and a recorded release session. |
| G-008 | Named operational ownership | Blocked — manual | Release operator, code reviewer, security reviewer, rollback owner, incident owner, monitoring destination, backup destination, image digests, and accepted residual risks are recorded. | Populate the release record before leaving draft. |
| G-009 | Human review | Blocked — manual | Code, architecture, security, and operational reviews are completed with no unresolved blocking threads. | No reviewer is currently assigned. |
| G-010 | Local operator gate fidelity | Closed | The npm scripts named in the release checklist cannot report success without executing the tests, validators, and tree walks they claim to cover. | `npm test` previously expanded `tests/**/*.test.mjs` as a single-level glob and ran 1 of 62 files while exiting 0; it now routes through `scripts/run-node-test-shard.mjs`, the same recursive discovery CI uses. `scripts/lint.mjs` no longer aborts on a dangling symlink. `scripts/validate-security.mjs` now scans git-visible files instead of the whole working tree, so a correctly configured host no longer fails `npm run build` or `certify-production-paper`. Regression coverage: `tests/node-test-script-coverage.test.mjs`. |
| G-011 | Research and execution stack landing | In progress | The eleven merged research/execution slices are integrated into `main` under one exact-head green run, with the execution authorization boundary reconciled rather than silently overwritten. | Source PRs #39, #40, #42, #44, #46, #48, #50, #54, #55, #56, #59 are merged; #52 closed as superseded by #54. #39 is on `main` at `66397e672`. The remaining ~136 commits are on `agent/certified-execution-lifecycle` and are **not** in `main`. Integrating them conflicts in 5 files — `packages/execution/src/overseer.mjs` (7 hunks), `tests/execution-engine.test.mjs` (5), `apps/api/src/executionRoutePersistence.mjs`, `packages/execution/src/executionEngine.mjs`, `docs/EXECUTION_OVERSEER.md`. Both sides rewrite the same fail-closed authorization boundary, so the resolution needs a deliberate decision rather than a mechanical merge. |

## Execution order

1. Re-run the exact-head gate after any further commit; G-001 through G-003 are closed at `5ab7f81f` and must be re-evidenced on the next head.
2. Implement G-004 with immutable replay envelopes and cost-adjusted attribution.
3. Implement the destination-neutral portions of G-005 and G-006; leave credentials and destination selection to host configuration.
4. Complete G-007 through G-009 during the supervised deployment review.

## Evidence rule

Every code or documentation commit invalidates prior exact-head certification. A run may be cited only when the workflow and artifact records identify the current branch head. Historical green runs remain useful diagnostic evidence but cannot certify a newer commit.

## Scope rule

Closing this register certifies only the supervised production-paper path. Live order submission, live settlement, automatic broker certification, unsupervised promotion, and remote model execution by default remain outside scope.
