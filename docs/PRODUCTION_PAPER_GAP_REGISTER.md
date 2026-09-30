# Production-Paper Gap Register

This register tracks the work required for the first supervised production-paper deployment. PR #30 merged to `main` on 2026-08-03; this register is maintained against the current `main` head rather than against a pull request. It does not authorize live trading.

## Evidence lookup

A SHA written into this document goes stale the moment the next commit lands, including the commit that writes it. So do not treat the SHA below as the certification. The authoritative check is always the same and must be run against the head being deployed:

```bash
git rev-parse HEAD
gh run list --branch main --limit 1 \
  --json headSha,conclusion -q '.[] | select(.headSha == "'"$(git rev-parse HEAD)"'")'
```

A deploy is certified only when that returns `conclusion: success` for the exact `HEAD` you are deploying, and the run includes the `release-readiness` job. Anything else — a green run on an older SHA, or a run that predates the last commit — is historical evidence, not certification.

For reference, the substantive release work landed as follows:

| Change | Head | Run |
|---|---|---|
| Research and execution stack (G-011) | `79470e34a` | `36768911776` |
| This register update | `41f039a68` | `36769319738` |

## Status definitions

- **Closed:** implementation and required automated evidence exist on the current branch.
- **Implemented — awaiting exact-head evidence:** code and documentation are committed, but a new exact-head green run is required.
- **In progress:** an implementation slice is actively defined or underway.
- **Blocked — manual:** requires the intended host, named owners, or external infrastructure.
- **Planned:** acceptance criteria are defined but implementation has not started.

## Current register

| ID | Gap | Status | Acceptance criteria | Evidence / next action |
|---|---|---|---|---|
| G-001 | Truthful maintained and generated test inventory | Closed | Maintained suite passes; every generated file is either active and passing or explicitly retired with a reason; no active timeouts. | Original evidence: full inventory run #65 on head `cf7b552`. Re-confirmed at `79470e34a` by `broad-python-suite`, `maintained-python-inventory`, and the eight `coverage-python-inventory` shards: 691 maintained tests pass, 408 Node tests pass. |
| G-002 | Runner-normalized performance gate | Closed | Checked-in runner profile; warmups and repeated samples; median, p95, and throughput limits; runner drift fails CI; job blocks `release-readiness`. | `config/release-performance-thresholds.json`, threshold tests, blocking `performance-gate`. Exact-head evidence: run `36768911776` on `79470e34a`, `performance-gate` success, artifact `performance-smoke-<sha>`. See Evidence lookup above before deploying. |
| G-003 | Broad whole-state execution rewrites | Closed | Execution submit, approve, reject, cancel, order, and fill paths persist through normalized row repositories with expected versions and idempotency; no delete-and-reinsert operator-state save occurs on those routes; append-only audit evidence remains consistent. | `apps/api/src/executionRoutePersistence.mjs` routes through `persistExecutionMutation`; `tests/targeted-execution-persistence.test.mjs` rejects `broad_mutate_must_not_run`, `broad_save_must_not_run`, and direct `DELETE FROM` replacement. `operational:validate` asserts all four invariants. |
| G-004 | Deterministic paid-agent counterfactual replay | Planned | Agent and bot receive identical immutable market window, decision timestamp, capital, fee/slippage model, risk limits, and instrument universe; replay produces reproducible action/PnL deltas; attribution records link provider cost, decision, execution, and counterfactual hash; missing evidence remains pending. | Build a replay envelope and attribution command around the existing replay engine, competition scoreboard, and economic attribution records. Add fixture-based determinism and cost-adjusted winner tests. |
| G-005 | Off-host backup retention | Implemented — awaiting host destination | Logical dumps leave the PostgreSQL volume; destination, encryption, retention, checksum, and ownership are configured; scheduled restore verification exists; failed upload or verification is visible and blocks deployment certification. | `packages/backup/src/manifest.mjs` and `uploader.mjs` provide Ed25519-signed manifests, AES-256-GCM encryption, sha256 and size checks, release/operator provenance, a daily/weekly/monthly retention policy, and filesystem plus SigV4-signed S3-compatible uploaders. Uploads are read back and compared; a missing, unknown, or ambiguous destination is an error rather than a local fallback. `scripts/backup-export.mjs` and `scripts/backup-verify.mjs` exit non-zero on failure. Remaining: the deployment owner must supply `BACKUP_DESTINATION` and its credentials, and schedule the restore verification. |
| G-006 | Immutable external audit anchoring | Implemented — awaiting host destination | Periodic audit-chain roots are exported to an append-only or WORM-capable external destination; local and external roots can be verified; anchor gaps and mismatches alert and fail certification. | `packages/audit/src/anchor.mjs` defines the anchor record, Ed25519 signature, sequence chaining, and `compareLocalAndExternalRoots`. An anchor refuses to publish a root for an already-invalid chain, publication is exclusive-create and append-only, and a removed anchor is a sequence gap. The control this buys is proven by test: a locally rebuilt chain verifies perfectly on its own and is still caught by comparison against the published root. `scripts/audit-anchor.mjs` and `scripts/audit-anchor-verify.mjs` exit non-zero on mismatch. Remaining: the deployment owner must select and provide the immutable destination. |
| G-007 | Target-host rehearsal | Blocked — manual | Real secrets, TLS/ingress, local inference nodes, monitoring, backup destination, kill switch, restart, restore, and rollback rehearsal pass on the intended host. | Requires host access and a recorded release session. |
| G-008 | Named operational ownership | Blocked — manual | Release operator, code reviewer, security reviewer, rollback owner, incident owner, monitoring destination, backup destination, image digests, and accepted residual risks are recorded. | Populate the release record before leaving draft. |
| G-009 | Human review | Blocked — manual | Code, architecture, security, and operational reviews are completed with no unresolved blocking threads. | No reviewer is currently assigned. |
| G-010 | Local operator gate fidelity | Closed | The npm scripts named in the release checklist cannot report success without executing the tests, validators, and tree walks they claim to cover. | `npm test` previously expanded `tests/**/*.test.mjs` as a single-level glob and ran 1 of 62 files while exiting 0; it now routes through `scripts/run-node-test-shard.mjs`, the same recursive discovery CI uses. `scripts/lint.mjs` no longer aborts on a dangling symlink. `scripts/validate-security.mjs` now scans git-visible files instead of the whole working tree, so a correctly configured host no longer fails `npm run build` or `certify-production-paper`. Regression coverage: `tests/node-test-script-coverage.test.mjs`. |
| G-011 | Research and execution stack landing | Closed | The eleven merged research/execution slices are integrated into `main` under one exact-head green run, with the execution authorization boundary reconciled rather than silently overwritten. | Landed in #61 at `79470e34a`, 141 commits, 55 files, +11,979/-1,826. Two defects were fixed on the way: the accumulation was missing three of #55's commits (1,873 lines, including the single-position lifecycle tests) because `agent/shadow-edge-calibration` had forked before its base finished, and the five conflicts were a v2/v4 overseer version skew rather than a rewrite. Fail-closed posture verified: a v2-stamped decision does not verify under `execution-admission-v4-certified-runtime` and a bad-signature v4 decision does not verify either. See `docs/DEPLOYMENT_ROLLBACK_RUNBOOK.md` for the re-authorization consequence. |
| G-012 | Stored overseer authorization migration | Planned | Operator and execution state persisted under a superseded overseer policy can be re-authorized, or the host is drained before a policy change, so a policy upgrade cannot silently strand live state. | The policy moved from `execution-admission-v2` to `execution-admission-v4-certified-runtime` with no migration path; affected state fails closed and must be re-minted. Harmless for the first production-paper host, which holds no prior state. Required before any host carries state across a future policy change. |

## Execution order

1. Re-run the exact-head gate after any further commit; G-001 through G-003 are closed at `5ab7f81f` and must be re-evidenced on the next head.
2. Implement G-004 with immutable replay envelopes and cost-adjusted attribution.
3. Implement the destination-neutral portions of G-005 and G-006; leave credentials and destination selection to host configuration.
4. Complete G-007 through G-009 during the supervised deployment review.

## Evidence rule

Every code or documentation commit invalidates prior exact-head certification. A run may be cited only when the workflow and artifact records identify the current branch head. Historical green runs remain useful diagnostic evidence but cannot certify a newer commit.

## Scope rule

Closing this register certifies only the supervised production-paper path. Live order submission, live settlement, automatic broker certification, unsupervised promotion, and remote model execution by default remain outside scope.
