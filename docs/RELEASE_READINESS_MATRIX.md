# Release Readiness Matrix

This matrix defines what “ready for review” means for PR #30. It covers the first supervised **production-paper** deployment only. Live trading remains excluded.

## Status vocabulary

- **Blocking — automated:** must pass in the GitHub Actions `release-readiness` dependency graph.
- **Blocking — manual:** must be performed on the intended deployment host and recorded in the release record.
- **Open engineering blocker:** implementation is incomplete; the PR remains draft.
- **Diagnostic:** useful evidence, but not a release gate unless it exposes a regression in the release path.

## Readiness gates

| Area | Status | Evidence | Required outcome |
|---|---|---|---|
| Static production contract | Blocking — automated | `validation` job | Build, API contract, security, deployment, migration, UI, and operational validators pass. |
| Deterministic Node behavior | Blocking — automated | `node-tests-0..3` | Every shard passes. |
| Critical Python trading/accounting | Blocking — automated | `python-critical` | Competition, accounting, trader controls, epoch start, and rebalance tests pass. |
| Focused coverage | Blocking — automated | `coverage-gate` | Critical focused coverage remains above the configured threshold. |
| PostgreSQL migrations | Blocking — automated | `postgres-integration`; migration artifacts | Migrations 001–006 apply, a second run is idempotent, and checksums match. |
| Migration 006 execution persistence | Blocking — automated | `postgres-integration.tap` | Normalized execution, order, event, and read-model data survive a fresh store instance. |
| Restart hydration | Blocking — automated | PostgreSQL integration and production runtime smoke | A fresh execution engine and a restarted API hydrate persisted execution state without rewind. |
| Append-only execution evidence | Blocking — automated | PostgreSQL integration | Updating an execution event is rejected by PostgreSQL. |
| Durable scheduler leases | Blocking — automated | PostgreSQL integration | Enqueue, claim, heartbeat, and completion work against PostgreSQL. |
| Authenticated browser/runtime | Blocking — automated | `production-runtime-smoke.json` | Browser assets load, unauthenticated policy access is rejected, CSRF-protected policy save succeeds, and state survives restart. |
| Local fleet discovery | Blocking — automated and manual | Runtime smoke plus host smoke | Exact configured model IDs are healthy through `/v1/models`. |
| OpenRouter deployment gate | Blocking — automated and manual | deployment validator plus host environment review | Remote execution defaults off and requires both explicit enablement and a host-managed key. |
| UI routing policy | Blocking — automated and manual | runtime smoke plus operator screenshot/release note | Local-only, economic-auto, and OpenRouter-eligible modes persist; hard caps are visible. |
| Remote cost controls | Blocking — automated | intelligence routing tests | Per-request cap, daily cap, minimum value coverage, and local fallback fail closed. |
| Remote provider at-most-once | Blocking — automated | `openrouter-at-most-once.test.mjs` | Confirmed non-started HTTP failures may retry; known or uncertain provider attempts cannot issue a second billable POST. |
| Delayed `usage_pending` reconciliation | Blocking — automated | provider reconciliation tests and economic worker | Known generation IDs reconcile through metadata lookup with bounded backoff; exhaustion becomes explicit manual review. |
| Logical backup and restore | Blocking — automated and manual | `portfolio-smoke.dump`, restore evidence, host pre-deploy restore | A dump restores into a clean database with migration 006 and execution data intact. |
| Worker liveness | Blocking — automated and manual | deployment validator, Compose health, host `docker inspect` | Worker heartbeat is fresh; stale or failed runs make the container unhealthy. |
| Locked Node dependencies | Blocking — automated | `npm ci`, deployment validator | Production image installs from `package-lock.json` with `npm ci`. |
| Secrets and session handling | Blocking — automated and manual | security validator, runtime smoke, host browser test | No secret is committed; browser credentials remain in same-tab `sessionStorage`; auth and CSRF are required. |
| Paper-only safety | Blocking — automated and manual | runtime validation, Compose render, live-route smoke | All live submission/redemption flags remain false and live routes reject. |
| Kill switch | Blocking — manual | host smoke/release record | Operator can stop all paper sessions and verify no new paper submissions. |
| Reverse proxy/TLS | Blocking — manual | host configuration evidence | API binds as intended; TLS and access controls are active at the trusted ingress. |
| Host resource sizing | Blocking — manual | release record | PostgreSQL, API, and worker memory/CPU limits fit the deployment host. |
| Rollback rehearsal | Blocking — manual | `DEPLOYMENT_ROLLBACK_RUNBOOK.md` release record | Prior image and restored backup pass readiness and smoke in a disposable rollback target. |
| Remaining whole-state rewrites | Open engineering blocker | PostgreSQL mutation review/tests | Operational mutations use targeted optimistic rows instead of broad compatibility replacement. |
| Deterministic paid-agent counterfactual replay | Open engineering blocker | replay/attribution tests | Automatic attribution compares paid-agent decisions with a reproducible bot counterfactual. |
| Legacy Python suite | Diagnostic | `legacy-python-diagnostic` | Failures are triaged; any release-path regression is promoted to blocking. |
| Performance suite | Diagnostic | `performance-diagnostic` | Results are retained; a severe release-path regression is promoted to blocking. |

## Required host certification sequence

1. Review and populate `.env.production.example` outside source control.
2. Record the exact commit and prior image digest.
3. Run locked install, tests, build, and strict runtime validation.
4. Render Compose and confirm all paper-only flags.
5. Create and restore a pre-deploy backup in a disposable database.
6. Deploy PostgreSQL, migrations, API, and worker.
7. Confirm API and worker container health.
8. Run `npm run smoke:production-paper`.
9. Authenticate the browser session and inspect Today, Risk & System, and Intelligence Routing.
10. Save the intended routing policy and caps.
11. Restart the API and verify execution and policy persistence.
12. Exercise the paper kill switch.
13. Rehearse the rollback target using the prior image and restored backup.
14. Record go/no-go with named operator, reviewer, and incident owner.

## Conditions for moving the PR out of draft

The PR should remain draft until:

- the exact-head `release-readiness` job is green;
- every open engineering blocker above is closed or explicitly removed from this release scope with a safe fail-closed implementation;
- the host certification sequence has been completed or a clearly identified deployment owner has accepted the remaining manual gates;
- the PR body reflects current evidence rather than an older green run;
- no live-trading certification claim is made.
