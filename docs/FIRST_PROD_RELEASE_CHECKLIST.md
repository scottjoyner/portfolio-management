# First Production-Paper Release Checklist

This checklist governs the first supervised production-like paper deployment. **Live trading remains disabled and uncertified.** The authoritative evidence matrix is [`RELEASE_READINESS_MATRIX.md`](./RELEASE_READINESS_MATRIX.md); deployment and rollback commands are in [`DEPLOYMENT_ROLLBACK_RUNBOOK.md`](./DEPLOYMENT_ROLLBACK_RUNBOOK.md).

## Release scope

Included:

- strict runtime validation;
- transactional PostgreSQL operator state;
- row-level product and opportunity tables;
- versioned economic state and actual-cost reconciliation;
- lease-backed runtime jobs and worker heartbeat health;
- stale model-call recovery;
- local-first LM Studio/llama.cpp intelligence routing;
- optional, explicitly gated OpenRouter execution;
- persisted UI routing policy and hard caps;
- at-most-once remote provider attempts with uncertain outcomes quarantined;
- delayed generation metadata reconciliation for known OpenRouter generations;
- normalized optimistic execution records;
- append-only execution events, durable orders, and durable fills;
- compatibility synchronization and restart hydration for the paper execution engine;
- health, readiness, metrics, structured request logs, and authenticated browser sessions;
- RBAC tokens, CSRF, CORS, and security headers;
- adapter certification gates;
- deterministic replay backtests;
- audit hash-chain verification;
- canonical Docker Compose deployment, logical backups, and rollback procedures;
- runner-normalized release-critical performance thresholds.

Excluded:

- live order submission;
- live settlement redemption;
- automatic broker certification;
- remote LLM execution by default;
- immutable external/WORM audit storage;
- point-in-time WAL archive automation;
- unsupervised production strategy promotion.

## Exact-head CI gate

Do not proceed unless the PR's exact head commit has a successful blocking `release-readiness` job. That aggregate requires:

- `validation`;
- all four deterministic Node shards;
- `postgres-integration`;
- `python-critical`;
- `broad-python-suite`;
- `coverage-gate`;
- `performance-gate`.

The maintained broad Python surface is blocking, not advisory. It runs `tests/` and `trading_system/tests/unit/` with the generated coverage directory omitted from that specific job.

The performance gate is also blocking. It uses `config/release-performance-thresholds.json`, requires the GitHub Node 22 Linux x64 runner profile, performs one warmup and five measured samples, and enforces median latency, p95 latency, and minimum median throughput for:

- the 10,000-bar moving-average replay path;
- construction and verification of a 5,000-event audit chain;
- normalization of the 500-record operator-state workload.

Thresholds may not be raised merely to clear CI. A threshold change requires exact-runner before/after evidence, a workload-preserving rationale, and updated baseline metadata.

Download and retain the exact-head artifacts:

- `postgres-readiness-<sha>`;
- `focused-coverage-<sha>`;
- `performance-smoke-<sha>`;
- maintained and generated full-inventory artifacts when the exhaustive workflow is run.

Artifact names may use the temporary PR merge SHA, but each workflow artifact record must identify the reviewed branch head SHA.

## Must pass before host deployment

Use the committed npm lockfile and canonical scripts:

```bash
npm ci --ignore-scripts
npm test
npm run build
npm run operational:validate
npm run api:validate
npm run migrations:validate
npm run migrations:dry-run
npm run security:validate
npm run deploy:validate
npm run first-prod:validate
PERFORMANCE_STRICT_RUNNER=true \
  PERFORMANCE_THRESHOLD_CONFIG=config/release-performance-thresholds.json \
  node scripts/benchmark-release-critical-paths.mjs
```

Validate the intended host-managed environment separately:

```bash
set -a
. /secure/path/portfolio.env
set +a
npm run runtime:validate
```

The real PostgreSQL integration command is blocking:

```bash
DATABASE_URL=<database-url> npm run test:integration:postgres
```

The maintained Python release surface is blocking:

```bash
PYTHONPATH="$PWD:$PWD/trading_system" python -m pytest \
  tests/ trading_system/tests/unit/ \
  --ignore=tests/coverage \
  --import-mode=importlib \
  -q
```

## Database and execution gates

- Migrations 001–006 apply and a second application is idempotent.
- Migration checksums are verified under a PostgreSQL advisory lock.
- Migration 006 creates normalized execution records, events, orders, and fills.
- Execution events are append-only and protected by a trigger rejecting update and delete.
- `TransactionalPostgresOperatorStore` uses one checked-out client, `SERIALIZABLE` isolation, and serialized queries on that client.
- `/ready/production-paper` rejects PostgreSQL without migrations 005 and 006.
- A fresh store and execution engine must hydrate normalized execution state after restart.
- Runtime jobs must prove enqueue, claim, heartbeat, completion, retry, and lease recovery against real PostgreSQL.
- Every execution has a unique intent idempotency key.
- Lifecycle updates require an expected version and lock the current row with `FOR UPDATE`.
- Duplicate transitions return the original append-only event.
- Invalid transitions and stale versions fail closed.
- Orders and fills have independent idempotency keys.
- Every execution preserves opportunity, model quote, economic decision, forecast, and execution-cost lineage.
- Unsupported compatibility statuses are skipped or recorded as divergence.
- Invalid fills are skipped and orphan order references are removed without fabricating a parent order.
- A newer in-process update is not overwritten by an older published snapshot.
- Coinbase dry-run preview, create, close, cancel, and bracket behavior remains local, deterministic, and subprocess-free.
- Paper scale-in fees are computed from the effective configured fee rate and agree across event, position, and portfolio accounting.
- The paper execution engine must not be represented as certified live execution.

## Runtime and browser gates

Strict production-paper deployment must fail if:

- PostgreSQL or its migration set is unavailable;
- operator authentication or CSRF is disabled;
- the CORS allowlist is missing;
- live trading flags are enabled;
- local inference is required but no valid local node is configured;
- remote inference is enabled without an explicit provider key;
- a configured local node has malformed or duplicate identity, URL, context, or concurrency fields.

The canonical deployment is local-first and remote-off. `REMOTE_LLM_EXECUTION_ENABLED=false` is the required default; changing it to true is a deliberate, separately reviewed host action and still does not authorize live trading.

The production runtime smoke must prove:

- shipped browser assets load;
- unauthenticated policy access is rejected;
- an authenticated, CSRF-protected policy update succeeds;
- routing policy survives API restart;
- the configured local fleet remains healthy after restart;
- `/ready/production-paper` succeeds against real PostgreSQL.

Browser operator and CSRF tokens remain only in same-tab `sessionStorage`; closing the tab clears them. The browser must never expose or persist the OpenRouter key.

## Intelligence and economic gates

- Every configured local node exposes OpenAI-compatible `/v1/models` and `/v1/chat/completions`.
- Configured model IDs exactly match provider output.
- Heavy nodes begin at `maxConcurrent=1` until measured evidence justifies more.
- Routing verifies health, model availability, context, queue capacity, and estimated cost.
- Provider I/O occurs outside the PostgreSQL operator-state transaction.
- Prompts and full responses are not persisted in the economic ledger.
- Node/model drift requires a new quote.
- Research jobs default to local and queued.
- Automatic remote comparison falls back to local or stops safely.
- Remote execution requires both `REMOTE_LLM_EXECUTION_ENABLED=true` with a host-managed key and an eligible persisted UI policy.
- Per-request, daily, and value-coverage limits fail closed.
- A known OpenRouter generation with incomplete usage enters `usage_pending` and reconciles through generation metadata without another billable POST.
- A transport-uncertain attempt without a generation ID enters manual reconciliation and cannot retry automatically.
- Actual measured/provider cost supersedes estimates and invalidates stale execution decisions.
- Missing counterfactual evidence remains visibly pending rather than being fabricated.

## Worker and scheduler gates

- `runtime_jobs` uses idempotency keys and `FOR UPDATE SKIP LOCKED` claiming.
- Running jobs have an owner and expiring lease.
- Workers heartbeat, recover expired leases, apply bounded retries, and dead-letter terminal failures.
- Multiple worker instances cannot execute the same interval job.
- External provider and market requests complete before serializable mutations begin.
- A running model call older than `MODEL_CALL_STALE_SECONDS` fails closed and requires the documented recovery path.
- The worker writes an atomic process heartbeat; stale or failed runs make the Compose service unhealthy.
- SIGTERM stops new claims, allows a bounded grace period, and closes the pool.

## Audit, backup, and rollback gates

- Audit events include `previousHash`, `eventHash`, and `sequenceNumber`.
- Chain verification detects tampering, predecessor mismatch, and sequence gaps.
- Execution lifecycle evidence is independently append-only.
- CI and the deployment host exercise `pg_dump` and `pg_restore` into a clean database.
- A pre-deploy backup exists outside the PostgreSQL data volume.
- The prior image digest and tested restore target are recorded before deployment.
- `docker compose down -v` is prohibited as rollback.
- Remaining after this release until separately closed: export audit roots and backups to an external immutable or WORM-capable destination.

## Required manual host certification

1. Name the release operator, reviewer, rollback owner, and incident owner.
2. Confirm the exact source SHA matches the reviewed PR head.
3. Record current and candidate image digests.
4. Validate the host-managed environment and rendered Compose model.
5. Confirm each local inference endpoint and loaded model ID.
6. Create a pre-deploy logical backup and restore it into a disposable database.
7. Deploy PostgreSQL, migrations, API, and worker.
8. Confirm API and worker health.
9. Run `npm run smoke:production-paper`.
10. Authenticate the browser and inspect Today, Risk & System, and Intelligence Routing.
11. Save the intended routing policy and hard caps.
12. Create a bounded local research job and verify actual local-cost reconciliation.
13. When remote is deliberately enabled, execute one capped paper-only request and verify provider-reported cost and post-reconciliation invalidation.
14. Create a paper execution and verify normalized execution, order, fill, and append-only event records.
15. Restart the API and verify execution and policy persistence.
16. Seed a stale running model quote and verify recovery under `MODEL_CALL_STALE_SECONDS`.
17. Trigger the paper kill switch and confirm no new paper submission proceeds.
18. Confirm every live execution and settlement route remains blocked.
19. Rehearse the prior image against the restored backup target.
20. Record final go/no-go and evidence locations.

## Open engineering blockers

The PR remains draft until these are closed or safely removed from release scope:

- remaining broad whole-state compatibility rewrites replaced by targeted optimistic PostgreSQL mutations;
- deterministic bot replay for automatic paid-agent counterfactual attribution;
- off-host backup retention and external immutable/WORM audit anchoring.
