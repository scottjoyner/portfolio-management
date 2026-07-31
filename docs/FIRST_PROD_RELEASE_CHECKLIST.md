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
- a persisted UI policy for local-only, economic-auto, and OpenRouter-eligible routing;
- at-most-once remote provider attempts with uncertain outcomes quarantined;
- delayed generation-metadata reconciliation for known OpenRouter generations;
- normalized optimistic execution records;
- append-only execution events, durable orders, and durable fills;
- compatibility synchronization and restart hydration for the paper execution engine;
- health, readiness, metrics, structured request logs, and authenticated browser sessions;
- RBAC tokens, CSRF, CORS, and security headers;
- adapter certification gates;
- deterministic replay backtests;
- audit hash-chain verification;
- canonical Docker Compose deployment, logical backups, and rollback procedures.

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
- `coverage-gate`.

The `legacy-python-diagnostic` and `performance-diagnostic` jobs remain diagnostic. A failure that affects the production-paper path must be promoted to blocking rather than dismissed as legacy.

Download and retain the exact-head `postgres-readiness-<sha>` artifact. It must contain:

- the first migration report;
- the idempotent second migration report;
- PostgreSQL integration TAP output;
- authenticated production runtime and browser restart smoke results;
- logical backup/restore evidence;
- the disposable test dump.

## Must pass before host deployment

Use the committed npm lockfile and canonical npm scripts:

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
```

Validate the intended host-managed environment separately:

```bash
set -a
. /secure/path/portfolio.env
set +a
npm run runtime:validate
```

The real PostgreSQL integration command is blocking, not optional:

```bash
DATABASE_URL=<database-url> npm run test:integration:postgres
```

## Database gates

- Migration 001 creates core operator state.
- Migration 002 creates product-layer row tables.
- Migration 003 adds audit hashes and adapter certifications.
- Migration 004 creates opportunity, research, budget, market snapshot, and cost tables.
- Migration 005 creates the lease-backed runtime job queue.
- Migration 006 creates normalized execution records, events, orders, and fills.
- Execution events are append-only and protected by a PostgreSQL trigger that rejects update or delete operations.
- Migrations are checksum-protected and serialized by a PostgreSQL advisory lock.
- Applying migrations a second time must be idempotent and preserve every checksum.
- `TransactionalPostgresOperatorStore` uses one checked-out client, `SERIALIZABLE` isolation, an advisory transaction lock, and serialized queries on the pinned client.
- `/ready/production-paper` rejects PostgreSQL without migrations 005 and 006.
- A real PostgreSQL test must persist an execution, close the store, open a fresh store, publish the durable read model, and hydrate a fresh execution engine.
- The integration test must enqueue, claim, heartbeat, and complete a real `runtime_jobs` row.
- PostgreSQL is not exposed on a host port in the canonical Compose stack.

## Runtime and browser gates

Strict production-paper deployment must fail if:

- PostgreSQL or its migration set is unavailable;
- operator authentication is disabled;
- CSRF is disabled;
- the CORS allowlist is missing;
- live trading flags are enabled;
- local inference is required but no valid local node is configured;
- remote inference is enabled without an explicit provider key;
- a configured local node has malformed or duplicate identity, URL, context, or concurrency fields.

The production runtime smoke must prove:

- the shipped HTML loads `/ui/operator-session.js` and `/ui/intelligence-policy.js`;
- an unauthenticated policy request is rejected;
- an authenticated, CSRF-protected policy update succeeds;
- the routing policy survives an API process restart;
- the configured local fleet remains healthy after restart;
- `/ready/production-paper` succeeds against real PostgreSQL.

The browser's operator and CSRF tokens remain only in same-tab `sessionStorage`; closing the tab clears them. The browser must never expose or persist the OpenRouter key.

## Intelligence routing gates

- Every configured local node exposes OpenAI-compatible `GET /v1/models` and `POST /v1/chat/completions`.
- Model IDs in deployment configuration exactly match `/v1/models` output.
- Heavy nodes begin at `maxConcurrent=1` until measured benchmarks justify more.
- Routing verifies health, model availability, context capacity, queue capacity, and estimated cost.
- Provider I/O occurs outside the PostgreSQL operator-state transaction.
- Local usage reconciliation records runtime, queue delay, token counts, throughput when available, power assumptions, and actual computed cost.
- Prompts and full responses are not persisted in the economic ledger.
- Node/model drift requires a new quote.
- Research jobs default to `local` and `queued`; a selected remote quote must carry its locality and provider lineage into the research job.
- Automatic remote comparison falls back to local when remote is unavailable or blocked and fallback is enabled.

OpenRouter has two independent controls:

1. Deployment must set `REMOTE_LLM_EXECUTION_ENABLED=true` and supply a host-managed `OPENROUTER_API_KEY`.
2. The UI policy must be `economic_auto` or `openrouter_allowed`.

`REMOTE_LLM_EXECUTION_ENABLED=false` remains the canonical default. Remote execution is configurable, not fixed on. Every remote request remains subject to a per-request cap, daily cap, value-coverage policy when automatic, and the existing intelligence-purchase gate.

Remote provider attempts are fail-closed and at-most-once under uncertainty:

- a response-level HTTP failure that proves generation did not begin may restore the quote for a controlled retry;
- a known generation ID with incomplete usage enters `usage_pending` and is reconciled through generation metadata without issuing another POST;
- a transport-uncertain attempt without a generation ID enters manual reconciliation and cannot be retried automatically;
- metadata reconciliation uses bounded exponential backoff and becomes explicit manual review after exhaustion;
- every second execution attempt against a pending or consumed quote is rejected.

## Economic gates

- A pre-call decision may authorize intelligence purchase but never trade execution.
- Actual measured/provider-reported cost supersedes the estimate.
- Reconciliation invalidates older execution decisions based on estimates.
- A post-reconciliation economic decision is required before paid-agent execution.
- Stale forecasts, venue previews, model quotes, or unreconciled usage fail closed.
- Missing counterfactual evidence remains visibly pending rather than being fabricated.
- OpenRouter spend-cap exhaustion must fall back to local or stop; operators must not raise caps merely to clear an incident.

## Worker and scheduler gates

- `runtime_jobs` uses idempotency keys.
- Claiming uses `FOR UPDATE SKIP LOCKED`.
- Running jobs have an owner and expiring lease.
- Workers heartbeat long jobs, recover expired leases, apply bounded retries, and dead-letter terminal failures.
- Multiple economic worker instances cannot execute the same interval job.
- The maintenance lease also runs stale model-call recovery and due OpenRouter usage reconciliation.
- External generation metadata and market/provider requests complete before the serializable mutation begins.
- A `running` model call older than `MODEL_CALL_STALE_SECONDS` fails closed.
- Recovered local calls require a new node quote before retry; `usage_pending` records remain available for later billing reconciliation.
- The worker writes an atomic process heartbeat; a stale heartbeat or failed run must make the Compose container unhealthy.
- SIGTERM stops new claims, allows a bounded grace period, and closes the database pool.

## Execution gates

- Every execution has a unique intent idempotency key.
- Lifecycle updates require an expected version and lock the current row with `FOR UPDATE`.
- Duplicate transitions return the original append-only event rather than applying twice.
- Invalid transitions and stale versions fail closed.
- Orders and fills have independent idempotency keys.
- Every execution preserves opportunity, model quote, economic decision, forecast, and execution-cost lineage.
- Compatibility state is mirrored into normalized tables inside the same PostgreSQL transaction.
- Unsupported compatibility statuses are skipped or recorded as divergence; durable state is not rewritten to an illegal status.
- Invalid fills are skipped and orphan order references are removed without fabricating a parent order.
- The API process republishes persisted execution state and the compatibility engine hydrates after restart.
- A newer in-process update is not overwritten by an older published snapshot.
- The current paper execution engine must not be represented as certified live execution.

## Audit and adapter gates

- Audit events support `previousHash`, `eventHash`, and `sequenceNumber`.
- Chain verification detects tampering, predecessor mismatches, and sequence gaps.
- Audit chain fields are uniquely indexed when present.
- Execution lifecycle evidence is independently append-only in `execution_events`.
- Paper adapters require `certified_paper` or `certified_live` status.
- Live adapters require `certified_live` and `liveEnabled=true`.
- Fail-closed adapters reject live submission.
- Remaining after this release: export audit roots to an external immutable sink.

## Deployment, backup, and rollback gates

- `docker-compose.production.yml` is the canonical runtime.
- The production image installs Node dependencies from `package-lock.json` using `npm ci`.
- The API and worker wait for successful migrations.
- PostgreSQL uses an internal-only network.
- API and worker receive controlled inference-network egress.
- Application root filesystems are read-only and Linux capabilities are dropped.
- Required secrets have no committed defaults.
- `LIVE_TRADING=false`, `LIVE_TRADING_ENABLED=false`, `COINBASE_DRY_RUN=true`, `ALLOW_POLYMARKET_ORDER_SUBMISSION=false`, and `ALLOW_LIVE_SETTLEMENT_REDEMPTION=false` remain fixed.
- Worker lease, retry, shutdown, stale-call, reconciliation, and heartbeat thresholds are explicitly passed into containers.
- CI and the deployment host must exercise `pg_dump` and `pg_restore` into a clean database.
- A pre-deploy backup must exist outside the PostgreSQL data volume.
- The prior image/digest and a tested restore target must be recorded before go-live.
- Do not use `docker compose down -v` as rollback.

## Required manual host certification

1. Name the release operator, reviewer, and incident owner.
2. Confirm the exact source SHA matches the reviewed PR head.
3. Record the current and candidate image identifiers/digests.
4. Validate the host-managed environment and rendered Compose model.
5. Confirm each local inference endpoint and exact loaded model ID.
6. Create a pre-deploy logical backup and restore it into a disposable database.
7. Deploy PostgreSQL, migrations, API, and worker.
8. Confirm both API and worker container health.
9. Run `npm run smoke:production-paper` against the deployed endpoint.
10. Authenticate the browser session and inspect Today, Risk & System, and Intelligence Routing.
11. Save the intended routing policy and hard caps.
12. Create a bounded local research job and verify actual local cost reconciliation.
13. When remote is deliberately enabled, execute one capped paper-only request and verify provider-reported actual cost and post-reconciliation decision invalidation.
14. Create a paper execution and verify normalized execution, order, fill, and append-only event records.
15. Restart the API and verify execution and routing-policy persistence.
16. Seed a stale running model quote and verify maintenance recovery.
17. Trigger the paper kill switch and confirm no new paper submission proceeds.
18. Confirm every live execution and settlement route remains blocked.
19. Rehearse the prior image against the restored backup target.
20. Record the final go/no-go decision and evidence paths.

## Open engineering blockers

The PR remains draft until these are closed or safely removed from release scope:

- remaining broad whole-state compatibility rewrites replaced by targeted optimistic PostgreSQL mutations;
- deterministic bot replay for automatic paid-agent counterfactual attribution.
