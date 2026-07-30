# First Production-Paper Release Checklist

This checklist tracks the first production-like supervised paper release. **Live trading remains disabled.**

## Release scope

Included:

- strict runtime validation;
- transactional PostgreSQL operator state;
- row-level product and opportunity tables;
- versioned economic state and actual-cost reconciliation;
- lease-backed runtime jobs;
- local-first LM Studio/llama.cpp intelligence routing;
- OpenRouter disabled by default;
- health, readiness, metrics, and structured request logs;
- RBAC tokens, CSRF, CORS, and security headers;
- adapter certification gates;
- deterministic replay backtests;
- audit hash-chain verification;
- canonical Docker Compose deployment and logical backups.

Excluded:

- live order submission;
- live settlement redemption;
- automatic broker certification;
- remote LLM execution by default;
- immutable external/WORM audit storage;
- point-in-time WAL archive automation;
- unsupervised production strategy promotion.

## Must pass before deployment

```bash
pnpm test
pnpm build
pnpm api:validate
pnpm migrations:validate
pnpm migrations:dry-run
pnpm security:validate
pnpm deploy:validate
pnpm first-prod:validate
```

Validate a real production environment file separately:

```bash
set -a
. /secure/path/portfolio.env
set +a
pnpm runtime:validate
```

Optional but recommended:

```bash
RUN_POSTGRES_INTEGRATION=true DATABASE_URL=<database-url> pnpm test:integration:postgres
```

## Database gates

- Migration 001 creates core operator state.
- Migration 002 creates product-layer row tables.
- Migration 003 adds audit hashes and adapter certifications.
- Migration 004 creates opportunity, research, budget, market snapshot, and cost tables.
- Migration 005 creates the lease-backed runtime job queue.
- Migrations are checksum-protected and serialized by a PostgreSQL advisory lock.
- `TransactionalPostgresOperatorStore` uses one checked-out client, `SERIALIZABLE` isolation, and an advisory transaction lock.
- `/ready/production-paper` rejects PostgreSQL without migration 005.
- PostgreSQL is not exposed on a host port in the canonical Compose stack.

## Runtime gates

Strict production-paper deployment must fail if:

- PostgreSQL or its migration set is unavailable;
- operator authentication is disabled;
- CSRF is disabled;
- the CORS allowlist is missing;
- live trading flags are enabled;
- local inference is required but no valid local node is configured;
- remote inference is enabled without an explicit provider key;
- a configured local node has malformed or duplicate identity, URL, context, or concurrency fields.

## Local intelligence gates

- Every configured node exposes OpenAI-compatible `GET /v1/models` and `POST /v1/chat/completions`.
- Model IDs in deployment configuration exactly match `/v1/models` output.
- Heavy nodes begin at `maxConcurrent=1` until measured benchmarks justify more.
- Routing verifies health, model availability, context capacity, queue capacity, and estimated cost.
- Model provider I/O occurs outside the PostgreSQL operator-state transaction.
- Local usage reconciliation records runtime, queue delay, token counts, throughput when available, power assumptions, and actual computed cost.
- Prompts and full responses are not persisted in the economic ledger.
- Node/model drift requires a new quote.

## Economic gates

- A pre-call decision may authorize intelligence purchase but never trade execution.
- Actual measured/provider-reported cost supersedes the estimate.
- Reconciliation invalidates older execution decisions based on estimates.
- A post-reconciliation economic decision is required before paid-agent execution.
- Stale forecasts, venue previews, model quotes, or unreconciled usage fail closed.
- Missing counterfactual evidence remains visibly pending rather than being fabricated.

## Worker and scheduler gates

- `runtime_jobs` uses idempotency keys.
- Claiming uses `FOR UPDATE SKIP LOCKED`.
- Running jobs have an owner and expiring lease.
- Workers heartbeat long jobs, recover expired leases, apply bounded retries, and dead-letter terminal failures.
- Multiple economic worker instances cannot execute the same interval job.
- SIGTERM stops new claims, allows a bounded grace period, and closes the database pool.

## Audit gates

- Audit events support `previousHash`, `eventHash`, and `sequenceNumber`.
- Chain verification detects tampering, predecessor mismatches, and sequence gaps.
- Audit chain fields are uniquely indexed when present.
- Remaining after this release: export audit roots to an external immutable sink.

## Adapter and execution gates

- Paper adapters require `certified_paper` or `certified_live` status.
- Live adapters require `certified_live` and `liveEnabled=true`.
- Fail-closed adapters reject live submission.
- Every execution preserves opportunity, model quote, economic decision, forecast, and execution-cost lineage.
- The current synthetic/paper execution engine must not be represented as certified live execution.

## Backtest gates

- Replay validates OHLCV input.
- Invalid moving-average parameters are rejected.
- Identical input produces deterministic output.
- Replay assumptions include fees and slippage.
- Remaining after this release: historical bot counterfactual replay for automatic paid-agent attribution.

## Deployment gates

- `docker-compose.production.yml` is the canonical runtime.
- The API and worker wait for successful migrations.
- PostgreSQL uses an internal-only network.
- API and worker receive controlled inference-network egress.
- Application root filesystems are read-only and Linux capabilities are dropped.
- Required secrets have no committed defaults.
- `LIVE_TRADING=false`, `COINBASE_DRY_RUN=true`, and `REMOTE_LLM_EXECUTION_ENABLED=false` remain fixed in the production-paper stack.
- `pg_dump` backups and a tested `pg_restore` procedure are documented and exercised.
- A rollback retains the prior image, migration compatibility, and database backup.

## Manual smoke test

1. Create a host-managed production environment file outside source control.
2. Confirm each local inference endpoint and exact loaded model ID.
3. Run `pnpm build` and the complete test suite.
4. Start `docker-compose.production.yml`.
5. Confirm migrations 001–005 are applied.
6. Call `/health`, `/ready/production-paper`, `/metrics`, and `/metrics.prom`.
7. Call `/api/economics/intelligence/nodes` and verify expected fleet health.
8. Run one economic-maintenance cycle.
9. Create a local model quote for a bounded test request.
10. Approve the intelligence-purchase decision.
11. Execute the model request and verify actual local cost reconciliation.
12. Create a new post-reconciliation decision.
13. Confirm stale or incomplete evidence blocks paper execution.
14. Run a deterministic replay/backtest.
15. Verify execution/economic lineage and audit integrity.
16. Trigger the paper kill switch.
17. Confirm every live execution route remains blocked.
18. Create and restore a logical database backup in a disposable environment.
