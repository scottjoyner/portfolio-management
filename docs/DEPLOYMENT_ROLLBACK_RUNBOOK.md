# Production-Paper Deployment and Rollback Runbook

This runbook governs the first supervised production-paper deployment of Portfolio OS. It does **not** authorize live order submission or settlement redemption.

## Operating roles

At minimum, identify these roles before deployment:

- **Release operator:** executes commands and records evidence.
- **Release reviewer:** confirms the commit, environment, smoke results, and rollback checkpoint.
- **Incident owner:** has authority to stop paper execution, disable remote inference, or roll back.

One person may fill multiple roles in a private deployment, but the release record must still name who made each decision.

## Required release evidence

Do not deploy unless all of the following are available:

1. Exact PR head commit SHA.
2. Green blocking `release-readiness` GitHub Actions job.
3. `postgres-readiness-<sha>` artifact containing:
   - first migration report;
   - idempotent second migration report;
   - PostgreSQL integration TAP output;
   - authenticated production runtime smoke report;
   - logical restore evidence;
   - disposable test dump.
4. Host-managed environment file outside source control.
5. Current image identifier or digest for rollback.
6. A fresh pre-deploy logical database backup.
7. A written rollback decision point and incident owner.

Diagnostic legacy/performance jobs may remain non-blocking only when their failures are documented as inherited and unrelated to the release path.

## Stop conditions before deployment

Stop immediately when any of these is true:

- the PR is not at the reviewed commit;
- a blocking CI job is missing, cancelled, or not successful;
- the production environment file fails `npm run runtime:validate`;
- migrations do not dry-run cleanly;
- a local node identity or model ID differs from `/v1/models`;
- remote inference is enabled without an OpenRouter key;
- no rollback image or pre-deploy backup exists;
- the current paper kill switch state is unknown;
- live-trading or live-settlement flags are enabled;
- the operator cannot authenticate to the browser/API with CSRF protection enabled.

## Pre-deployment procedure

Set reusable shell variables:

```bash
export COMPOSE_FILE=docker-compose.production.yml
export ENV_FILE=/secure/path/portfolio.env
export RELEASE_SHA=<reviewed-commit-sha>
```

Confirm the source revision:

```bash
git rev-parse HEAD
test "$(git rev-parse HEAD)" = "$RELEASE_SHA"
git status --short
```

Validate source and environment:

```bash
npm ci --ignore-scripts
npm test
npm run build
set -a
. "$ENV_FILE"
set +a
npm run runtime:validate
```

Render the Compose model before touching running services:

```bash
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" config > /tmp/portfolio-compose-rendered.yml
```

Confirm the rendered file keeps these values:

```text
LIVE_TRADING=false
LIVE_TRADING_ENABLED=false
COINBASE_DRY_RUN=true
ALLOW_POLYMARKET_ORDER_SUBMISSION=false
ALLOW_LIVE_SETTLEMENT_REDEMPTION=false
```

Record current images and container state:

```bash
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" images
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" ps
```

Create a pre-deploy logical backup. The backup must be stored outside the database data volume:

```bash
mkdir -p /secure/backups/portfolio
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T postgres \
  pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom --no-owner \
  > "/secure/backups/portfolio/predeploy-${RELEASE_SHA}-${stamp}.dump"
test -s "/secure/backups/portfolio/predeploy-${RELEASE_SHA}-${stamp}.dump"
```

## Deployment procedure

Build without starting application services:

```bash
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" build --pull
```

Start PostgreSQL and apply migrations:

```bash
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d postgres
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" run --rm migrate
```

Run the migration command a second time. It must report every migration as skipped and must not change a checksum:

```bash
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" run --rm migrate
```

Start API and worker:

```bash
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d api economic-worker
```

Start logical backups when the profile is part of the deployment:

```bash
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" --profile backup up -d postgres-backup
```

## Post-deployment verification

Wait for container health, then record status:

```bash
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" ps
docker inspect --format '{{json .State.Health}}' portfolio-production-api-1
docker inspect --format '{{json .State.Health}}' portfolio-production-economic-worker-1
```

Run the production-paper smoke test from the deployed source tree:

```bash
export PORTFOLIO_BASE_URL="http://127.0.0.1:${API_PORT:-3000}"
export OPERATOR_ADMIN_TOKEN
npm run smoke:production-paper | tee "/secure/backups/portfolio/smoke-${RELEASE_SHA}-${stamp}.json"
```

Open the browser console and establish a same-tab operator session. The browser stores the bearer token and CSRF token only in `sessionStorage`; closing the tab clears them. Confirm:

- the Today page renders;
- the operator session indicator is authenticated;
- Risk & System shows PostgreSQL and fresh evidence;
- the worker container is healthy;
- the intelligence-routing card shows the intended local/OpenRouter capability state;
- live execution remains disabled;
- no unexplained critical item appears in the attention queue.

Restart the API once during the release window and verify state hydration:

```bash
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" restart api
npm run smoke:production-paper
```

Confirm a previously persisted execution and the intelligence-routing policy remain visible after restart.

## Intelligence routing procedure

Remote inference has two independent controls:

1. **Deployment gate:** `REMOTE_LLM_EXECUTION_ENABLED=true` and a host-managed `OPENROUTER_API_KEY` must be present in the API and worker containers.
2. **Operator policy:** the UI must be set to `openrouter_allowed` or `economic_auto`.

The UI cannot reveal the key or override the deployment gate.

Available modes:

- **Local fleet only:** all remote calls are rejected.
- **Economic auto-selection:** remote calls require value evidence, per-request and daily caps, and the configured minimum value-coverage multiple. A blocked remote comparison falls back to local when enabled.
- **OpenRouter eligible:** an explicit remote request may proceed within hard caps, but the normal intelligence-purchase and post-reconciliation trade gates still apply.

Before enabling remote inference:

- verify OpenRouter account/credit status outside Portfolio OS;
- set a small daily cap and per-request cap;
- confirm local fallback is healthy;
- confirm `usage_pending` is zero or understood;
- run one bounded paper-only request;
- verify provider-reported actual cost appears in the ledger;
- verify the pre-call decision is superseded and a new post-reconciliation trade decision is required.

Disable remote inference immediately when provider billing cannot be reconciled, the key may be exposed, rate limits cause repeated retries, or actual cost exceeds the configured cap.

## Incident response

### API unhealthy or readiness failing

1. Stop new operator actions.
2. Trigger the paper kill switch when the UI/API is available.
3. Inspect API logs and `/ready/production-paper` blockers.
4. Verify PostgreSQL health and migrations.
5. Restart only the API once.
6. Roll back when readiness does not recover or state hydration differs from PostgreSQL.

### Economic worker unhealthy

1. Deny new paid-research approvals.
2. Inspect the worker health JSON and container logs.
3. Check `runtime_jobs` for expired leases, retries, and terminal failures.
4. Confirm the database is reachable and no external provider call is being held inside a transaction.
5. Restart the worker once; lease recovery must prevent duplicate interval execution.
6. Roll back when the heartbeat remains stale or jobs duplicate.

### Local fleet unavailable

1. Keep or switch the UI policy to `local_only` unless the remote deployment gate and budget were deliberately approved.
2. Verify node DNS, `/v1/models`, exact model IDs, context capacity, and concurrency.
3. Do not silently route to a different model.
4. Pause research when neither an approved remote route nor a healthy local route exists.

### OpenRouter unavailable or spend cap exhausted

1. Switch the UI policy to `local_only` or leave `economic_auto` with local fallback enabled.
2. Do not raise caps to clear an incident.
3. Review remote committed cost, actual cost, and unresolved `usage_pending` rows.
4. Retry only after provider status and account credits are independently confirmed.

### `usage_pending` does not reconcile

1. Block new paid execution decisions that depend on the unresolved quote.
2. Preserve provider generation/request identifiers.
3. Retry reconciliation idempotently; never create a replacement cost row for the same provider call.
4. Escalate to rollback or disable remote inference when actual cost cannot be established within the release policy window.

### Duplicate execution suspected

1. Trigger the paper kill switch.
2. Inspect normalized execution, order, fill, and append-only event records.
3. Compare intent, transition, order, and fill idempotency keys.
4. Do not retry until the venue and database agree on whether an order filled.

### Backup service unhealthy

1. Stop deployment progression.
2. Create a manual `pg_dump` outside the data volume.
3. Restore it into a disposable database and verify migration 006 and execution counts.
4. Resume only after restore evidence is recorded.

## Rollback decision triggers

Rollback rather than continue debugging in place when any of these occurs:

- PostgreSQL migration or checksum mismatch;
- API restart loses or rewinds durable execution state;
- append-only execution events can be modified;
- duplicate job or provider execution is observed;
- worker heartbeat remains unhealthy after one controlled restart;
- authentication or CSRF can be bypassed;
- paper kill switch does not stop new paper submissions;
- actual remote cost exceeds a hard cap;
- the deployed image cannot pass `smoke:production-paper`;
- a backup cannot be restored.

## Application rollback without database restore

Use this only when migrations are additive and the previous image is known to read the current schema.

```bash
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" stop api economic-worker
# Restore the previously recorded image tag/digest in the deployment configuration.
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d api economic-worker
npm run smoke:production-paper
```

Do not run `docker compose down -v`.

## Database restore rollback

Use a new database or volume; do not overwrite the only copy of the failed deployment.

1. Stop API, worker, and backup writer.
2. Preserve a failure-time dump and logs.
3. Create a clean PostgreSQL database/volume.
4. Restore the selected pre-deploy dump.
5. Point a rollback environment file at the restored database.
6. Start the prior application image.
7. Run readiness, smoke, audit, execution-count, and routing-policy checks.
8. Retain the failed database until incident review is complete.

Example restore into a disposable database:

```bash
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T postgres \
  createdb -U "$POSTGRES_USER" portfolio_rollback
cat /secure/backups/portfolio/predeploy-<sha>-<stamp>.dump | \
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T postgres \
  pg_restore -U "$POSTGRES_USER" -d portfolio_rollback --exit-on-error --no-owner
```

## Release record

Record:

- release SHA and image digest;
- start/end timestamps;
- release operator, reviewer, and incident owner;
- CI artifact name;
- migration output;
- backup path and restore result;
- smoke report path;
- selected routing policy and hard caps;
- known non-blocking diagnostics;
- rollback image and backup;
- final go/no-go decision.
