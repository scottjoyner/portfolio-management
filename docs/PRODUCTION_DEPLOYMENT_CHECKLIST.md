# Production Deployment Checklist

This checklist prepares the operator product for a production-like paper-only deployment.

Live trading remains blocked. Do not set live execution flags to true.

## 1. Pre-deployment gates

Run locally or in CI:

```bash
pnpm install
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm api:validate
pnpm migrations:validate
pnpm security:validate
pnpm deploy:validate
```

Optional Postgres smoke:

```bash
docker compose up -d postgres
RUN_POSTGRES_INTEGRATION=true DATABASE_URL=<database-url> pnpm test:integration:postgres
```

## 2. Required runtime configuration

Production-like deployment must use strict runtime validation:

```text
NODE_ENV=production
DEPLOYMENT_ENV=production
STRICT_RUNTIME_VALIDATION=true
OPERATOR_STORE=postgres
OPERATOR_AUTH_REQUIRED=true
CSRF_REQUIRED=true
PAPER_TRADING=true
LIVE_TRADING=false
ALLOW_POLYMARKET_ORDER_SUBMISSION=false
ALLOW_LIVE_SETTLEMENT_REDEMPTION=false
```

Required secret-backed values:

```text
DATABASE_URL
OPERATOR_ADMIN_TOKEN
OPERATOR_CSRF_TOKEN
CORS_ORIGINS
```

Recommended optional role tokens:

```text
OPERATOR_PAPER_TOKEN
OPERATOR_READONLY_TOKEN
```

Validate runtime configuration before starting service:

```bash
STRICT_RUNTIME_VALIDATION=true DEPLOYMENT_ENV=production pnpm runtime:validate
```

## 3. Database migration

Before starting or updating the API service:

```bash
DATABASE_URL=<database-url> pnpm migrations:up
```

Confirm migration validation:

```bash
pnpm migrations:validate
```

Docker Compose production deployment includes a one-shot `migrate` service and the API waits for `service_completed_successfully` before starting. Kubernetes deployment includes a `portfolio-management-migrate` Job; run and verify that Job before rolling out the API Deployment.

## 4. Backup and restore policy

Before deployment, create a database backup with your platform backup utility or PostgreSQL custom-format dump.

Recommended properties:

- custom-format backup
- timestamped artifact
- checksum
- encrypted storage
- retention policy
- restore test before first production-like rollout

Minimum backup command shape:

```bash
pg_dump <database-url> --format=custom --file=<backup-file>
```

Minimum restore command shape:

```bash
pg_restore --clean --if-exists --dbname=<database-url> <backup-file>
```

Do not commit backup files or database URLs.

## 5. Deployment options

### Docker Compose

Use:

```bash
docker compose -f deploy/compose.production.yml up -d --build
```

Store real secrets outside git and inject them with your deployment system.

### Kubernetes

Use `deploy/kubernetes.yaml` as a starting template.

Required secret name:

```text
portfolio-management-secrets
```

The secret should provide database/auth/CSRF/CORS values.

## 6. Health checks and observability

After deploy:

```bash
curl /health
curl /ready
curl /api/operator/summary
curl /metrics
curl /metrics.prom
```

Expected:

- `/health` returns 200 when service and storage are reachable.
- `/ready` may remain 503 because live trading is intentionally uncertified.
- `/api/operator/summary` should include redacted runtime config and storage status.
- `/metrics` remains the JSON metrics endpoint for existing API consumers.
- `/metrics.prom` exposes Prometheus-style process/request metrics.
- Structured request logs are emitted as JSON lines on stdout.

## 7. Smoke workflow

Run paper-only workflow:

1. List accounts.
2. List instruments.
3. Create strategy from template.
4. Run backtest.
5. Request approval.
6. Approve with admin token.
7. Start paper execution.
8. Send paper signal.
9. Verify paper fill, position, account NAV/cash update, and reconciliation.
10. Stop paper execution.
11. Verify audit trail.
12. Enable/disable kill switch.

## 8. Rollback

Rollback should include both app and data steps:

1. Stop traffic to new app version.
2. Revert image to previous known-good version.
3. Confirm migrations are backward-compatible before app rollback.
4. If data rollback is required, restore the pre-deploy backup into a controlled environment first.
5. Run smoke tests against restored environment before promoting traffic.

## 9. Live trading prohibition

The following must remain false until a separate certification release:

```text
LIVE_TRADING=false
ALLOW_POLYMARKET_ORDER_SUBMISSION=false
ALLOW_LIVE_SETTLEMENT_REDEMPTION=false
```

Any deployment that sets these to true must fail strict runtime validation.

## 10. Outstanding before true production/live operations

- Replace broader full-state core save path with row-level repositories.
- Add historical market data replay backtesting.
- Add dedicated secret scanning such as Gitleaks.
- Add external observability stack integration.
- Add immutable audit log storage.
- Add real adapter contract tests for each broker/venue.
- Complete live trading certification package.
