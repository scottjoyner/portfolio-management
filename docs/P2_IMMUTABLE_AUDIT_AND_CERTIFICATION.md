# P2 Immutable Audit and Production-Paper Certification

This slice adds runtime audit verification, production-paper readiness, deploy smoke testing, and a consolidated production-paper certification gate on top of the first-production certification baseline already on `main`.

Live trading remains blocked.

## Canonical audit chain

The canonical audit implementation is:

```text
packages/storage/src/auditChain.mjs
```

It provides:

- `buildAuditEvent(event, previous)`
- `hashAuditEvent(event)`
- `verifyAuditChain(events)`

Audit events use:

- `previousHash`
- `eventHash`
- `sequenceNumber`

Database support is provided by:

```text
packages/storage/src/migrations/003_audit_and_certification.sql
```

## Runtime audit verification

API endpoint:

```text
GET /api/audit/verify
```

CLI:

```bash
pnpm audit:verify
```

The endpoint returns HTTP 200 when the chain is valid and HTTP 409 when hashes are missing or invalid.

## Production-paper readiness

Added a paper-production readiness endpoint that remains separate from live-production `/ready`:

```text
GET /ready/production-paper
```

It checks:

- strict runtime validation
- SQL durable storage
- Postgres migration readiness
- audit-chain integrity
- live trading remains false

It returns HTTP 200 only when production-paper requirements pass. It returns HTTP 503 with blockers otherwise.

## Production-paper smoke test

Added:

```bash
pnpm smoke:production-paper
```

Script:

```text
scripts/smoke-production-paper.mjs
```

It checks:

- `/health`
- `/ready`
- `/ready/production-paper`
- `/api/operator/summary`
- `/metrics`
- `/metrics.prom`
- `/api/audit/verify`

By default, the smoke test allows `/ready` and `/ready/production-paper` to return 503 because live trading remains uncertified and local/dev storage may not be production-paper ready.

## Production-paper certification gate

Added:

```bash
pnpm certify:production-paper
```

Script:

```text
scripts/certify-production-paper.mjs
```

The gate aggregates:

- migration validation
- API contract validation
- security validation
- deployment validation
- first-production release validation
- strict runtime validation
- live-trading prohibition

It emits a JSON report with:

```json
{
  "ok": true,
  "certification": "production-paper",
  "liveTradingCertified": false
}
```

## Required environment for certification

Run with production-like env values injected by your deployment system:

```text
DEPLOYMENT_ENV=production
STRICT_RUNTIME_VALIDATION=true
OPERATOR_STORE=postgres
DATABASE_URL=<managed database URL>
OPERATOR_AUTH_REQUIRED=true
OPERATOR_ADMIN_TOKEN=<secret>
CSRF_REQUIRED=true
OPERATOR_CSRF_TOKEN=<secret>
CORS_ORIGINS=<operator origin allowlist>
LIVE_TRADING=false
ALLOW_POLYMARKET_ORDER_SUBMISSION=false
ALLOW_LIVE_SETTLEMENT_REDEMPTION=false
```

Do not commit these values.

## Tests

Current tests added by this slice:

```text
tests/audit-integrity-api.test.mjs
```

Existing first-production tests cover the canonical audit-chain implementation.

## Remaining gaps

This is not full live production certification. Still outstanding:

- immutable append-only audit storage rather than full table rewrite paths
- external/WORM audit sink
- historical market data replay backtesting
- dedicated secret scanner such as Gitleaks
- adapter-specific contract tests for every broker/venue
- separate live trading certification package
