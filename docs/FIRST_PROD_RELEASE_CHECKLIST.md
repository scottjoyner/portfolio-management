# First Production Release Checklist

This checklist tracks the first production-like paper-only release. Live trading remains disabled.

## Release scope

This release is production-deployable for supervised paper operation only.

Included:

- strict runtime validation
- Postgres-backed state
- row-level product-layer tables
- health checks and metrics
- structured request logs
- deployment manifests
- RBAC tokens
- CSRF/CORS/security headers
- adapter certification gates
- replay backtest scaffold
- audit hash-chain scaffolding

Excluded:

- live order submission
- live settlement redemption
- real broker/venue production connectors
- immutable external audit-log storage
- full historical data vendor integration

## Must pass before deployment

```bash
pnpm test
pnpm build
pnpm api:validate
pnpm migrations:validate
pnpm security:validate
pnpm runtime:validate
pnpm deploy:validate
```

Optional but recommended:

```bash
RUN_POSTGRES_INTEGRATION=true DATABASE_URL=<database-url> pnpm test:integration:postgres
```

## Database gates

- Migration 001 exists for core operator state.
- Migration 002 exists for product-layer row tables.
- Migration 003 exists for audit hash fields and adapter certifications.
- `adapter_certifications` table exists.
- `audit_events` includes `previous_hash`, `event_hash`, and `sequence_number`.

## Runtime gates

Strict production-like deployment must fail if:

- Postgres is not configured.
- operator auth is not required.
- CSRF is not required.
- CORS allowlist is missing.
- live trading flags are enabled.

## Audit gates

- New audit inserts through `OperatorRowRepository.insertAudit()` are hash-chained.
- Chain verification detects tampering.
- Audit chain fields are indexed.

Remaining after first release:

- Write audit chain verification endpoint/job.
- Export audit roots to external immutable storage.

## Adapter gates

- Adapters require certification records.
- Paper execution requires `certified_paper` or `certified_live` status.
- Live execution requires `certified_live` and `liveEnabled=true`.
- Fail-closed adapter still rejects live submission.

## Backtest gates

- Replay engine validates OHLCV data.
- Replay engine rejects invalid moving-average parameters.
- Replay output is deterministic for identical inputs.
- Replay assumptions include fees and slippage.

Remaining after first release:

- Add real market data adapters.
- Add walk-forward validation and scenario stress tests.

## Deployment gates

- Docker image builds.
- Compose production template keeps live flags disabled.
- Kubernetes template keeps live flags disabled and includes probes.
- Deployment manifest validator passes.
- Backup and restore procedure is documented.
- Rollback procedure is documented.

## Manual smoke test

1. Start production-like paper environment.
2. Apply migrations.
3. Validate runtime config.
4. Call `/health`.
5. Call `/metrics`.
6. Create strategy from template.
7. Run replay or deterministic backtest.
8. Request approval.
9. Approve with admin token.
10. Start paper execution.
11. Submit paper signal.
12. Verify account cash/NAV, position, fill, reconciliation, and audit log.
13. Stop paper execution.
14. Trigger kill switch.
15. Confirm live execution route remains blocked.
