# First Production Certification Hardening

This slice adds release-certification controls for a production-like paper-only deployment.

Live trading remains disabled.

## Audit hash chain

Added migration 003 fields on `audit_events`:

- `previous_hash`
- `event_hash`
- `sequence_number`

Added helpers:

- `hashAuditEvent()`
- `buildAuditEvent()`
- `verifyAuditChain()`

`OperatorRowRepository.insertAudit()` now appends sequence/hash fields when inserting audit events through the row repository.

## Adapter certification gates

Added `adapter_certifications` table with:

- adapter name/kind
- status
- live-enabled flag
- reviewer
- evidence JSON
- expiration metadata

Adapter contracts now enforce:

- paper actions require `certified_paper` or `certified_live`
- live actions require `certified_live` and `liveEnabled=true`
- revoked/blocked/expired/mismatched certifications fail closed
- fail-closed adapter still rejects live submission

## Replay backtesting certification tests

Replay engine coverage now validates:

- OHLCV normalization
- duplicate timestamp rejection
- invalid OHLC rejection
- invalid moving-average period rejection
- deterministic output for identical input
- explicit fee/slippage assumptions

## Release gate

Added:

```bash
pnpm first-prod:validate
```

The validator checks for:

- audit/certification migration
- audit hash helpers
- adapter certification gates
- replay engine artifacts
- first production release checklist

The gate is wired into:

- `pnpm build`
- GitHub Actions CI

## Remaining after this slice

- Add live audit chain verification endpoint/job.
- Export audit root hashes to external immutable storage.
- Add real adapter-specific contract suites before introducing real connectors.
- Add production historical data provider integration.
- Add walk-forward validation and stress-test suites.
