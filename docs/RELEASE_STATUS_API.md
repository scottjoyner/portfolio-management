# Release Status and Audit Verification API

This document covers read-only endpoints for first-production paper-only certification posture.

Live trading remains disabled.

## GET /api/release/status

Returns first-prod release posture, blockers, runtime state, audit-chain state, and capability flags.

Expected key fields:

```json
{
  "status": {
    "ok": true,
    "release": "first-prod-paper-only",
    "liveTradingCertified": false,
    "liveBlocked": true,
    "strictRuntime": false,
    "runtimeOk": true,
    "killSwitchEnabled": false,
    "runningPaperExecutions": 0,
    "audit": {
      "ok": true,
      "mode": "no_chained_events_yet"
    },
    "blockers": [],
    "capabilities": {
      "paperTrading": true,
      "replayBacktesting": true,
      "adapterCertificationGates": true,
      "auditHashChain": true,
      "liveOrderSubmission": false
    }
  }
}
```

A deployment is not first-prod healthy if `blockers` is non-empty.

Example blockers:

- `live_trading_flag_enabled`
- `audit_chain_invalid`
- `runtime_invalid`

## GET /api/audit/verify

Verifies hash-chained audit events available in current state.

Expected clean result when no hash-chained events exist yet:

```json
{
  "audit": {
    "ok": true,
    "count": 0,
    "lastHash": null,
    "issues": [],
    "mode": "no_chained_events_yet"
  }
}
```

Expected failure if tampering is detected:

```json
{
  "audit": {
    "ok": false,
    "issues": [
      { "id": "audit2", "issue": "event_hash_mismatch" }
    ],
    "mode": "hash_chain"
  }
}
```

## Operational usage

Run these after deployment and after any paper execution workflow:

```bash
curl /api/release/status
curl /api/audit/verify
```

For production-like mode, include the configured operator bearer token.

## Notes

- These endpoints are read-only.
- They do not enable live execution.
- They are intended for release certification and operator dashboards.
- External immutable audit-root export remains a future hardening item.
