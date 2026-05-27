# Coinbase Read-Only Sync Harness — Deployment Guide

## Overview

The Coinbase read-only sync harness provides safe, credential-gated read operations against the Coinbase brokerage API for validating account/portfolio state before any live execution.

**Status**: ✅ Production-ready for staging validation

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                Coinbase Read-Only Sync           │
│               (Production Staging Harness)       │
├─────────────────────────────────────────────────┤
│                                                 │
│  GET /exchange/health                           │
│    → Returns integration status                 │
│                                                 │
│  GET /exchange/accounts                         │
│    → Lists brokerage accounts                   │
│                                                 │
│  GET /exchange/portfolios                       │
│    → Lists portfolios                           │
│                                                 │
│  GET /exchange/products                         │
│    → Lists trading products                     │
│                                                 │
│  GET /exchange/credentials/validate             │
│    → Validates API credentials                  │
│                                                 │
└─────────────────────────────────────────────────┘
            ↑           ↑           ↑
    All endpoints are READ-ONLY (no execution)
```

---

## Safety Gates

### 1. Credential Gating (Critical)

All live data access requires Coinbase API credentials in `.env`:

```bash
COINBASE_API_KEY=***          # Required for /exchange/* endpoints
COINBASE_API_SECRET=***        # Required for /exchange/* endpoints
LIVE_TRADING_ENABLED=false     # ⚠️ Critical: blocks all execution paths
```

**Behavior without credentials**:
- Returns safe default values (empty lists, degraded status)
- No errors raised
- Graceful degradation ensures system continues operating

### 2. Execution Path Isolation

Zero order placement capabilities exposed in read-only sync endpoints:
- **NO** order creation via `/exchange/` namespace
- **NO** cancel orders functionality
- **NO** batch operations
- All execution paths are separate, guarded by `LIVE_TRADING_ENABLED`

### 3. Rate Limiting Considerations

Coinbase API rate limits (v3 endpoints):
- Accounts: ~60 requests/hour
- Products: ~60 requests/hour  
- Orders: ~480 requests/hour

**Recommendation**: Implement exponential backoff in error handlers, add request throttling middleware if needed.

---

## Deployment Procedures

### Option A: Deploy with Existing Credentials (Staging/Prod)

1. Obtain Coinbase API credentials from Developer Dashboard
2. Create read-only scopes (`Accounts:R`, `Orders:R`)
3. Add to `.env`:
   ```bash
   COINBASE_API_KEY=<your_a...e>
   COINBASE_API_SECRET=<your_s...t>
   LIVE_TRADING_ENABLED=false  # ⚠️ Do not enable until explicitly approved
   ```

4. Redeploy application:
   ```bash
   cd trading_system
   pip install -e .
   # or docker build & push if using container deployment
   ```

### Option B: Deploy for Testing/Development (No Credentials)

1. Leave credentials empty in `.env`:
   ```bash
   COINBASE_API_KEY=""
   COINBASE_API_SECRET=""
   LIVE_TRADING_ENABLED=false  # Must be false
   ```

2. Application will respond with safe defaults:
   ```json
   {
     "status": "ok",
     "coinbase_configured": false,
     "accounts": [],
     "timestamp": "..."
   }
   ```

---

## API Usage Examples

### Test Integration Status (No Credentials)

```bash
curl -s http://localhost:8001/exchange/health | jq .
# Output:
# {
#   "status": "ok",
#   "coinbase_configured": false,
#   "live_enabled": false,
#   "timestamp": "2026-05-26T..."
# }
```

### List Accounts (With Credentials)

```bash
curl -s http://localhost:8001/exchange/accounts | jq .
# Output with credentials:
# {
#   "status": "ok",
#   "accounts": [
#     {"id": "acc_***", "name": "BTC-Wallet", "type": "wallet", "currency": "BTC"},
#     {"id": "acc_***", "name": "ETH-Trading", "type": "trading", "currency": "ETH"}
#   ],
#   "timestamp": "2026-05-26T..."
# }
```

### Validate Credentials (Safety Check)

```bash
curl -s http://localhost:8001/exchange/credentials/validate | jq .
# Returns valid=true if credentials work with Coinbase API
```

---

## Rollback Procedures

### Immediate Rollback (Live Trading Enabled Accidentally Set)

If `LIVE_TRADING_ENABLED` is somehow set to `true`:

1. **Immediately** change back to false:
   ```bash
   sed -i 's/LIVE_TRADING_ENABLED=true/LIVE_TRADING_ENABLED=false/' .env
   # or
   echo "LIVE_TRADING_ENABLED=false" >> .env
   ```

2. Restart application:
   ```bash
   # If using uvicorn in background
   pkill -f uvicorn
   python3 -m uvicorn apps.api.main:app --host 0.0.0.0 --port 8001 &
   ```

### Credential Rotation (Compromise Suspected)

```bash
# 1. Revoke existing keys in Coinbase Developer Dashboard
# 2. Generate new read-only scoped keys
# 3. Update .env with new credentials
# 4. Redeploy application
```

---

## Monitoring & Alerts

### Health Check Endpoints

Add to deployment monitoring:
- `GET /exchange/health` — Integration status
- `GET /exchange/credentials/validate` — Credential validity
- Standard `/health` — Application health

### Recommended Alert Conditions

| Condition | Severity | Action |
|-----------|----------|--------|
| `/exchange/health` returns error | Critical | Check application logs, redeploy |
| Credentials valid but no accounts returned | Warning | Verify Coinbase API connection |
| `/exchange/credentials/validate` fails | High | Rotate credentials immediately |

### Log Monitoring

```bash
# Watch for read-only sync errors
tail -f trading_system/logs/app.log 2>&1 | grep -E "coinbase.*error"
```

---

## Testing Checklist

### Local Development Tests

```bash
cd trading_system
make dev  # Start both API and worker
# Then test endpoints:
curl http://localhost:8001/exchange/health
curl http://localhost:8001/exchange/accounts
```

### CI Integration Tests

New E2E tests automatically run on PR push (`.github/workflows/ci.yml`):
- `tests/e2e/test_coinbase_sync.py` — Endpoint response validation
- Runs in addition to existing unit/integration test suite

### Manual Staging Validation

```bash
# With staging credentials:
curl -s http://localhost:8001/exchange/health | grep "ok"
curl -s http://localhost:8001/exchange/accounts | jq '.accounts[].id'

# Verify no execution paths exposed:
# (No order placement endpoints in /exchange/* namespace)
```

---

## Security Considerations

### Key Storage Best Practices

❌ **DO NOT** commit `.env` to version control (already excluded)

✅ Store credentials in separate secret store (Vault, AWS Secrets Manager) for production

✅ Use environment-specific configs:
   ```bash
   .env.development  # For local testing, no real credentials
   .env.staging      # Real credentials, read-only scope only
   .env.production   # Read-only + separate execution approval flow
   ```

### API Scope Restrictions

Create Coinbase API scopes with **READ-ONLY ONLY**:
- `Accounts:R` (read accounts)
- `Orders:R` (read orders — for reconciliation)
- ❌ Do NOT include `Orders:W`, `Transfers:W`, `Auth:Admin`

---

## Next Steps After Deployment

### 1. Wire WebSocket Market Feed (Optional)

For live price updates to worker consumption:
```python
# In apps/api/ws_routes.py or new ws_sync.py
@router.websocket("/ws/market/{product_id}")
async def ws_market_feed(websocket, product_id):
    # Subscribe to Coinbase WebSocket for real-time prices
    # Publish to hub for worker processing
```

### 2. Add Reconciliation Service (Recommended)

Compare local state vs Coinbase actuals:
```python
# Create new service: exchange/reconciliation/service.py
class ExchangeStateReconciler:
    async def reconcile(self, portfolio_id: str):
        # Fetch from Coinbase
        # Compare with stored state
        # Log discrepancies if any
```

### 3. Set Up Shadow Mode (Advanced Safety)

Run parallel orders in shadow mode first:
1. Execute same logic but don't place real orders
2. Compare predicted fills vs actual
3. Gradually increase shadow ratio to full live

---

## Troubleshooting

### "status": "credentials_missing" in Response

**Cause**: No API keys configured or expired

**Fix**: 
```bash
# Check .env has valid credentials
grep COINBASE_API_KEY .env
grep COINBASE_API_SECRET .env

# Revoke & regenerate if needed in Coinbase Developer Dashboard
```

### 503 Service Unavailable on Endpoints

**Cause**: HTTP client connection errors

**Check**:
```bash
# Verify network connectivity to Coinbase API
curl -v https://api.coinbase.com/api/v3/brokerage/accounts

# Check application logs for connection pool exhaustion
tail -f trading_system/logs/app.log | grep "connection"
```

### Timeouts on Large Product Lists

**Cause**: Rate limiting or slow responses

**Mitigation**: Add request throttling middleware in `ops_layer.py`

---

## Support Contacts

- Developer Dashboard: https://www.coinbase.com/dashboard/account
- API Documentation: https://docs.cloud.coinbase.com/coinbase/reference
- Internal Security Team (for credential rotation)

---

## Appendix: Endpoint Specifications

| Endpoint | Method | Path | Status Code | Response Example |
|----------|--------|------|-------------|------------------|
| Health check | GET | `/exchange/health` | 200 | `{"status":"ok","coinbase_configured":false,"live_enabled":false}` |
| List accounts | GET | `/exchange/accounts` | 200 | `{"status":"ok","accounts":[],"timestamp":"..."}` |
| List portfolios | GET | `/exchange/portfolios` | 200 | `{"status":"ok","portfolios":[],"timestamp":"..."}` |
| List products | GET | `/exchange/products?limit=N` | 200 | `{"status":"ok","products":[...],"timestamp":"..."}` |
| Validate credentials | GET | `/exchange/credentials/validate` | 200 | `{"valid":true,"reason":"...","timestamp":"..."}` |

**All responses include ISO-8601 timestamp for auditability.**

---

**Document Version**: 1.0  
**Last Updated**: May 2026  
**Status**: ✅ Production-ready
