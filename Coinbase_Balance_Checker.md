# Coinbase Balance Checker - Tool & Documentation

## Overview

This tool checks your Coinbase API credentials and retrieves your account balance information. It validates whether your current API key is working, or helps you obtain new credentials from the Coinbase Developer Dashboard.

**Status**: ✅ Ready for use

---

## Quick Start

### Check Balance with Current Credentials

```bash
cd /home/falcon/git/portfolio-management

# Run the balance checker script
python3 scripts/coinbase_balance_checker.py

# Expected output:
# === Coinbase Balance Checker ===
# Status: CREDENTIALS_VALIDATED
# 
# Account 1:
#   ID: acc_7k2...9xm
#   Name: BTC Wallet
#   Currency: BTC
#   Available Balance: 0.05432 BTC ($2,156.78)
#   USD Value: $2,156.78
# 
# Account 2:
#   ID: acc_3n9...1kp
#   Name: ETH Wallet  
#   Currency: ETH
#   Available Balance: 2.456 ETH ($4,523.12)
#   USD Value: $4,523.12
# 
# Total Portfolio Value: $6,680.90
```

---

## How to Get New Coinbase API Credentials

Your current API key appears to be invalid (expired or revoked). Follow these steps:

### Step 1: Go to Coinbase Developer Dashboard

1. Visit: **https://dashboard.pro.coinbase.com/api/settings**
2. Log in with your Coinbase account credentials
3. Click "Create API Key" or find existing keys
4. Give the key a descriptive name (e.g., "Portfolio Management Read-Only")
5. Set appropriate permissions:
   - ✅ `Accounts:read` (for balance checking)
   - ✅ `Orders:read` (if needed for order history)
   - ❌ **DO NOT** enable `Transfer:wallet` or any write permissions
6. Copy both the API Key and API Secret

### Step 2: Add Passphrase (Optional but Recommended)

For enhanced security, you can set a withdrawal passphrase:
- Go to Settings → Advanced settings
- Enable "Withdrawal passphrase" for API keys
- This adds an extra layer of protection
- Optional since we're only using read-only access

### Step 3: Update .env File

```bash
# Edit your .env file
vim .env  # or nano .env

# Add or update these lines:
COINBASE_API_KEY=your_new_api_key_here
COINBASE_API_SECRET=your_new_api_secret_here
COINBASE_PASSPHRASE=your_passphrase_or_empty_if_not_set

# IMPORTANT: Keep this set to false for read-only access!
LIVE_TRADING_ENABLED=false
```

### Step 4: Run the Balance Checker

```bash
python3 scripts/coinbase_balance_checker.py
```

---

## Understanding Coinbase Account Types

Your account will likely have multiple accounts. Here's what you'll see:

| Account Type | Description | Typical Balance |
|--------------|-------------|-----------------|
| **BTC Wallet** | Bitcoin storage | BTC holdings (or 0) |
| **ETH Wallet** | Ethereum storage | ETH holdings (or 0) |
| **Trading** | Futures/advanced trading | Mixed crypto holdings |
| **USD Wallet** | Fiat USD balance | $0.00 to various amounts |

The balance checker will aggregate all accounts and provide:
- Individual account balances
- Total portfolio value in USD
- Per-currency breakdown

---

## Error Handling & Troubleshooting

### "CREDENTIALS_INVALID" Error

**Cause**: API key expired, revoked, or incorrect permissions

**Fix**:
1. Visit https://dashboard.pro.coinbase.com/api/settings
2. Check if your keys are still active
3. Create new read-only keys if needed
4. Update `.env` file with new credentials
5. Re-run the balance checker

### "RATE_LIMITED" Error

Coinbase API enforces ~12 requests/second for REST v3 endpoints. The tool implements automatic backoff to handle this.

**Fix**: Wait 10-30 seconds and retry, or create a new API key with higher rate limits if needed.

### "ACCOUNT_NOT_FOUND" Error

This is normal for read-only sync mode. The tool gracefully handles missing accounts.

---

## Tool Features

✅ **Credential Validation** - Confirms keys work before querying balance  
✅ **Multi-Account Support** - Lists all Coinbase brokerage accounts  
✅ **Currency Aggregation** - Shows USD values for easy portfolio overview  
✅ **Passphrase Support** - Handles accounts with withdrawal passphrase enabled  
✅ **Automatic Retry** - Implements exponential backoff on rate limits  
✅ **Read-Only Safety** - Never makes write requests (LIVE_TRADING_ENABLED=false)  

---

## API Endpoints Used

The balance checker uses these read-only Coinbase v3 API endpoints:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v3/brokerage/accounts` | POST | List all brokerage accounts |
| `/api/v3/exchange-rates` | GET | Fetch real-time exchange rates (for USD conversion) |

**Rate Limits** (v3 endpoints):
- Accounts: ~60 requests/hour
- Exchange rates: ~60 requests/hour  

---

## Example Output

```
================================================================================
           COINBASE ACCOUNT BALANCE CHECKER
================================================================================

Credential Validation: ✅ PASSED
API Key Active: Yes
Rate Limit Status: OK (8/60 this hour)

=== Account Summary ===

Account 1 of 3:
  ID: acc_7k2m9n4p1q8r5t2w
  Name: BTC-Wallet
  Type: wallet
  Currency: BTC
  Available Balance: 0.05432 BTC
  USD Value: $2,156.78
  
Account 2 of 3:
  ID: acc_3n9x7y2k1j4h8g5f
  Name: ETH-Trading
  Type: trading
  Currency: ETH
  Available Balance: 2.456 ETH
  USD Value: $4,523.12
  
Account 3 of 3:
  ID: acc_9p2q3r4s5t6u7v8w
  Name: USD-Wallet
  Type: wallet
  Currency: USD
  Available Balance: 1250.50 USD
  USD Value: $1,250.50

================================================================================
Total Portfolio Value: $7,930.40
================================================================================
Timestamp: 2026-06-01T14:23:45Z
================================================================================
```

---

## Production Deployment

For production environments (deployed services):

### Environment Variables

```bash
# In your deployment .env file
COINBASE_API_KEY=<from_coinbase_dashboard>
COINBASE_API_SECRET=<from_coinbase_dashboard>
LIVE_TRADING_ENABLED=false  # Must be false for read-only
```

### Service Endpoint

The read-only sync harness exposes:

```bash
curl http://localhost:8001/exchange/health
# Returns integration status

curl http://localhost:8001/exchange/accounts
# Returns list of accounts with balances
```

---

## Security Best Practices

❌ **NEVER** commit `.env` to version control (already in `.gitignore`)

✅ Use separate environments:
   - `.env.development` — For local testing, temporary keys
   - `.env.staging` — Read-only keys for staging
   - `.env.production` — Production read-only keys

✅ Rotate API keys every 30-60 days for enhanced security

✅ Monitor API usage in Coinbase Dashboard → Settings → API access

✅ Set up alerts for unusual API key usage patterns

---

## Alternative: Use Testnet/Paper Trading

For development without real funds:

1. Coinbase offers a testnet/sandbox environment
2. Or use Alpaca paper trading (already configured in your system)
3. The balance checker will gracefully handle missing credentials

To disable coinbase checks temporarily:

```bash
# Comment out credentials in .env
COINBASE_API_KEY=""
COINBASE_API_SECRET=""

# Run with mock mode
python3 scripts/coinbase_balance_checker.py --mock
```

---

## Related Documentation

- **Deployment Guide**: `COINBASE_READ_ONLY_SYNC_DEPLOYMENT.md`
- **Integration Report**: `EXCHANGE_CONNECTORS_INTEGRATION_REPORT.md`
- **API Settings**: https://dashboard.pro.coinbase.com/api/settings
- **API Documentation**: https://docs.cloud.coinbase.com/coinbase/reference

---

**Document Version**: 1.0  
**Last Updated**: June 2026  
**Status**: ✅ Ready for credential validation