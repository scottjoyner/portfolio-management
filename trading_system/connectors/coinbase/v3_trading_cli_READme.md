# Coinbase v3 Trading CLI

## Overview

This CLI provides balance checking and trading functionality for Coinbase using:
- Organization-based keys from `cdp_api_key.json`
- JWT/ES256 authentication for v3 API
- Mock mode for testing without real API calls

## Quick Start

### Check BTC Balance (Mock Mode)
```bash
cd /home/falcon/git/portfolio-management/trading_system/connectors/coinbase
python3 v3_trading_cli.py balance -m
```

**Sample Output:**
```
Loaded key name: organizations/534df359-3e4e-4383-83a6-2625030ec715/apiKeys/0012c0ce-befc-4a9d-a32f-c512ca98e6f9
{
  "success": true,
  "data": {
    "BTC": 0.0245,
    "ETH": 1.23,
    "USD": 1847.56
  },
  "wallet_id": null,
  "mock": true
}
```

### View Available Trading Pairs (Markets)
```bash
python3 v3_trading_cli.py markets -m
```

**Sample Output:**
```
Loaded key name: organizations/534df359-3e4e-4383-83a6-2625030ec715/apiKeys/0012c0ce-befc-4a9d-a32f-c512ca98e6f9
- Bitcoin USD (BTC-USD)
- Etheer USD (ETH-USD)
```

## Full Usage Examples

### Balance Checking
```bash
# Check all balances
python3 v3_trading_cli.py balance -m

# Check specific wallet
python3 v3_trading_cli.py balance -w wallet_test_123 -m
```

### Trading Operations (Mock Mode)
```bash
# Place a buy order (bid) on BTC-USD at price 9248.50 for 0.1 BTC
python3 v3_trading_cli.py place-buy -p BTC-USD -a 0.1 -pr 9248.50 -m

# Place a sell order (ask) on ETH-USD
python3 v3_trading_cli.py place-sell -p ETH-USD -a 0.5 -pr 9247.00 -m
```

## Key Configuration Files

### API Keys File
Your Coinbase v3 API keys are stored at:
```bash
mnt/c/Users/AMD/Downloads/cdp_api_key.json
```

**File Contents:**
```json
{
  "name": "organizations/534df359-3e4e-4383-83a6-2625030ec715/apiKeys/0012c0ce-befc-4a9d-a32f-c512ca98e6f9",
  "privateKey": "[REDACTED PRIVATE KEY]"
}
```

**Extracted Information:**
- **Organization ID**: `534df359-3e4e-4383-83a6-2625030ec715`
- **API Key ID**: `0012c0ce-befc-4a9d-a32f-c512ca98e6f9`

## Real API vs Mock Mode

### Mock Mode (Default)
Use `-m` or `--mock-mode` flag to use mock responses. This is useful for:
- Testing your workflow without making real trade calls
- Development and debugging
- Demonstrating functionality safely

### Production Mode
Remove the `-m` flag to connect to the real Coinbase API (requires proper JWT token generation):
```bash
python3 v3_trading_cli.py balance  # No mock mode - uses real API
```

## Requirements

Install required Python packages:
```bash
pip install requests jwt cryptography
```

## Notes

- The CLI currently runs in **mock mode** by default (safe for testing)
- Mock responses provide realistic-looking data without making actual trade calls
- To use real API calls, you'll need to implement JWT token generation using the `jwt` and `cryptography` libraries with your organization-based keys