# Coinbase Rest Client Submodule - Production Read-Only Brokerage API

**Status**: ✅ P1 Production-ready (June 2026)

## Overview

This submodule provides production-grade read-only access to your Coinbase Advanced Trade brokerage account via OAuth 2.0 authentication.

### Features

- Real-time balance fetching from Coinbase exchange API
- Account information and transaction history access
- Rate-limit aware with exponential backoff retry logic  
- Graceful fallback to mock data for development environments
- Comprehensive error handling with typed exceptions
- Health status endpoints for monitoring

### Quick Start

#### Using .env (Recommended)

```python
from trading_system.connectors.coinbase.rest.client import create_advanced_rest_client_from_env

# Load credentials from /home/falcon/git/portfolio-management/.env
client = create_advanced_rest_client_from_env()

# List accounts
import asyncio
async def main():
    accounts = await client.list_accounts()
    
    for acc in accounts:
        currency = acc['currency'].upper()
        balance = acc['available']
        
        if currency == 'BTC':
            print(f"💰 {acc['name']}: {balance:.8f} BTC")
        elif currency == 'ETH':
            print(f"🔷 {acc['name']}: {balance:.4f} ETH")  
        elif currency == 'USD':
            print(f"💵 {acc['name']}: ${balance:,.2f}")

asyncio.run(main())
```

#### Using Explicit Credentials

```python
from trading_system.connectors.coinbase.rest.client import CoinbaseAdvancedRestClient

client = CoinbaseAdvancedRestClient(
    api_key=os.getenv('COINBASE_API_KEY'),
    api_secret=os.getenv('COINBASE_API_SECRET'),  
    passphrase=os.getenv('COINBASE_PASSPHRASE', ''),
)

accounts = await client.list_accounts()
```

### Integration Pattern

```python
import os

# Try real API first, fallback to mock if no credentials
if os.getenv('COINBASE_API_KEY'):
    from trading_system.connectors.coinbase.real_client import CoinbaseAdvancedRestClient
    client = CoinbaseAdvancedRestClient(
        api_key=os.getenv('COINBASE_API_KEY'),
        api_secret=os.getenv('COINBASE_API_SECRET'),
        passphrase=os.getenv('COINBASE_PASSPHRASE', ''),
    )
else:
    from trading_system.connectors.coinbase.mock_client import create_default_client
    client = create_default_client()  # Uses mock data

# Use with your portfolio system
accounts = await client.list_accounts()
```

### Error Handling

```python
from trading_system.connectors.coinbase.real_client import (
    CoinbaseAdvancedRestClientError,
    AuthenticationError,
    RateLimitError,
)

try:
    accounts = await client.list_accounts()
except AuthenticationError as e:
    print(f"❌ Auth failed: {e}")
    # Fallback to mock or exit gracefully
except RateLimitError as e:
    print(f"⚠️  Rate limit hit - implementing retry...")
    # Implement exponential backoff retry logic
```

## Directory Structure

```
trading_system/connectors/coinbase/rest/
├── __init__.py          # Submodule exports
└── client.py            # Main implementation (13KB)
```

## Files

- `rest/client.py` - CoinbaseAdvancedRestClient production implementation
- `real_client.py` - Standalone read-only brokerage API client (legacy/main connector)
- `coinbase.py` - Advanced trading API with OAuth signing, WebSocket support

## Related Documentation

- [Coinbase Mock Client](../MOCK_CLIENT_README.md)
- [Coinbase Balance Checker](../COinbase_Balance_Checker.md)
- [Read-Only Sync Deployment Guide](../COINBASE_READ_ONLY_SYNC_DEPLOYMENT.md)
- [Exchange Connectors Integration Report](../EXCHANGE_CONNECTORS_INTEGRATION_REPORT.md)

## Status

| Component | Status | Notes |
|-----------|--------|-------|
| Production rest client submodule | ✅ Complete | OAuth 2.0, rate-limit aware |
| Mock fallback mechanism | ✅ Complete | Seamless dev/prod switching |
| Error handling | ✅ Complete | Typed exceptions for all cases |
| Documentation | ✅ Updated | All broken paths fixed |

## License

Part of Portfolio Management project at `/home/falcon/git/portfolio-management`
