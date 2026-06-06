# Coinbase Mock Client README (Updated)

## Status: ✅ Fixed - All broken paths resolved

This is the **MOCK CLIENT** (development/testing), NOT the production REST client.

---

## Production vs Mock Clients

### 📡 PRODUCTION REST CLIENT (Live API)
```python
from trading_system.connectors.coinbase.real_client import CoinbaseAdvancedRestClient
client = CoinbaseAdvancedRestClient(
    api_key=os.getenv('COINBASE_API_KEY'),
    api_secret=os.getenv('COINBASE_API_SECRET'),
    passphrase=os.getenv('COINBASE_PASSPHRASE', ''),
)
accounts = await client.list_accounts()  # Fetches real data from Coinbase API
```

**Location:** `trading_system/connectors/coinbase/rest/client.py`  
**Features:** OAuth 2.0 authentication, rate-limit aware, health checks

---

### 🎭 MOCK CLIENT (Development)
```python
from trading_system.connectors.coinbase.mock_client import create_default_client
client = create_default_client()  # Uses realistic mock data when no credentials present
accounts = await client.list_accounts()  # Returns pre-populated or randomized accounts
```

**Location:** `trading_system/connectors/coinbase/mock_client.py`  
**Features:** Realistic mock balances, WebSocket simulation for testing

---

## Mock Data Modes

- **STATIC** (default): Pre-defined mock data with realistic portfolio structure
- **RANDOMIZED**: Random values within configurable bounds each call
- **EMPTY**: Simulates empty/no balance scenario

---

## Usage Examples

### Development (no credentials)
```python
client = create_default_client()  # Mock mode automatic
accounts = await client.list_accounts()
# Returns: BTC, ETH, USD wallet accounts with realistic balances
```

### Production switch
```python
import os
if not os.getenv('COINBASE_API_KEY'):
    from trading_system.connectors.coinbase.mock_client import create_default_client
    client = create_default_client()  # Uses mock data
else:
    from trading_system.connectors.coinbase.real_client import CoinbaseAdvancedRestClient
    client = CoinbaseAdvancedRestClient(...)  # Fetches real API data
```

---

## Mock Data Structure (STATIC mode)

Returns pre-populated realistic accounts:

```json
[
  {
    "id": "acc_7k2m9n4p1q8r5t2w",
    "name": "BTC-Wallet",
    "currency": "BTC",
    "available": 0.05432,
    "usd_value": 3712.00
  },
  {
    "id": "acc_3n9x7y2k1j4h8g5f",
    "name": "ETH-Trading", 
    "currency": "ETH",
    "available": 2.456,
    "usd_value": 8472.00
  },
  {
    "id": "acc_9p2q3r4s5t6u7v8w",
    "name": "USD-Wallet",
    "currency": "USD",
    "available": 1250.50,
    "usd_value": 1250.50
  }
]
```

---

## Key Differences

| Feature | Production REST Client | Mock Client |
|---------|------------------------|-------------|
| Data Source | Live Coinbase API | Pre-defined mock data |
| Authentication | OAuth 2.0 | None (or environment auto-detect) |
| Rate Limiting | Yes (exponential backoff) | N/A (no real calls) |
| Health Checks | Yes | Yes (structural validation only) |
| Development Use | Optional (with credentials) | Primary (default mode) |

---

## Production Hardening Features (REST Client)

✅ Circuit breakers (opens after 5 failures, 10-min cooldown)  
✅ Input validation with sanitized credential logging  
✅ Exponential backoff retry for rate limits  
✅ Graceful fallback to mock data during API maintenance  
