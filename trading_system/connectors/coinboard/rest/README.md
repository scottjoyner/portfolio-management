# Coinboard REST Client Package

Production Read-Only Brokerage API with Full Safety Features

## Quick Start

```python
from trading_system.connectors.coinboard.rest import create_read_only_client

client = await create_read_only_client()
accounts = await client.list_accounts()
balances = await client.fetch_account('cb-primary-wallet-usd')
```

## Features

### ✅ OAuth 2.0 Token Management
- Authorization Code flow with PKCE support
- Automatic token refresh before expiry
- Secure storage in encrypted environment variables

### ✅ Circuit Breaker Pattern
- Opens after **5 consecutive failures**
- **10-minute cooldown** before retry
- Prevents cascade failures during API maintenance

### ✅ Input Validation
- Validates all input parameters
- Masked error logging: `fxp_***...****1234`
- No raw credentials in logs or output

### ✅ Position Limits
- Max **10% position size** per asset (configurable)
- Prevents over-concentration risk
- Enforced at portfolio level

### ✅ Rate Limiting Compliance
- Parses rate limit headers from API responses
- Exponential backoff for transient errors
- Respects Coinbase v3 API limits

### ✅ Health Check Endpoints
- Structured status for monitoring systems
- Auto-detection of environment state
- Circuit breaker health tracking

## Architecture

```
trading_system/connectors/coinboard/rest/
├── __init__.py          # Module exports
├── client.py            # Main REST client implementation
├── oauth.py             # OAuth 2.0 token management
├── circuit_breaker.py   # Reusable circuit breaker pattern
└── README.md            # This file
```

## Configuration

### Read-Only Mode (Default)

```python
from trading_system.connectors.coinboard.rest import create_read_only_client

client = await create_read_only_client()
```

### Production Mode with Credentials

```python
config = {
    'redirect_uri': 'http://localhost/callback',  # OAuth callback
    'access_token': 'fxp_***...****1234',  # OAuth token
}

client = await create_default_rest_client(config)
```

## API Endpoints Implemented

### Account Management
- `GET /v3/accounts/{id}` - Fetch single account balance
- `GET /v3/accounts` - List all brokerage accounts
- `GET /v3/accounts/{id}/ledger` - Transaction history

### Position Management  
- `POST /v3/positions` - Create position (write endpoint, optional)
- `GET /v3/positions` - Fetch current holdings

### Health Checks
- `GET /v3/health` - Service health status
- Auto-detect health endpoints

## Safety Features

### Circuit Breaker

```python
client = await create_read_only_client()

try:
    accounts, error = await client.list_accounts()
except CircuitBreakerError as e:
    print(f"Circuit breaker open: {e}")
    # Use mock data or fallback connector
```

### Input Validation

```python
# Validates before API call
try:
    account = await client.fetch_account('invalid')  # Too short ID
except ValueError as e:
    print(f"Validation error (masked): {e}")  # fxp_***...****1234
```

### Rate Limiting

```python
# Automatic retry with exponential backoff
client = await create_read_only_client()
try:
    for attempt in range(5):
        try:
            accounts = await client.list_accounts()
            break  # Success, exit retry loop
        except CircuitBreakerError:
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
except Exception as e:
    print(f"All retries failed (masked): fxp_***...****1234")
```

## Production Deployment Checklist

### Prerequisites
- [ ] Coinbase OAuth application registered at dashboard
- [ ] Client ID and client secret obtained
- [ ] Redirect URI configured in OAuth settings

### Configuration Files
```bash
mkdir -p ~/.hermes/coinboard

# Create .env with sensitive data (use secure storage)
cat > ~/.hermes/coinboard/.env << EOF
COINBOARD_CLIENT_ID=your_client_id_here
COINBOARD_REDIRECT_URI=http://your-app/callback
EOF
```

### Container Deployment
```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY trading_system/connectors/coinboard/rest/*.py ./rest/

RUN pip install -r requirements.txt

CMD ["python", "-m", "trading_system.connectors.coinboard.rest.main"]
```

### Health Check Endpoint
```python
from trading_system.connectors.coinboard.rest import create_read_only_client

client = await create_read_only_client()
health, error = await client.health_check()
if not error:
    print(f"Status: {health['status']}")
    print(f"Circuit Breaker Active: {health['components']['circuit_breaker_active']}")
```

## Testing

### Unit Tests
```python
from trading_system.connectors.coinboard.rest import create_read_only_client

client = await create_read_only_client()

# Test health check
health, error = await client.health_check()
assert health['status'] == 'healthy'

# Test circuit breaker
async def failing_call():
    raise Exception("API Error")

try:
    await client.circuit_breaker.call_if_closed(failing_call)
except CircuitBreakerError as e:
    print(f"Circuit breaker works: {e}")
```

### Integration Tests with Mock Data
```python
from trading_system.connectors.coinboard.mock_client import CoinbaseMockClient

mock_client = await create_default_mock_client()
accounts = await mock_client.list_accounts()
print(f"Accounts: {len(accounts)}")
```

## Troubleshooting

### Circuit Breaker Keeps Opening

```python
# Check failure count and cooldown
print(f"Failure Count: {breaker.state.failure_count}")
print(f"Circuit Open: {breaker.state.is_open()}")
print(f"Cooldown Until: {cooldown_until}")
```

### Token Expiry Issues

```python
from trading_system.connectors.coinboard.rest.oauth import CoinbaseOAuthManager

oauth = CoinbaseOAuthManager(config)
if oauth.is_token_expired(token_response):
    print("Token expiring soon, refreshing...")
    new_token = await oauth.refresh_access_token(refresh_token)
```

### Input Validation Errors

```python
# Check input before API call
try:
    account = await client.fetch_account('cb-12345')  # May be invalid
except ValueError as e:
    print(f"Input validation error (masked): fxp_***...****1234")
```

## License

MIT License - Production Read-Only Coinbase API Access with Circuit Breaker Pattern
