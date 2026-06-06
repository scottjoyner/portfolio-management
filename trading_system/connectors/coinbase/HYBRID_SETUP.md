# Hybrid Coinbase Architecture Documentation

## Overview

The Hybrid Coinbase Setup implements a unified interface for both Commerce and Consumer APIs using existing Commerce API credentials.

### Architecture Diagram

```
┌─────────────────────────────────────────────┐
│         HYBRID COINBASE SETUP                │
├─────────────────────────────────────────────┤
│  Commerce API (Existing Keys)               │
│    → Payment processing                     │
│    → Wallet management                      │
│                                         │
│  Consumer API (Same Keys, Different Route)  │
│    → Balance viewing                        │
│    → Trading operations                     │
│    → Market data                            │
└─────────────────────────────────────────────┘
```

## API Endpoint Mappings

### Commerce API Endpoints
- **Base URL**: `https://api.exchange.coinbase.com`
- **Primary Use**: Payment processing, wallet management
- **Authentication**: Bearer token with Commerce credentials

### Consumer API Endpoints  
- **Base URL**: Same as Commerce (routes internally)
- **Primary Use**: Balance viewing, trading operations, market data
- **Authentication**: Reuses Commerce credentials but routes to different internal endpoints

## Configuration Examples

### Environment Variables (.env)

```bash
# Commerce API Credentials
COMMERCE_API_KEY="your_commerce_api_key"
COMMERCE_API_SECRET="your_commerce_api_secret"

# Optional: Enable mock mode for development
MOCK_MODE=false
```

### Python Usage

```python
from trading_system.connectors.coinbase.hybrid_setup import HybridCoinbaseSetup
from trading_system.connectors.coinbase.consumer_connector import CoinbaseConsumerConnector

# Initialize hybrid setup
setup = HybridCoinbaseSetup()
await setup.initialize()

# Connect to Consumer API
result = await setup.connect_consumer_api()
print(f"Connected: {result['status']}")

# Retrieve balances (mock mode for testing)
balances = await setup.get_balances(mock=True)
for balance in balances:
    print(f"{balance['currency']}: {balance['amount']} {balance['currency']}")
```

## Circuit Breaker Protection

The hybrid setup includes built-in circuit breakers:
- **Max Failures**: 5 consecutive failures before opening
- **Cooldown Period**: 10 minutes after opening
- **Auto-Recovery**: Automatic retry with exponential backoff

## Position Limit Enforcement

Before any trading operation, the system enforces:
- **Per Asset Limit**: Maximum 10% of total portfolio value
- **Total Portfolio Limit**: Hard cap on aggregate positions
- **Real-time Validation**: Checks performed before order submission

## Error Handling

### Common Errors and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `Credentials not found` | Missing .env file | Create `.env` with proper credentials |
| `Connection timeout` | Network issues | Check network connectivity, retry |
| `Rate limit exceeded` | Too many requests | Implement request throttling |
| `Invalid API key` | Wrong credentials | Verify credentials in Commerce dashboard |

## Testing Procedures

### 1. Mock Mode Testing
```bash
cd /home/falcon/git/portfolio-management
python3 trading_system/connectors/coinbase/hybrid_setup.py
```

Expected output:
```
[TEST] Running Full Setup Test
-" * 60
Initialization: initialized
Connection: connected

Sample Balances:
  - BTC: 0.15423 BTC
  - ETH: 2.87654 ETH
  - USDC: 15,420.50 USDC
```

### 2. Real API Testing
```bash
# Set MOCK_MODE=false in .env
MOCK_MODE=false
python3 trading_system/connectors/coinbase/hybrid_setup.py
```

## Validation Checklist

- [x] Hybrid setup module created
- [x] Consumer connector implemented
- [x] Mock mode testing available
- [ ] Real API credentials configured
- [ ] Balance display verified with real data
- [ ] Trading pair discovery tested
- [ ] Market data retrieval validated
- [ ] Error handling confirmed
- [ ] Circuit breaker protection active
- [ ] Position limit enforcement working

## Next Steps

1. **Configure Real Credentials**: Add actual Commerce API keys to `.env`
2. **Test Balance Display**: Verify crypto balances appear correctly
3. **Integration Testing**: Connect with trading systems
4. **Production Deployment**: Deploy with proper monitoring

## Troubleshooting

### Issue: No Balances Showing
**Solution**: Ensure `MOCK_MODE=false` and valid API credentials are set in `.env`

### Issue: Connection Timeout
**Solution**: Check network connectivity, verify endpoint URL is accessible

### Issue: Invalid Credentials Error
**Solution**: Regenerate Commerce API keys from Coinbase dashboard and update `.env`
