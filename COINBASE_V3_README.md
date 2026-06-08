# Production Coinbase v3 Trading Integration

## Overview

This replaces all broken mock implementations with **real, production-ready Coinbase Advanced Trade API integration** using the official Coinbase CLI.

**Key Features:**
- ✅ Real JWT/ES256 authentication (handled by the CLI)
- ✅ Market and limit orders with dry-run preview
- ✅ Portfolio and balance management
- ✅ Real-time price data
- ✅ Order management and fill tracking
- ✅ Idempotent order creation
- ✅ Comprehensive error handling
- ✅ Full JSON output for integration

## Quick Start

### 1. Install Dependencies

Node.js 22+ is already installed. Install the Coinbase CLI globally:

```bash
npm install -g @coinbase/coinbase-cli
```

On Linux, install libsecret for secure credential storage:

```bash
sudo apt install -y libsecret-tools
```

### 2. Configure Credentials

Get your API key from the [CDP Portal](https://portal.cdp.coinbase.com/projects/api-keys):
1. Create an API key with **Trade** and **Transfer** permissions
2. Set key type to **ECDSA** (important!)
3. Download the JSON key file

Then configure the CLI:

```bash
python3 scripts/setup_coinbase_credentials.py ~/Downloads/cdp_api_key.json
```

Verify configuration:

```bash
coinbase balance
```

### 3. Use in Your Code

```python
from trading_system.connectors.coinbase_v3 import CoinbaseConnectorV3

# Initialize
cb = CoinbaseConnectorV3()

# Get balances
balances = cb.get_balances()

# Get price
price_data = cb.get_price('BTC-USD')
print(f"BTC: ${price_data['price']}")

# Preview an order (no execution)
preview = cb.preview_order(
    product_id='BTC-USD',
    side='BUY',
    order_type='market',
    quote_size=100.0  # $100
)
print(f"Estimated fee: ${preview.total_fee}")

# Execute order (with idempotency)
order = cb.create_order(
    product_id='BTC-USD',
    side='BUY',
    order_type='market',
    quote_size=100.0,
    client_order_id='my-unique-id-12345'  # Makes it idempotent
)
print(f"Order ID: {order.order_id}")
```

## API Reference

### Market Data

```python
# Get price + volume + 24h change
price_data = cb.get_price('BTC-USD')

# List all tradable products
products = cb.list_products(product_type='SPOT')

# Get order book
book = cb.get_order_book('BTC-USD', level=2)

# Get OHLCV candles
candles = cb.get_candles('BTC-USD', granularity='1h', limit=100)
```

### Balances & Portfolios

```python
# Get all balances
balances = cb.get_balances()

# Get portfolios
portfolios = cb.get_portfolios()

# Get portfolio details
portfolio = cb.get_portfolio(portfolio_id)

# Create portfolio
new_portfolio = cb.create_portfolio(name='Trading Bot')
```

### Orders

```python
# Preview WITHOUT executing
preview = cb.preview_order(
    product_id='BTC-USD',
    side='BUY',
    order_type='market',
    quote_size=100.0
)

# Execute market order
order = cb.create_order(
    product_id='BTC-USD',
    side='BUY',
    order_type='market',
    quote_size=100.0
)

# Execute limit order
order = cb.create_order(
    product_id='BTC-USD',
    side='BUY',
    order_type='limit',
    base_size=0.001,
    limit_price=50000.0
)

# Get order details
order = cb.get_order(order_id)

# List orders
orders = cb.list_orders(product_id='BTC-USD')

# Get fills
fills = cb.get_fills(product_id='BTC-USD')

# Cancel order
cb.cancel_order(order_id)
```

### Conversions

```python
# Get conversion quote (USDC → USD)
quote = cb.get_conversion_quote(
    from_currency='USDC',
    to_currency='USD',
    amount=100.0
)

# Execute conversion
result = cb.execute_conversion(
    quote_id=quote['id'],
    from_currency='USDC',
    to_currency='USD'
)
```

### Account Info

```python
# Get fee tier and 30-day volume
fees = cb.get_fees()
```

## Architecture

```
├── scripts/
│   └── setup_coinbase_credentials.py       # Configure CLI with your key
├── trading_system/
│   ├── connectors/
│   │   └── coinbase_v3.py                  # Main production connector
│   └── coinbase_v3_examples.py             # Usage examples
└── README.md                                # This file
```

## Removed/Replaced Files

The following broken/mock implementations have been replaced:

- ❌ `check_balance_v3_jwt.py` - Mock JWT (no real signing)
- ❌ `test_coinbase_real.py` - Mock tokens and requests
- ❌ `update_coinbase_v3.py` - Incomplete setup
- ❌ `update_coinbase_v3_clean.py` - Placeholder credentials
- ❌ `check_balance_v2*.py` - Old v2 API (deprecated)
- ❌ `check_balance_commerce.py` - Wrong API path
- ❌ `check_balance_direct.py` - Missing authentication

**Replaced by:** `trading_system/connectors/coinbase_v3.py` ✅

## Important Concepts

### Idempotency

Always provide a `client_order_id` when creating orders:

```python
import uuid

order = cb.create_order(
    product_id='BTC-USD',
    side='BUY',
    order_type='market',
    quote_size=100.0,
    client_order_id=str(uuid.uuid4())  # Unique ID
)
```

If your connection drops and you retry with the same `client_order_id`, the API returns the **existing order** instead of creating a duplicate.

### Order Types

**Market Order** - Executes immediately at best available price:

```python
# Buy order: specify quote_size (amount to spend in USD)
cb.create_order(
    product_id='BTC-USD',
    side='BUY',
    order_type='market',
    quote_size=100.0  # Spend $100
)

# Sell order: specify base_size (amount of asset to sell)
cb.create_order(
    product_id='BTC-USD',
    side='SELL',
    order_type='market',
    base_size=0.001   # Sell 0.001 BTC
)
```

**Limit Order** - Executes at specified price or better:

```python
cb.create_order(
    product_id='BTC-USD',
    side='BUY',
    order_type='limit',
    base_size=0.001,    # Buy 0.001 BTC
    limit_price=50000   # At $50,000 or less
)
```

### Preview Before Executing

Always preview before executing:

```python
# See estimated fees, slippage, fill price
preview = cb.preview_order(
    product_id='BTC-USD',
    side='BUY',
    order_type='market',
    quote_size=100.0
)

print(f"Estimated fill: ${preview.estimated_fill_price}")
print(f"Fees: ${preview.total_fee}")
print(f"Total cost: ${preview.total_cost}")

# If preview looks good, execute
if preview.total_fee < 1.0:  # Less than $1 fee
    order = cb.create_order(...)
```

## Global Flags (CLI)

You can also use the Coinbase CLI directly with these flags:

| Flag | Purpose |
|------|---------|
| `--dry-run` | Assemble request without sending (for any command) |
| `--template` | Show expected request body format |
| `--jq <expr>` | Filter JSON response with jq expression |
| `-e <env>` | Override active environment |

Example:

```bash
# Preview order (no execution)
coinbase orders preview product_id=BTC-USD side=BUY type=market quote_size=100 --dry-run

# Get just the price
coinbase products get BTC-USD --jq '.price'

# Switch environment
coinbase balance -e sandbox
```

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `HTTP 401` | Ed25519 key or expired creds | Create new **ECDSA** key in Portal |
| `HTTP 403 Missing required scopes` | Missing Trade/Transfer permissions | Check key permissions in Portal |
| `insufficient fund` | Balance too low | Run `coinbase balance` to verify |
| `coinbase: command not found` | CLI not on PATH | `npm install -g @coinbase/coinbase-cli` |
| `MISSING_FIELDS` | Required fields not provided | Run `coinbase <command> --template` |

## Integration with Trading System

To use in your existing trading system:

```python
# trading_system/unified_price_fetcher.py
from trading_system.connectors.coinbase_v3 import CoinbaseConnectorV3

class UnifiedPriceFetcher:
    def __init__(self):
        self.coinbase = CoinbaseConnectorV3()
    
    async def fetch_crypto_price(self, symbol: str):
        product_id = f"{symbol}-USD"
        price_data = self.coinbase.get_price(product_id)
        return {
            'symbol': symbol,
            'price': price_data['price'],
            'change_24h': price_data.get('price_percentage_change_24h'),
            'volume_24h': price_data.get('volume_24h')
        }
    
    async def place_trade(self, product_id: str, side: str, amount: float):
        order = self.coinbase.create_order(
            product_id=product_id,
            side=side,
            order_type='market',
            quote_size=amount if side == 'BUY' else None,
            base_size=amount if side == 'SELL' else None
        )
        return order
```

## Next Steps

1. **Test the setup:**
   ```bash
   python3 trading_system/coinbase_v3_examples.py
   ```

2. **Integrate into your trading system:**
   - Import `CoinbaseConnectorV3` in your strategy files
   - Use `preview_order()` before any real trades
   - Implement proper error handling and logging

3. **Build trading strategies:**
   - DCA (dollar-cost averaging)
   - Momentum-based trading
   - Arbitrage detection
   - Portfolio rebalancing

## Support

For detailed API documentation, see:
https://docs.cdp.coinbase.com/coinbase-cli/

For CDP Portal access:
https://portal.cdp.coinbase.com/

## License

This integration uses the official Coinbase CLI, which is licensed under the Coinbase Terms of Service.
