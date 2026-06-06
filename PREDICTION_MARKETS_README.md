# Prediction Markets Trading Infrastructure

## Overview
Complete trading infrastructure for prediction markets, integrating with your portfolio management system. Built for seamless integration with Alpaca CLI for stocks/crypto positions.

### Platforms Supported
- **Alpaca** - Stocks & crypto (already active - paper trading by default)
- **Kalshi** - US-regulated prediction markets (CPI, GDP, elections, weather, sports, crypto events)
- **Polymarket** - Decentralized blockchain prediction markets (global events, sports, crypto prices, entertainment)

---

## Quick Start

### 1. Add API Keys (When Ready)
Edit `.env` file in portfolio-management directory:

```bash
# Kalshi - US-regulated platform
KALSHI_API_KEY=your_a...re
KALSHI_API_SECRET=your_a...re
# Get from: https://kalshi.com/account > Settings > API Access

# Polymarket - Decentralized platform  
POLYMARKET_API_KEY=your_a...re
POLYMARKET_WALLET_ADDRESS=0xYourWalletAddressHere
# Get from: https://docs.polymarket.io/reference

# Alpaca - Already configured (paper trading default)
# To enable live trading, uncomment:
# ALPACA_LIVE_TRADE=true
```

### 2. Test Connections
```bash
cd /home/falcon/git/portfolio-management

# Test Kalshi connector
python trading_system/connectors/kalshi.py

# Test Polymarket connector  
python trading_system/connectors/polymarket.py

# Test unified price fetcher
python trading_system/unified_price_fetcher.py
```

### 3. Use CLI Interface (predictm)
```bash
chmod +x scripts/predictm
./scripts/predictm balance          # Check balances
./scripts/predictm markets --event us-pres-24   # List election markets
./scripts/predictm history market_id           # View historical data
./scripts/predictm watcher bitcoin-price       # Continuous price monitoring
```

---

## Connectors Reference

### Kalshi Connector (`connectors/kalshi.py`)
**Platform**: US-regulated prediction market (CFTC compliant)  
**Use Case**: Economic indicators, elections, weather, sports outcomes

**Key Features:**
- Binary outcome contracts (yes/no propositions)
- Weekly settlement cycles
- Real-money trading on popular markets
- Market-making opportunities

**API Capabilities:**
```python
from trading_system.connectors.kalshi import KalshiConnector

kalshi = KalshiConnector(
    api_key=os.environ.get('KALSHI_API_KEY'),
    api_secret=os.environ.get('KALSHI_API_SECRET')
)

# List available markets
markets = await kalshi.get_markets(category='Economic Indicators')

# Place a prediction market trade
order = await kalshi.place_market_order(
    market_id='2025-02-CPI-US-H',  # CPI inflation rate contract
    direction='call',              # Buy (yes) contracts
    quantity=10,                   # Contract shares
    price=None                     # Use market order
)

# Fetch historical prices
history = await kalshi.get_market_history(
    market_id='2024-11-PRES-Demo-Q',
    granularity=86400  # Hourly data
)
```

**Common Market Categories:**
- Economic Indicators (CPI, GDP, inflation)
- Elections (Presidential, midterm, local)
- Weather (temperature, precipitation forecasts)
- Sports (Super Bowl, Olympics outcomes)
- Crypto Events (Bitcoin halving, network events)

---

### Polymarket Connector (`connectors/polymarket.py`)
**Platform**: Decentralized blockchain-based prediction markets  
**Use Case**: Global events, sports, crypto prices, entertainment

**Key Features:**
- Blockchain settlement (Ethereum, Solana, Polygon)
- Smart contract execution
- High liquidity across popular markets
- Global accessibility

**API Capabilities:**
```python
from trading_system.connectors.polymarket import PolymarketConnector

polymarket = PolymarketConnector(
    api_key=os.environ.get('POLYMARKET_API_KEY'),
    wallet_address=os.environ.get('POLYMARKET_WALLET_ADDRESS'),
    chain='ethereum'  # Options: 'ethereum', 'solana', 'polygon'
)

# List available markets
markets = await polymarket.query_markets(
    event='us-pres-24',           # Filter by event (optional)
    limit=20                      # Max results
)

# Place a bet on a market outcome
bet = await polymarket.place_bet(
    market_id='us-pres-24-biden-win',  # Biden win election 2024
    outcome_index=0,                    # 0=first option (Biden), 1=second (Trump)
    amount_usdc=50.0,                   # Wager in USDC stablecoins
    limit_price=None                    # Use fair price from order book
)

# Get market details and order book
details = await polymarket.get_market_details(market_id='bitcoin-price-above-100k')
order_book = await polymarket.get_order_book(market_id='us-pres-24-biden-win')
```

**Common Market Categories:**
- US elections and politics
- Sports outcomes (championships, tournaments)
- Crypto price predictions
- Global events and news
- Entertainment industry results
- Science & finance indicators

---

### Unified Price Fetcher (`unified_price_fetcher.py`)
**Use Case**: Cross-platform price aggregation and arbitrage detection

**API Capabilities:**
```python
from trading_system.unified_price_fetcher import UnifiedPriceFetcher

fetcher = UnifiedPriceFetcher()
await fetcher.initialize()  # Auto-discover credentials from .env

# Fetch prices across all platforms
prices = await fetcher.fetch_all_prices(
    stocks=['AAPL', 'TSLA', 'NVDA'],          # Stock symbols via Alpaca
    cryptos=['BTC-USD', 'ETH-USD'],           # Crypto pairs via Coinbase
    predictions=[                              # Prediction markets
        {'market_id': 'us-pres-24-biden-win', 'outcome': 0}
    ]
)

print("Stocks:", prices['stocks'])
print("Cryptos:", prices['cryptos'])
print("Predictions:", prices['predictions'])
```

---

### Backtesting Engine (`backtesters/prediction_markets.py`)
**Use Case**: Historical analysis and strategy backtesting for prediction markets

**Key Features:**
- Fetch historical market data
- Calculate performance metrics (returns, volatility)
- Analyze price movements and trading patterns

**API Capabilities:**
```python
from trading_system.backtesters.prediction_markets import PredictionMarketsBacktester

backtester = PredictionMarketsBacktester()
await backtester.initialize()

# Fetch historical data for a market
history = await backtester.fetch_historical_market_data(
    market_id='us-pres-24-biden-win',
    start_date='2024-01-01T00:00:00Z',
    end_date=datetime.now().isoformat(),
    platform='polymarket'
)

# Calculate backtesting metrics
metrics = backtester.calculate_backtest_metrics(history)
print(f"Total Return: {metrics['total_return_percent']}%")
print(f"Annualized Volatility: {metrics['volatility_annualized']}%")
```

---

## CLI Integration (predictm)

**Use Case**: Terminal-based interaction with prediction markets

**Commands:**
```bash
# Check account balances
./scripts/predictm balance

# List available prediction markets
./scripts/predictm markets --event us-pres-24      # Filter by event
./scripts/predictm markets                          # List top markets

# Get detailed market information
./scripts/predictm market info us-pres-24-biden-win

# View historical price data
./scripts/predictm history bitcoin-price-above-100k 2024-01-01

# Continuous price watcher (like tail -f)
./scripts/predictm watcher bitcoin-price-above-100k
```

**Integration with Alpaca CLI:**
The predictm CLI also checks your Alpaca position for cross-trading opportunities:
```bash
# View Alpaca positions for stocks/crypto
./scripts/predictm order list

# This automatically calls: alpaca position list --quiet
```

---

## Market Examples

### Kalshi Sample Markets:
| Category | Example ID | Description |
|----------|------------|-------------|
| Economic Indicators | `2025-02-CPI-US-H` | CPI inflation rate above threshold |
| Elections | `2024-11-PRES-Demo-Q` | Democratic Party nominee 2024 |
| Weather | `2025-03-TEMP-NYC` | NYC temperature in March 2025 |
| Crypto Events | `2024-06-BTC-HALVING` | Bitcoin halving date prediction |
| Fed Rates | `2025-02-FED-RATES-3.5` | Fed rate at 3.5% in February 2025 |

### Polymarket Sample Markets:
| Category | Market ID | Description |
|----------|-----------|-------------|
| US Elections | `us-pres-24-biden-win` | Biden wins 2024 presidential election |
| Crypto Prices | `bitcoin-price-above-100k` | Bitcoin price above $100,000 |
| Sports | `sb-lii-winner-kansas-city` | Kansas City wins Super Bowl LIX |
| Global Events | `china-deficit-march-2025` | China trade deficit in March 2025 |

---

## Pricing & Trading Costs

### Alpaca (Stocks/Crypto)
- **Maker/Taker Fees**: $0 for paper trading, ~$2.49/share for live stock trading
- **Crypto Fees**: Varies by exchange integration (typically <1%)

### Kalshi
- **Trading Fees**: 0% commission on trades
- **Spread Costs**: Built into contract prices (typically 1-3 basis points)
- **Settlement**: Full payout if prediction correct, no payout otherwise

### Polymarket
- **Trading Fees**: No explicit fees (market-driven spreads)
- **Platform Spread**: 0.01% - 0.5% depending on liquidity
- **USDC Network Fees**: Minimal gas costs on Ethereum/Polygon
- **Settlement**: Full payout if prediction correct, no payout otherwise

---

## Risk Management

### Position Sizing Guidelines:
- **Prediction Markets**: Treat as binary bets (max 1-2% of portfolio per contract)
- **Stocks/Crypto (Alpaca)**: Use standard position sizing (<5% per position)
- **Diversification**: Spread predictions across different event categories

### Stop-Loss Strategies:
- **Time-based**: Close positions near settlement date if conviction weakens
- **Price-based**: Exit when market price falls below your thesis (Kalshi/Polymarket)
- **Portfolio-level**: Set max 10% allocation to prediction markets combined

---

## Development Workflow

### Testing with Mock Mode:
All connectors automatically detect mock credentials and use test mode:

```bash
# When API keys contain "***" - safe development mode
python trading_system/connectors/kalshi.py
# Output: "Mock mode activated - no live connection"

# To enable live trading, replace *** in .env with real keys
```

### Example Development Session:
```bash
# 1. Initialize connectors (checks credentials from .env)
python trading_system/connectors/kalshi.py
python trading_system/connectors/polymarket.py

# 2. Test unified price fetching
python trading_system/unified_price_fetcher.py

# 3. Check CLI integration
./scripts/predictm balance

# 4. Start continuous monitoring (once credentials added)
./scripts/predictm watcher us-pres-24-biden-win
```

---

## Production Deployment Checklist

Before enabling live trading:

### Kalshi:
- [ ] KYC completed on Kalshi account (required for US platform)
- [ ] Live API keys generated and tested
- [ ] Paper trading validated first
- [ ] Risk management rules in place

### Polymarket:
- [ ] Wallet funded with sufficient gas tokens (ETH/SOL/Polygon native token)
- [ ] Live API key generated
- [ ] Test bets placed (<$5 per transaction)
- [ ] Understanding of blockchain settlement timelines

### Alpaca (already configured):
- [ ] Paper trading validated (default - safe for development)
- [ ] To enable live: `ALPACA_LIVE_TRADE=true` in .env
- [ ] Position sizing rules enforced
- [ ] Stop-loss strategies implemented

---

## Architecture Overview

```
Portfolio Management System
├── Alpaca Connector ✅ (Active - Paper Trading Default)
│   └── Stocks/Crypto positions via Alpaca CLI
│
├── Kalshi Connector ⚡ (Ready for keys)
│   └── US-regulated prediction markets
│       ├── CPI/GDP indicators
│       ├── Election outcomes
│       ├── Weather events
│       └── Sports/cryptocurrency events
│
├── Polymarket Connector ⚡ (Ready for keys)
│   └── Decentralized blockchain prediction markets
│       ├── Global events
│       ├── Sports outcomes
│       ├── Crypto prices
│       └── Entertainment industry results
│
├── Unified Price Fetcher ✅ (Integrated)
│   └── Aggregates across all platforms for arbitrage detection
│
├── Backtesting Engine ✅ (Ready)
│   └── Historical analysis for strategy validation
│
└── CLI Interface (predictm) ✅ (Active)
    └── Terminal interaction with all platforms
```

---

## Getting Help

### API Documentation:
- **Kalshi**: https://www.kalshi.com/docs
- **Polymarket**: https://docs.polymarket.io/
- **Alpaca**: https://alpaca.markets/docs/api-documentation/

### Support Channels:
- Kalshi: https://kalshi.com/support
- Polymarket: https://github.com/polymarket/polymarket/issues
- Alpaca: https://alpaca.markets/contact-us/

---

## Next Steps

Once you add your API keys:

1. **Test Connections**: Verify all connectors work with live data
2. **Explore Markets**: Browse available prediction markets on Kalshi/Polymarket
3. **Place Test Orders**: Small trades to validate trading flows
4. **Analyze Performance**: Use backtesting engine for strategy validation
5. **Monitor Live Positions**: Use CLI watcher for continuous monitoring

---

## Security Notes

- **Never commit `.env` files with real keys** - They contain sensitive credentials
- **Use separate `.env.production`** for live trading keys
- **Rotate API keys regularly**, especially for prediction markets (less regulated)
- **Kalshi requires KYC** - US platform with identity verification
- **Polymarket is global** but subject to sanctions/compliance

---

## License

This infrastructure is part of your portfolio management system. All rights reserved.
