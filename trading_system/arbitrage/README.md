# Kalshi <-> Polymarket Arbitrage System

A complete end-to-end implementation for detecting and executing arbitrage opportunities between Kalshi and Polymarket prediction markets.

## Overview

This system identifies price discrepancies between similar binary outcome markets on Kalshi and Polymarket, enabling arbitrage strategies where you can:
- Buy on the cheaper platform (Kalshi or Polymarket)
- Sell on the more expensive platform
- Lock in risk-free profit from the price difference

### Example Strategy

```
Bitcoin > $100K by Jan 31, 2025:
  Kalshi Price: 58.5% ($58.50 per contract)
  Polymarket Price: 46.8% ($46.80 per share)

Arbitrage Plan:
  1. Buy Bitcoin=Yes on Polymarket @ $46.80/share
  2. Sell Bitcoin=Yes on Kalshi @ $58.50/contract
  3. Profit: ~11.7% before fees, or ~9.4% after trading fees
```

## Architecture

### Components

```
trading_system/arbitrage/
├── opportunity_detector.py   # Market matching and opportunity detection
├── arb_trader.py             # Trade execution logic
├── main.py                   # Orchestration script
├── data/                     # Mock data and results storage
│   ├── kalshi_mock.json      # Sample Kalshi market data
│   ├── polymarket_mock.json  # Sample Polymarket event data
│   └── opportunities.json    # Detected arbitrage opportunities
└── scripts/
    ├── detect_opportunities.py  # Standalone detection tool
    └── debug_matching.py        # Matching algorithm debugging
```

### Data Flow

```
┌─────────────┐     ┌─────────────────┐     ┌────────────────┐
│ Kalshi API  │────>│ Opportunity     │────>│ Trade Execution │
│ or mock data│     │ Detector        │     │ (Kalshi/PM)    │
└─────────────┘     └─────────────────┘     └────────────────┘

┌─────────────┐     ┌─────────────────┐     ┌────────────────┐
│ Polymarket  │<----│                  │<----│ Kalshi/PM API │
│ or mock data│     │                  │     │ Response       │
└─────────────┘     │                  │     └────────────────┘
```

## Installation & Setup

### Requirements

- Python 3.8+
- pip packages: `requests` (for API calls)

### Dependencies

```bash
pip install requests
```

### Environment Variables

Create a `.env` file or set these environment variables:

```bash
# Optional: Real Kalshi API access
export KALSHI_API_KEY="your_kalshi_api_key"
export KALSHI_API_BASE_URL="https://api.kalshi.com/v2"

# Optional: Real Polymarket API access  
export POLYMARKET_API_KEY="your_polymarket_api_key"
export POLYMARKET_API_BASE_URL="https://api.polygon.io/v2"

# Default: Mock data mode (development/testing)
```

## Usage

### Quick Start (Development Mode)

The system uses mock data by default. No API keys required for testing.

```bash
cd /home/falcon/git/portfolio-management

# Run opportunity detection
python trading_system/arbitrage/main.py
```

Expected output:
```
================================================================================
Kalshi <-> Polymarket Arbitrage System
================================================================================

[1/4] Fetching Kalshi markets...
      Loaded 2 Kalshi markets
[2/4] Fetching Polymarket events...
      Loaded 2 Polymarket events
[3/4] Detecting arbitrage opportunities...
      Found 4 arbitrage opportunity(ies)
[4/4] Executing arbitrage trades...

Trade Execution Summary
================================================================================
```

### Standalone Opportunity Detection

Detect opportunities without executing trades:

```bash
python trading_system/scripts/detect_opportunities.py
```

Output saved to `trading_system/data/opportunities.json`.

### Using Real APIs

For live trading with real Kalshi and Polymarket markets:

1. Get API keys from each platform:
   - **Kalshi**: Sign up at [kalshi.com](https://kalshi.com) → Settings → API Keys
   - **Polymarket**: Use [Polygon.io](https://polygon.io/) (covers multiple exchanges including Polymarket)

2. Set environment variables:

```bash
export KALSHI_API_KEY="your_kalshi_api_key"
export POLYMARKET_API_KEY="your_polygonio_api_key"
```

3. Run the system - it will automatically use real APIs when keys are set.

## Trading Strategies

### Strategy 1: Balanced Arbitrage (Recommended for Testing)

Split capital 50/50 between platforms:

```python
strategy = "balanced"  # $100 on each side, $50 per platform
```

**Pros**: Lower risk, diversified exposure  
**Cons**: Smaller absolute profits

### Strategy 2: Kalshi-First

Focus more capital on Kalshi positions:

```python
strategy = "kalshi_first"  # 67% Kalshi, 33% Polymarket
```

**Use case**: When you want higher leverage on one platform

### Strategy 3: Polymarket-First

Focus more capital on Polymarket positions:

```python
strategy = "pm_first"  # 33% Kalshi, 67% Polymarket
```

**Use case**: When Kalshi liquidity is stronger or fees are lower

## Fee Structure

### Kalshi Fees
- Standard: ~1% per trade
- Maker/taker model on order book
- Settlement is free

### Polymarket Fees (via Polygon.io)
- Standard: ~2% per trade
- Flat fee structure
- Settlement is free

### Net Profit Example

```
Kalshi price: 58.50% ($58.50/contract)
Polymarket price: 46.80% ($46.80/share)
Divergence: 11.70%

Trade $10,000 notional:
  - Buy on Polymarket: -$4,680 (50% allocation)
  - Sell on Kalshi: +$5,850 (50% allocation)
  - Gross profit: $1,170
  
  - Fees (-2% PM, -1% KLS): -$97.50
  - Net profit: $1,072.50 (~10.72% return)
```

## Testing & Debugging

### Test Detection Algorithm

```bash
# Run with mock data
python trading_system/scripts/detect_opportunities.py

# Expected output format:
"""
================================================================================
Kalshi <-> Polymarket Arbitrage Opportunity Detection
================================================================================

Found 4 opportunity(ies):

Opportunity #1:
  Kalshi: BTC-FEB28-75K
  Polymarket: bitcoin-75k-by-feb-28
  Kalshi Price: 71.8%
  Polymarket Price: 60.2%
  Divergence: 11.6%
  Potential Return: 114.88%

[More opportunities...]
"""
```

### Test Trade Execution

The mock clients will generate sample orders and track them in memory. To inspect tracked orders after execution:

```python
from trading_system.arbitrage.main import MockKalshiClient, MockPolymarketClient

kalshi_client = MockKalshiClient()
polymarket_client = MockPolymarketClient()

# Execute trades
orders = kalshi_client.create_order(...)
orders = polymarket_client.create_order(...)

print("Tracked orders:")
for order in kalshi_client.orders:
    print(f"  {order}")
```

### Debug Matching Algorithm

```bash
python trading_system/scripts/debug_matching.py
```

This will show you which Kalshi markets match which Polymarket events based on text similarity.

## Real API Integration

### VPN / VPS Setup

For accessing US-based markets from outside the US, you have several options:

#### Option 1: VPS (Recommended)

Rent a cloud server in the US:
- **AWS EC2**: $0.04/hour for t3.micro (permanent free tier eligible if unused)
- **DigitalOcean Droplet**: ~$5/month
- **Linode**: ~$5/month
- **Oracle Cloud**: Free tier available

**Setup:**
```bash
# On your VPS in US:
sudo apt update && sudo apt install python3-pip -y
pip install requests
git clone YOUR_REPOSITORY_URL
cd portfolio-management
python trading_system/arbitrage/main.py
```

#### Option 2: VPN Service

Use a premium VPN service with US servers:
- **NordVPN**: $3.39/month (long-term)
- **Surfshark**: Unlimited devices, good for multiple machines
- **Mullvad**: Privacy-focused, pay-as-you-go

**Limitations:**
- Some exchanges block known VPN IPs
- May experience latency issues
- Not ideal for real-time trading

#### Option 3: Home Network Configuration

Set up your home internet with a US proxy IP via:
- Smart DNS services (e.g., SmartDNS, Unbound IDN)
- OpenVPN/WireGuard server running on a separate device
- Residential proxy services (e.g., Bright Data, IPRoyal - pay by traffic volume)

### API Rate Limits & Best Practices

**Kalshi API:**
- Rate limit: ~60 requests/minute
- Use exponential backoff for retry logic
- Cache market data (updates every 1-5 seconds typically)

**Polymarket/Polygon.io API:**
- Rate limit: Varies by endpoint
- Public endpoints often have stricter limits
- Use caching aggressively

### Recommended Caching Strategy

```python
from functools import lru_cache
import requests
import time

@lru_cache(maxsize=100)
def fetch_market_data(base_url: str, endpoint: str):
    """Cache API responses to avoid rate limit issues."""
    try:
        response = requests.get(f"{base_url}/{endpoint}", timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"API fetch error: {e}")
    return None

# In production code, cache market snapshots:
market_snapshot = {}

def get_fresh_markets(cache_timeout: int = 180):  # 3 minutes
    if datetime.now() - snapshot_time < timedelta(seconds=cache_timeout):
        return market_snapshot
    market_snapshot = fetch_market_data(...)
    snapshot_time = datetime.now()
    return market_snapshot
```

## Production Hardening Checklist

- [ ] Set up real API keys and configure environment variables
- [ ] Implement circuit breaker for failed trades
- [ ] Add comprehensive logging with structured JSON output
- [ ] Set up alerts for trading errors (email, Telegram, etc.)
- [ ] Implement position limits per market (max $500 per trade)
- [ ] Add rate limiting and retry logic with exponential backoff
- [ ] Set up monitoring dashboard (Grafana/Prometheus)
- [ ] Configure VPN or VPS for US market access
- [ ] Test error handling thoroughly
- [ ] Document all API endpoints used
- [ ] Implement kill switch for emergency stop

## Monitoring & Alerts

### Alert Configuration

Add alerts to trading_system/arbitrage/alerts.py:

```python
class TradingAlerts:
    def __init__(self):
        self.telegram_bot = TelegramNotifier(...)
        self.email_sender = EmailSender(...)
    
    def alert_trade_error(self, error: str):
        """Send immediate alert on trade failure."""
        self.telegram_bot.send(f"⚠️ Trade Error:\n{error}")
    
    def alert_opportunity_found(self, opportunity: dict):
        """Alert when new arb opportunity detected."""
        self.email_sender.send(f"🎯 New Opportunity!\n{opportunity}")
```

### Monitoring Dashboard

Create a simple HTML dashboard showing:
- Open positions (Kalshi)
- Open positions (Polymarket)
- Total P/L
- Recent trade history
- Active opportunities

## Security Considerations

When using real APIs:

1. **Never hardcode API keys** - use environment variables
2. **Use .gitignore** for `.env` files
3. **Set rate limits** to prevent accidental over-trading
4. **Implement position limits** per market pair
5. **Log all trades** with audit trail
6. **Use separate account** for testing (Kalshi has sandbox mode)

## Troubleshooting

### "No opportunities detected"

- Check that mock data files exist: `trading_system/data/kalshi_mock.json`, `trading_system/data/polymarket_mock.json`
- Verify text similarity threshold isn't too high (default is 75%)
- Inspect matching logic in opportunity_detector.py to ensure markets match correctly

### "Real API order failed"

Common errors:
- **401 Unauthorized**: Invalid or expired API key
- **429 Too Many Requests**: Rate limit exceeded - add retry with backoff
- **400 Bad Request**: Malformed order parameters - check price/quantity format

### "Market not found"

Possible causes:
- Market has already closed (Kalshi shows `closed` status)
- Market doesn't exist or slug changed
- API version mismatch - ensure you're using the same API version for both platforms

## Future Enhancements

Planned features:
1. **Real-time WebSocket integration** for live market feeds
2. **Machine learning model** to predict opportunity likelihood
3. **Automated trading bot** with configurable risk parameters
4. **Multi-exchange arbitrage** (add more prediction markets)
5. **Slippage protection** for volatile markets
6. **Portfolio management** dashboard

## License

This project is licensed under the MIT License - see LICENSE file for details.

## Support

For issues, questions, or contributions:
- File an issue on GitHub
- Review existing issues before creating new ones
- Check documentation in README files

---

**Author**: Hermes Agent  
**Version**: 0.3.5  
**Last Updated**: June 2026
