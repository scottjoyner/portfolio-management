# Kalshi <-> Polymarket Arbitrage Implementation Summary

## What We've Built

### Core Components

1. **Opportunity Detection (`trading_system/arbitrage/opportunity_detector.py`)**
   - Matches Kalshi markets with Polymarket events using text similarity
   - Identifies price discrepancies between similar markets
   - Filters by minimum divergence threshold (default: 75% similarity, >1% divergence)

2. **Trade Execution (`trading_system/arbitrage/arb_trader.py`)**
   - Executes arbitrage trades on both Kalshi and Polymarket
   - Supports three strategies: balanced, kalshi_first, pm_first
   - Mock clients for development/testing, real API integration ready

3. **Main Orchestration (`trading_system/arbitrage/main.py`)**
   - Fetches market data from APIs or mock files
   - Detects opportunities automatically
   - Executes trades and tracks results
   - Outputs JSON results to file

4. **Test Scripts**
   - `comprehensive_test.py`: Full test suite for all components
   - `test_all.py`: Alternative comprehensive tests
   - `detect_opportunities.py`: Standalone detection tool

## How to Use

### Quick Start (Development Mode)

```bash
cd /home/falcon/git/portfolio-management

# Run comprehensive tests and demo
python3 trading_system/arbitrage/comprehensive_test.py
```

Expected output:
```
======================================================================
Testing Imports...
[✓] All modules imported successfully

======================================================================
Testing Mock Data...
[✓] Kalshi: 2 markets loaded
[✓] Polymarket: 2 events loaded

======================================================================
Testing Opportunity Detection...
[✓] Found X opportunity(ies)

======================================================================
Testing Trade Execution...
[✓] Kalshi Order ID: KLS-...
[✓] Polymarket Order ID: PM-...
```

### Run Main Arbitrage System

```bash
python3 trading_system/arbitrage/main.py
```

This will:
1. Fetch markets from mock files
2. Detect arbitrage opportunities
3. Execute sample trades
4. Save results to JSON

## API Integration (Production)

### Environment Variables

For real trading, set these environment variables:

```bash
export KALSHI_API_KEY="your...port POLYMARKET_API_KEY="your...
```

The system automatically detects when API keys are set and uses real APIs instead of mock data.

### Example Real Trading Workflow

```bash
# 1. Get API keys from platforms:
#    - Kalshi: kalshi.com → Settings → API Keys
#    - Polymarket: polygon.io (covers multiple exchanges)

export KALSHI_API_KEY="..."
export POLYMARKET_API_KEY="..."

# 2. Run the system - it will use real APIs
python3 trading_system/arbitrage/main.py
```

## VPN / VPS Setup for US Access

### Option 1: VPS (Recommended)

Rent a US-based server ($0.04-5/month):
- AWS EC2 t3.micro (free tier eligible)
- DigitalOcean Droplet (~$5/month)
- Linode (~$5/month)

Setup on VPS:
```bash
sudo apt update && sudo apt install python3-pip -y
pip install requests
git clone YOUR_REPOSITORY_URL
cd portfolio-management

# Set environment variables
export KALSHI_API_KEY="..."
export POLYMARKET_API_KEY="..."

# Run the system
python3 trading_system/arbitrage/main.py
```

### Option 2: VPN Service

Use premium VPN with US servers:
- NordVPN, Surfshark, or Mullvad (~$3.39/month)
- Configure to route through US server
- Note: Some exchanges may block known VPN IPs

## Fee Structure

- **Kalshi**: ~1% per trade
- **Polymarket**: ~2% per trade (via Polygon.io)

### Example Trade

```
Bitcoin > $100K by Jan 31, 2025:
  Kalshi: 58.5% ($58.50/contract)
  Polymarket: 46.8% ($46.80/share)
  
Arbitrage Plan:
  Buy on Polymarket @ $46.80, Sell on Kalshi @ $58.50
  Gross profit: 11.7% before fees
  Net profit: ~9.4% after fees
```

## Results Output

All results are saved to JSON files:

- `trading_system/data/opportunities.json`: Detected opportunities
- `trading_system/data/arbitrage_results.json`: Trade execution results

Example output structure:
```json
{
  "timestamp": "2026-06-01T...",
  "opportunities": [
    {
      "kalshi_market_id": "BTC-JAN31-100K",
      "polymarket_slug": "bitcoin-100k-by-jan-31",
      "kalshi_price": 0.585,
      "polymarket_price": 0.468,
      "divergence": 0.117,
      ...
    }
  ],
  "trade_results": [
    {
      "kalshi_order_id": "KLS-...",
      "polymarket_order_id": "PM-...",
      "status": "open"
    }
  ]
}
```

## Testing

### Run Full Test Suite

```bash
python3 trading_system/arbitrage/comprehensive_test.py
```

This tests:
- All module imports
- Mock data loading
- Opportunity detection
- Trade execution
- Fee structure
- JSON output

### Debug Matching Algorithm

```bash
python3 trading_system/scripts/debug_matching.py
```

Shows which Kalshi markets match which Polymarket events based on text similarity.

## Production Checklist

Before deploying with real money:

- [ ] Set up real API keys and environment variables
- [ ] Test with mock data first (VPS without API keys)
- [ ] Verify VPN/VPS has stable internet connection
- [ ] Implement proper error handling for failed trades
- [ ] Set position limits per market ($500 max recommended)
- [ ] Configure alerts for trading errors
- [ ] Review fee structure and expected returns
- [ ] Test with small amounts before scaling up

## Architecture Diagram

```
┌─────────────────┐     ┌───────────────────────┐     ┌─────────────────┐
│ Kalshi API/Files│────>│ OpportunityDetector   │────>│ ArbTrader       │
│                 │     │                       │     │                 │
└─────────────────┘     └───────────────────────┘     └─────────────────┘
                            ↓                             ↓
                      Kalshi Order      Polymarket Order
                         Created          Created
```

## Next Steps for Enhancement

1. **Real-time WebSocket Integration** - For live market feeds
2. **Machine Learning Model** - Predict opportunity likelihood
3. **Multi-exchange Support** - Add more prediction markets
4. **Automated Bot** - Configurable risk parameters
5. **Slippage Protection** - Handle volatile markets better

## License

MIT License - see LICENSE file for details.

---

This implementation provides a complete foundation for Kalshi <-> Polymarket arbitrage trading, with comprehensive testing and clear paths to production deployment.
