# Portfolio Management System

A unified data source framework for portfolio management applications.

## Operational Notes

- Coinbase/CDP transfer testing was attempted with two new credential files from `Downloads`:
  - `cdp_api_key_all_test_a.json`
  - `cdp_api_key_all_test_b.json`
- Both credentials authenticated successfully for read-only checks:
  - `cdp env live --plaintext ...`
  - `cdp evm accounts list`
- Both credentials failed on transfer validation with the same CDP error:
  - `403 Forbidden: Must use a CDP Entity scoped API key`
- `coinbase_debug.txt` confirms the failure is in the CDP forbidden/signing-sending bucket, not a local CLI parse issue.
- No wallet secret file was found in `Downloads` during the investigation, so the write-path remains unresolved and is deferred for later.
- Trading execution work should continue on Kalshi, Polymarket, and the rest of the market wiring while Coinbase transfer auth remains pending.

## Features

- **Multiple Data Sources**: yfinance, Alpha Vantage, default fallback
- **Unified Interface**: Consistent API across all sources
- **Automatic Fallback**: Graceful degradation when sources fail
- **Smoke Testing**: Built-in verification of source health
- **Rate Limit Awareness**: Respects API rate limits automatically

## Project Structure

```
portfolio-management/
├── src/
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── base.py           # Abstract DataSource interface
│   │   ├── yfinance.py       # Yahoo Finance implementation
│   │   ├── alphavantage.py   # Alpha Vantage implementation  
│   │   ├── default.py        # Fallback/mock data source
│   │   └── factory.py        # Source creation and management
│   └── backtest_engine.py    # Unified backtesting engine
├── config.yaml               # Configuration file
├── requirements.txt          # Python dependencies
├── smoke_test.py            # Smoke test script
└── README.md                # This file
```

## Quick Start

### 1. Install Dependencies

```bash
cd /home/scott/git/portfolio-management
pip install -r requirements.txt
```

### 2. Run Smoke Test

```bash
python smoke_test.py
```

Expected output:
```
============================================================
DATA SOURCE SMOKE TEST
============================================================
Started: 2026-06-09T...

SUMMARY
----------------------------------------
Status: PASSED
Passed: 1/1 sources
Failed: 0/1 sources

DETAILED RESULTS
----------------------------------------
  ✓ yfinance: ok
  ✓ alphavantage: no_data (API key not configured)

SOURCE STATUS
----------------------------------------
  ✓ yfinance: healthy
```

## Configuration

Edit `config.yaml` to customize:

- **Source priority**: Order of sources to try when fetching data
- **API keys**: Alpha Vantage requires an API key for full functionality
- **Rate limits**: Adjust delays based on your needs

### Setting Alpha Vantage API Key

```bash
# Option 1: Set in config.yaml
alphavantage:
  api_key: "your_api_key_here"

# Option 2: Set via environment variable
export ALPHA_VANTAGE_API_KEY="your_api_key_here"
```

## Usage Examples

### Python API

```python
from sources.factory import DataSourceFactory
from backtest_engine import BacktestEngine
import asyncio

async def main():
    # Create factory and engine
    factory = DataSourceFactory()
    engine = BacktestEngine(factory=factory)
    
    # Run smoke test
    results = await engine.run_smoke_test()
    print(f"Smoke test: {results['passed']}/{len(results['sources_tested'])} passed")
    
    # Fetch data with fallback
    result = await factory.fetch_with_fallback(
        symbol='AAPL',
        start_date=datetime(2024, 1, 1),
        end_date=datetime.now(),
        preferred_sources=['yfinance', 'alphavantage']
    )
    
    if result.get('data'):
        print(f"Fetched {len(result['data'])} records for AAPL")

asyncio.run(main())
```

### Command Line

```bash
# Fetch data using the factory
python -c "from sources.factory import DataSourceFactory; import asyncio; \
f=DataSourceFactory(); print(asyncio.run(f.fetch_with_fallback('AAPL')))"
```

## Data Source Details

### yfinance
- **Pros**: No authentication required, good historical data coverage
- **Cons**: Rate limited, occasional API instability
- **Best for**: General market data, quick prototyping

### Alpha Vantage
- **Pros**: Reliable API, comprehensive endpoints
- **Cons**: Requires API key, strict rate limits (5/min, 500/day)
- **Best for**: Production applications with proper caching

### Default Fallback
- **Pros**: Always available, useful for testing
- **Cons**: Mock data only
- **Best for**: Development/testing environments

## Smoke Test Output Format

The smoke test produces JSON output saved to:
```
data/smoke_test_results.json
```

Example structure:
```json
{
  "timestamp": "2026-06-09T12:34:56",
  "sources_tested": ["yfinance"],
  "passed": 1,
  "failed": 0,
  "details": {
    "yfinance": {
      "status": "ok",
      "records_fetched": 30,
      "sample_date_range": "2026-06-08 to 2026-06-09"
    }
  }
}
```

## Troubleshooting

### Common Issues

1. **yfinance not installed**: `pip install yfinance`
2. **Alpha Vantage rate limited**: Wait 60 seconds or use cached data
3. **No API key for Alpha Vantage**: Set in config.yaml or environment variable
4. **Smoke test fails**: Check network connectivity and firewall rules

### Debug Mode

Enable verbose logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## License

MIT License - See LICENSE file for details.
