#!/usr/bin/env python3
"""
Unified Smoke Test for Portfolio Data Sources
Tests yfinance and Alpha Vantage (with graceful fallback)
"""
import json
import os
from datetime import datetime, timedelta

def test_yfinance():
    """Test yfinance data fetching"""
    print("\n=== Testing YFinance ===")
    
    try:
        import yfinance as yf
        
        # Test with a single ticker
        ticker = yf.Ticker('AAPL')
        hist = ticker.history(period='1d', start='2025-06-09', end='2025-07-09')
        
        if not hist.empty:
            print(f"✓ YFinance: Retrieved {len(hist)} rows of data")
            print(f"  Date range: {hist.index.min()} to {hist.index.max()}")
            return True
        else:
            print("✗ YFinance: No data returned (rate limited or unavailable)")
            return False
    except Exception as e:
        print(f"✗ YFinance Error: {e}")
        return False

def test_alpha_vantage():
    """Test Alpha Vantage with graceful fallback"""
    print("\n=== Testing Alpha Vantage ===")
    
    try:
        import requests
        from hermes_tools import json_parse
        
        # Check if API key is configured
        config_path = os.path.expanduser('~/git/portfolio-management/config.yaml')
        with open(config_path) as f:
            content = f.read()
        
        api_key = ''
        for line in content.split('\n'):
            if 'api_key:' in line and 'alphavantage' not in line.lower():
                parts = line.strip().split('=')
                if len(parts) > 1:
                    api_key = parts[1].strip().strip('"\'')
        
        if not api_key:
            print("⚠ Alpha Vantage: No API key configured (free tier requires claim at alphavantage.co)")
            return False
        
        # Test with compact output to save rate limit
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey={api_key}&outputsize=compact"
        r = requests.get(url, timeout=10)
        
        if r.status_code == 200:
            data = json_parse(r.text)
            if 'Meta Data' in data and 'Downloaded' in data['Meta Data']:
                print(f"✓ Alpha Vantage: API responded successfully")
                print(f"  Downloaded: {data['Meta Data'].get('Downloaded', 'N/A')}")
                return True
            else:
                print(f"✗ Alpha Vantage: Invalid response format")
                return False
        else:
            print(f"✗ Alpha Vantage: HTTP {r.status_code}")
            return False
    except Exception as e:
        print(f"✗ Alpha Vantage Error: {e}")
        return False

def test_unified_fetcher():
    """Test unified data fetching with source priority"""
    print("\n=== Testing Unified Fetcher ===")
    
    try:
        import yfinance as yf
        from hermes_tools import json_parse
        
        # Test list of tickers
        tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']
        results = {}
        
        for symbol in tickers:
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period='1d')
                if not hist.empty:
                    min_price = hist['Close'].min()
                    max_price = hist['Close'].max()
                    results[symbol] = {
                        'source': 'yfinance',
                        'rows': len(hist),
                        'price_range': f"${min_price:.2f}-${max_price:.2f}"
                    }
                else:
                    results[symbol] = {'error': 'no data'}
            except Exception as e:
                results[symbol] = {'error': str(e)}
        
        print(f"✓ Unified fetcher: Processed {len(tickers)} tickers")
        for symbol, result in results.items():
            status = "OK" if 'source' in result else f"ERROR: {result.get('error', 'N/A')}"
            print(f"  {symbol}: {status}")
        
        return all('source' in v for v in results.values())
    except Exception as e:
        print(f"✗ Unified fetcher Error: {e}")
        return False

def main():
    """Run all smoke tests"""
    print("="*60)
    print("PORTFOLIO DATA SOURCE SMOKE TEST")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("="*60)
    
    results = {
        'yfinance': test_yfinance(),
        'alpha_vantage': test_alpha_vantage(),
        'unified_fetcher': test_unified_fetcher()
    }
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    for name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {name}: {status}")
    
    total = sum(1 for p in results.values() if p)
    print(f"\nOverall: {total}/{len(results)} sources operational")
    
    # Save test report
    report_path = os.path.expanduser('~/git/portfolio-management/data/smoke_test_report.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'results': results,
            'summary': {
                'operational_sources': sum(1 for p in results.values() if p),
                'total_sources': len(results)
            }
        }, f, indent=2)
    print(f"\nReport saved to: {report_path}")

if __name__ == '__main__':
    main()
