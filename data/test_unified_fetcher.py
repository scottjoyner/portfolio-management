#!/usr/bin/env python3
"""
Smoke Test Script for Unified Market Data Fetcher

Tests all data sources (yfinance, Alpha Vantage, Coinbase) to verify they work correctly.
Run this script before integrating into the backtesting framework.

Usage:
    cd /home/scott/git/portfolio-management
    source .venv/bin/activate  
    python3 data/test_unified_fetcher.py
    
Or with environment variables for Alpha Vantage API key:
    export ALPHA_VANTAGE_API_KEY=***
    python3 data/test_unified_fetcher.py
"""

import sys
sys.path.insert(0, '/home/scott/git/portfolio-management/.venv/lib')
sys.path.insert(1, '/home/scott/git/portfolio-management/data')

from data.fetch_unified import UnifiedMarketDataAdapter
import datetime as dt


def test_stock_prices():
    """Test stock price retrieval from yfinance (primary) or Alpha Vantage (fallback)."""
    
    print("=" * 60)
    print("Testing Stock Price Retrieval")  
    print("=" * 60)
    
    fetcher = UnifiedMarketDataAdapter()
    
    for ticker in ['AAPL', 'MSFT']:
        start_date = dt.datetime(2024, 12, 31)
        end_date = dt.datetime.now()
        
        print(f"\nFetching {ticker} history from {start_date.date()} to {end_date.date()}...")
        
        prices = fetcher.fetch_historical_data(ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        if prices:
            print(f"  ✅ Retrieved {len(prices)} price records")  
            print(f"     First record: {prices[0]['timestamp'].date()} - ${prices[0]['close']:.2f}")
            print(f"     Last record: {prices[-1]['timestamp'].date()} - ${prices[-1]['close']:.2f}")
        else:
            print("  ❌ No historical data available")


def test_etf_prices():
    """Test ETF price retrieval from yfinance (primary) or Alpha Vantage (fallback)."""
    
    print("\n" + "=" * 60)  
    print("Testing ETF Price Retrieval")
    print("=" * 60)
    
    fetcher = UnifiedMarketDataAdapter()
    
    for etf in ['SPY', 'QQQ']:
        start_date = dt.datetime(2024, 12, 31)
        end_date = dt.datetime.now()
        
        print(f"\nFetching {etf} history from {start_date.date()} to {end_date.date()}...")
        
        prices = fetcher.fetch_historical_data(etf, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        if prices:
            print(f"  ✅ Retrieved {len(prices)} ETF records")  
            print(f"     First record: {prices[0]['timestamp'].date()} - ${prices[0]['close']:.2f}")
            print(f"     Last record: {prices[-1]['timestamp'].date()} - ${prices[-1]['close']:.2f}")
        else:
            print("  ❌ No ETF data available")


def test_crypto_prices():
    """Test crypto price retrieval from Coinbase Advanced Trade API."""
    
    print("\n" + "=" * 60)  
    print("Testing Crypto Price Retrieval (Coinbase)")
    print("=" * 60)
    
    fetcher = UnifiedMarketDataAdapter()
    
    for symbol in ['BTC-USD', 'ETH-USD']:
        start_date = dt.datetime(2024, 12, 31)
        end_date = dt.datetime.now()
        
        print(f"\nFetching {symbol} history from {start_date.date()} to {end_date.date()}...")
        
        prices = fetcher.fetch_historical_data(symbol, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        if prices:
            print(f"  ✅ Retrieved {len(prices)} price records")  
            print(f"     First record: {prices[0]['timestamp'].date()} - ${prices[0]['close']:.2f}")
            print(f"     Last record: {prices[-1]['timestamp'].date()} - ${prices[-1]['close']:.2f}")
        else:
            print("  ❌ No crypto data available")


def main():
    """Run all smoke tests."""
    
    print("=" * 60)  
    print("Unified Market Data Fetcher Smoke Test Suite")
    print("=" * 60)
    
    # Run each test suite
    try:
        test_stock_prices()
    except Exception as e:
        import traceback
        print(f"\n❌ Stock price test failed:")
        traceback.print_exc()
    
    try:  
        test_etf_prices()
    except Exception as e:
        import traceback
        print(f"\n❌ ETF price test failed:")
        traceback.print_exc()
    
    try:
        test_crypto_prices()
    except Exception as e:
        import traceback
        print(f"\n❌ Crypto price test failed:")  
        traceback.print_exc()
    
    # Print summary
    print("\n" + "=" * 60)  
    print("Smoke Test Suite Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
