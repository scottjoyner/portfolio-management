#!/usr/bin/env python3
"""Test CLI-based historical data fetching"""

import subprocess
import json
import sys
sys.path.insert(0, '/home/scott/git/portfolio-management')


def test_cli_direct():
    """Test direct CLI invocation for BTC-USD candles."""
    print("=" * 60)
    print("TEST 1: Direct CLI - BTC-USD candles")
    print("=" * 60)
    
    result = subprocess.run(
        ["/home/scott/.npm-global/bin/coinbase", "products", "candles", "BTC-USD", 
         "granularity==1h", "limit==5"],
        capture_output=True, text=True, timeout=30,
        env={**dict(__import__("os").environ), "PATH": "/home/scott/.npm-global/bin:" + __import__("os").environ.get("PATH", "")}
    )
    
    if result.returncode == 0:
        print(f"✅ CLI successful - {len(result.stdout.splitlines())} lines output")
        try:
            data = json.loads(result.stdout)
            if isinstance(data, list):
                print(f"📊 Got {len(data)} candles directly in array format")
            elif "candles" in data:
                print(f"📊 Got wrapped response with {len(data['candles'])} candles")
                print("\nSample candle structure:")
                if data["candles"]:
                    sample = data["candles"][0]
                    for k, v in list(sample.items())[:5]:
                        print(f"   {k}: {v}")
        except json.JSONDecodeError:
            print(f"Raw output preview:\n{result.stdout[:500]}")
    else:
        print(f"❌ CLI failed: {result.stderr or result.stdout.strip()}")


def test_fetcher():
    """Test the new CoinbaseHistoryFetcher."""
    print("\n" + "=" * 60)
    print("TEST 2: CoinbaseHistoryFetcher - BTC-USD and ETH-USD")
    print("=" * 60)
    
    from trading_system.data.coinbase_history import (
        CoinbaseHistoryFetcher, FetchConfig
    )
    
    # Create fetcher with live environment
    config = FetchConfig(environment='live')
    fetcher = CoinbaseHistoryFetcher(config=config)
    
    for product in ["BTC-USD", "ETH-USD"]:
        print(f"\n--- Fetching {product} ---")
        
        result = fetcher.fetch_candles(
            product_id=product, 
            granularity="1h", 
            days_back=7,
            limit=50
        )
        
        print(f"Success: {result.success}")
        print(f"Candles fetched: {len(result.candles)}")
        if result.error:
            print(f"Error: {result.error}")
        if result.cached:
            print("📁 Used cache")
        
        if result.candles and len(result.candles) > 0:
            c = result.candles[0]
            print(f"\nSample candle:")
            print(f"  ts={c.ts} ({c.ts // 86400})")
            print(f"  open=${c.open:.2f}, high=${c.high:.2f}")
            print(f"  low=${c.low:.2f}, close=${c.close:.2f}")


if __name__ == "__main__":
    test_cli_direct()
    test_fetcher()
