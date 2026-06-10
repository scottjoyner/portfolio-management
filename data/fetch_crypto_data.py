#!/usr/bin/env python3
"""
Historical Crypto Data Fetcher - Coinbase API Integration

Fetches historical OHLCV data for major cryptocurrencies from Coinbase.
Stores data in CSV format for backtesting purposes.
"""

import json
import csv
from datetime import datetime, timedelta
from collections import defaultdict

def fetch_historical_data():
    """
    Fetch historical OHLCV data for major crypto assets.
    Coinbase API returns 100 candles per request (max allowed).
    """
    
    # Assets to fetch - major cryptos with good liquidity
    assets = [
        'BTC-USD',
        'ETH-USD',
        'SOL-USD',
        'ADA-USD',
        'DOT-USD',
        'MATIC-USD',
        'AVAX-USD',
        'LINK-USD',
    ]
    
    # Time range: last 2 years (approx 730 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)
    
    data_dir = '/home/scott/git/portfolio-management/data'
    
    for asset in assets:
        print(f"Fetching {asset}...")
        
        # Coinbase API endpoint
        url = f'https://api.coinbase.com/v2/prices/{asset}/history'
        params = {
            'start': start_date.strftime('%Y-%m-%d'),
            'end': end_date.strftime('%Y-%m-%d')
        }
        
        try:
            import urllib.request
            with urllib.request.urlopen(url, params) as response:
                data = json.loads(response.read())
                
                # Parse candlestick data
                candles = []
                for item in data.get('data', []):
                    candle = {
                        'timestamp': item['time'],
                        'open': float(item['high']),  # Approximation
                        'high': float(item['high']),
                        'low': float(item['low']),
                        'close': float(item['low']),   # Approximation
                    }
                    candles.append(candle)
                
                # Save to CSV
                csv_path = f'{data_dir}/{asset.replace("-USD", "")}_ohlc.csv'
                with open(csv_path, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=['timestamp', 'open', 'high', 'low', 'close'])
                    writer.writeheader()
                    writer.writerows(candles)
                
                print(f"  Saved {len(candles)} candles to {csv_path}")
                
        except Exception as e:
            print(f"  Error fetching {asset}: {e}")

if __name__ == '__main__':
    fetch_historical_data()
