#!/usr/bin/env python3
import json
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError
import csv
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data/fetch_coinbase.log')
    ]
)
logger = logging.getLogger(__name__)

class CoinbaseDataFetcher:
    GRANULARITY_DELTA = {
        'ONE_MINUTE': timedelta(minutes=1),
        'FIVE_MINUTE': timedelta(minutes=5),
        'FIFTEEN_MINUTE': timedelta(minutes=15),
        'THIRTY_MINUTE': timedelta(minutes=30),
        'ONE_HOUR': timedelta(hours=1),
        'THREE_HOUR': timedelta(hours=3),
        'SIX_HOUR': timedelta(hours=6),
        'TWELVE_HOUR': timedelta(hours=12),
        'ONE_DAY': timedelta(days=1),
    }
    
    def __init__(self, base_url="https://api.exchange.coinbase.com", rate_limit_delay=0.5):
        self.base_url = base_url.rstrip('/')
        self.rate_limit_delay = rate_limit_delay
    
    def fetch_candles(self, product_id, granularity, start_date, end_date, max_retries=3):
        step_delta = self.GRANULARITY_DELTA.get(granularity, timedelta(days=1))
        current_start = start_date
        all_candles = []
        
        while current_start < end_date:
            max_fetch_window = 365 if granularity == 'ONE_DAY' else 120
            current_end = min(
                current_start + step_delta,
                end_date,
                current_start + timedelta(days=max_fetch_window)
            )
            
            params = {
                "start": int(current_start.timestamp()),
                "end": int(current_end.timestamp()),
                "granularity": int(step_delta.total_seconds())
            }
            
            logger.info(f"Fetching {product_id} [{granularity}] {current_start.date()} to {current_end.date()}")
            
            success = False
            for attempt in range(max_retries):
                try:
                    url_with_params = f"{self.base_url}/products/{product_id}/candles?{urlencode(params)}"
                    logger.info(f"Request URL: {url_with_params}")
                    req = Request(url_with_params, headers={
                        'User-Agent': 'HermesPortfolio/1.0 (Backtesting)',
                        'Accept': 'application/json'
                    })
                
                    with urlopen(req, timeout=30) as response:
                        data = json.loads(response.read().decode('utf-8'))
                        if isinstance(data, list):
                            candles = data
                            logger.info(f"Fetched {len(candles)} candles")
                        else:
                            candles = []
                            logger.warning("No candles returned from API (expected list)")
                        success = True
                        break
                except HTTPError as e:
                    if e.code == 429:
                        retry_after = int(e.headers.get('Retry-After', 60))
                        time.sleep(retry_after)
                    else:
                        logger.error(f"HTTP Error {e.code}")
                        break
                except Exception as e:
                    logger.error(f"Error: {e}")
                    time.sleep(2 ** attempt)
                
                if not success:
                    if attempt == max_retries - 1:
                        break
                    time.sleep(1)
                    success = True # Continue if we can't catch all errors
            
            if not success or current_start >= end_date:
                break
            current_start = current_end
            time.sleep(self.rate_limit_delay)
        
        return all_candles
    
    def fetch_multiple(self, assets_config):
        results = {}
        for config in assets_config:
            asset_name = config['product_id']
            print(f"Fetching {asset_name}...")
            try:
                candles = self.fetch_candles(
                    product_id=asset_name,
                    granularity=config.get('granularity', 'ONE_HOUR'),
                    start_date=datetime.now() - timedelta(days=30),
                    end_date=datetime.now()
                )
                if candles:
                    results[asset_name] = candles
                    csv_path = f"data/{asset_name}_backtest.csv"
                    with open(csv_path, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                        writer.writeheader()
                        for c in candles:
                            writer.writerow({'timestamp': c['start'], 'open': c['open'], 'high': c['high'], 'low': c['low'], 'close': c['close'], 'volume': c['volume']})
            except Exception as e:
                print(f"Error: {e}")
        return results

if __name__ == "__main__":
    ASSETS_CONFIG = [
        {'product_id': 'BTC-USD', 'granularity': 'ONE_HOUR'},
        {'product_id': 'ETH-USD', 'granularity': 'ONE_HOUR'},
    ]
    fetcher = CoinbaseDataFetcher()
    fetcher.fetch_multiple(ASSETS_CONFIG)
