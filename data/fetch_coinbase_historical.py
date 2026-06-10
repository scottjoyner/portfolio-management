#!/usr/bin/env python3
"""
Coinbase Historical Data Fetcher - Standard Library Only

Fetches historical OHLCV data from Coinbase Advanced Trade API for backtesting.
Uses only Python standard library (no external dependencies required).
"""

import json
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.parse import urlencode
import csv
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/home/scott/git/portfolio-management/data/fetch_coinbase.log')
    ]
)
logger = logging.getLogger(__name__)


class CoinbaseDataFetcher:
    """
    Fetches historical candle data from Coinbase Advanced Trade API.
    Uses only standard library modules for maximum compatibility.
    """
    
    # Granularity mappings to approximate time windows for safe pagination
    GRANULARITY_DELTA = {
        'ONE_MINUTE': timedelta(minutes=300),
        'FIVE_MINUTE': timedelta(minutes=1500),
        'FIFTEEN_MINUTE': timedelta(hours=75),
        'THIRTY_MINUTE': timedelta(hours=150),
        'ONE_HOUR': timedelta(days=12),
        'TWO_HOUR': timedelta(days=24),
        'FOUR_HOUR': timedelta(days=50),
        'SIX_HOUR': timedelta(days=75),
        'ONE_DAY': timedelta(days=365),  # Coinbase's max cache window
    }
    
    def __init__(self, base_url="https://api.exchange.coinbase.com", rate_limit_delay=0.5):
        """
        Initialize the fetcher.
        
        Args:
            base_url: Base API URL (default uses Coinbase Advanced Trade v2)
            rate_limit_delay: Seconds to wait between requests
        """
        self.base_url = base_url.rstrip('/')
        self.rate_limit_delay = rate_limit_delay
    
    def fetch_candles(self, product_id, granularity,
                      start_date, end_date,
                      max_retries=3):
        """
        Fetch historical candles with pagination.
        
        Args:
            product_id: Trading pair (e.g., 'BTC-USD', 'ETH-USDC')
            granularity: Candle interval ('ONE_HOUR', 'ONE_DAY', etc.)
            start_date: Start datetime for data fetch
            end_date: End datetime for data fetch
            max_retries: Maximum retry attempts per request
        
        Returns:
            List of candle dictionaries (compatible with pandas DataFrame)
        """
        # Calculate step size for pagination
        step_delta = self.GRANULARITY_DELTA.get(granularity, timedelta(days=1))
        current_start = start_date
        all_candles = []
        
        while current_start < end_date:
            # Don't fetch beyond the cache window (Coinbase ~365 days for daily)
            max_fetch_window = 365 if granularity == 'ONE_DAY' else 120
            current_end = min(
                current_start + step_delta,
                end_date,
                current_start + timedelta(days=max_fetch_window)
            )
            
            # Convert to UNIX timestamps
            params = {
                "start": int(current_start.timestamp()),
                "end": int(current_end.timestamp()),
                "granularity": granularity
            }
            
            logger.info(
                f"Fetching {product_id} [{granularity}] "
                f"{current_start.date()} to {current_end.date()}"
            )
            
            # Make request with retry logic
            success = False
            for attempt in range(max_retries):
                try:
                    url = self.base_url + "/products/" + product_id + "/candles?" + urlencode(params)
                    req = Request(url, headers={
                        'User-Agent': 'HermesPortfolio/1.0 (Backtesting)',
                        'Accept': 'application/json',
                        'Cache-Control': 'no-cache'
                    })
                    
                    with urlopen(req, timeout=30) as response:
                        data = json.loads(response.read().decode('utf-8'))
                        candles = data.get('candles', [])
                        
                        if candles:
                            all_candles.extend(candles)
                            logger.info(
                                f"Fetched {len(candles)} candles "
                                f"({current_start.date()} -> {min(c['start'] for c in candles).date()})"
                            )
                        else:
                            logger.warning("No candles returned from API")
                    
                    if response.status_code == 429:  # Rate limited
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning(
                            f"Rate limited. Waiting {retry_after}s..."
                        )
                        time.sleep(retry_after)
                        continue
                    
                    else:
                        error_msg = response.read().decode('utf-8')[:200] if response.read() else "No details"
                        logger.error(f"API Error [{response.status_code}]: {error_msg}")
                        break
                    
                    success = True
                    break
                    
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = min(2 ** attempt * 2, 30)  # Exponential backoff
                        logger.warning(f"Request failed (attempt {attempt+1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed after {max_retries} attempts: {e}")
            
            if not success and current_start < end_date:
                # If we failed mid-fetch, try to continue from where we left off
                logger.warning("Continuing fetch from last successful point...")
                break  # Exit loop on failure
            
            # Move to next window
            current_start = current_end
            
            # Rate limiting between requests
            if current_start < end_date:
                time.sleep(self.rate_limit_delay)
        
        if not all_candles:
            logger.error("No data fetched. Returning empty list.")
            return []
        
        logger.info(
            f"Total: {len(all_candles)} candles for {product_id} "
            f"({min(c['start'] for c in all_candles).date()} -> {max(c['start'] for c in all_candles).date()})"
        )
        
        return all_candles
    
    def fetch_multiple(self, assets_config):
        """
        Fetch data for multiple assets.
        
        Args:
            assets_config: List of dicts with keys:
                - product_id: Trading pair (e.g., 'BTC-USD')
                - granularity: Candle interval
                - start_date: Start datetime
                - end_date: End datetime
        
        Returns:
            Dictionary mapping product_id to list of candles
        """
        results = {}
        total_start = datetime.now()
        
        for i, config in enumerate(assets_config):
            asset_name = config.get('product_id', 'Unknown')
            progress = f"[{i+1}/{len(assets_config)}] {asset_name}"
            print(progress)
            logger.info(progress)
            
            try:
                candles = self.fetch_candles(
                    product_id=config['product_id'],
                    granularity=config.get('granularity', 'ONE_HOUR'),
                    start_date=config.get('start_date', datetime.now() - timedelta(days=365)),
                    end_date=config.get('end_date', datetime.now())
                )
                
                if candles:
                    results[asset_name] = candles
                    # Save to CSV
                    csv_path = os.path.join(config.get('output_dir', 'data'), f"{config['product_id']}_backtest.csv")
                    with open(csv_path, 'w', newline='') as f:
                        fieldnames = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        for candle in candles:
                            row = {
                                'timestamp': candle['start'],
                                'open': candle['open'],
                                'high': candle['high'],
                                'low': candle['low'],
                                'close': candle['close'],
                                'volume': candle['volume']
                            }
                            writer.writerow(row)
                    logger.info(f"  Saved: {csv_path}")
                else:
                    results[asset_name] = []
                    logger.warning(f"  No data for {asset_name}")
                    
            except Exception as e:
                logger.error(f"Error fetching {asset_name}: {e}")
                results[asset_name] = []
        
        total_time = (datetime.now() - total_start).total_seconds()
        print("\n" + "="*60)
        print(f"Fetch complete in {total_time:.1f}s")
        print(f"Assets with data: {sum(1 for v in results.values() if v)}/{len(results)}")
        print("="*60)
        
        return results
    
    def close(self):
        """Close session and release resources."""
        pass  # No session to close with urllib


def main():
    """
    Main execution function with default configuration.
    """
    print("="*60)
    print("Coinbase Historical Data Fetcher")
    print("Standard Library Only - No External Dependencies")
    print("="*60)
    
    # Default backtest window
    BACKTEST_START = datetime(2024, 1, 1)
    BACKTEST_END = datetime.now()
    
    # Assets to fetch with configuration
    ASSETS_CONFIG = [
        {'product_id': 'BTC-USD', 'granularity': 'ONE_HOUR'},
        {'product_id': 'ETH-USD', 'granularity': 'ONE_HOUR'},
        {'product_id': 'SOL-USD', 'granularity': 'ONE_HOUR'},
        {'product_id': 'ADA-USD', 'granularity': 'ONE_DAY'},
        {'product_id': 'DOT-USD', 'granularity': 'ONE_DAY'},
        {'product_id': 'MATIC-USD', 'granularity': 'ONE_DAY'},
        {'product_id': 'AVAX-USD', 'granularity': 'ONE_HOUR'},
        {'product_id': 'LINK-USD', 'granularity': 'ONE_HOUR'},
    ]
    
    # Initialize fetcher
    fetcher = CoinbaseDataFetcher(
        rate_limit_delay=0.5  # Be polite to the API
    )
    
    try:
        # Fetch all data
        results = fetcher.fetch_multiple(ASSETS_CONFIG)
        
        # Print summary
        print("\n" + "="*60)
        print("FETCH SUMMARY")
        print("="*60)
        for asset, candles in sorted(results.items()):
            if candles:
                timestamps = [c['start'] for c in candles]
                start_date = min(timestamps).date()
                end_date = max(timestamps).date()
                low = min(c['low'] for c in candles)
                high = max(c['high'] for c in candles)
                print(f"{asset:12} | {len(candles):6} candles | "
                      f"${low:.2f} -> ${high:.2f}")
            else:
                print(f"{asset:12} | NO DATA")
        
        # Save session metadata
        metadata = {
            'fetch_time': datetime.now().isoformat(),
            'assets_configured': ASSETS_CONFIG,
            'results_summary': {k: len(v) if v else 0 for k, v in results.items()}
        }
        
        with open('data/fetch_metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"\nMetadata saved to data/fetch_metadata.json")
        
    finally:
        fetcher.close()


if __name__ == "__main__":
    main()
