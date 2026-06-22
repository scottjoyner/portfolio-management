#!/usr/bin/env python3
"""
Multi-Source Historical Data Fetcher for Backtesting

Fetches OHLCV data from multiple sources:
- Coinbase Advanced Trade API (crypto)
- yfinance (stocks, ETFs)
- Alpha Vantage (free tier available)
- Finnhub (requires API key)

Uses only Python standard library where possible.
"""

import json
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.parse import urlencode
import csv
import logging
import os
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).resolve().parent / 'fetch_multi_source.log'))
    ]
)
logger = logging.getLogger(__name__)


class MultiSourceDataFetcher:
    """
    Unified data fetcher with multiple source support.
    """
    
    # Coinbase Advanced Trade v2 API - correct granularity values
    COINBASE_GRANULARITY_MAP = {
        '1m': 'minute',
        '5m': 'minute',  # Use minute for short intervals
        '15m': 'minute',
        '30m': 'minute',
        '1h': 'hour',
        '4h': 'hour',
        '6h': 'hour',
        '8h': 'hour',
        '12h': 'hour',
        '1d': 'day',
    }
    
    def __init__(self, coinbase_base_url="https://api.exchange.coinbase.com"):
        self.coinbase_base_url = coinbase_base_url.rstrip('/')
    
    def fetch_coinbase(self, product_id, granularity='hour',
                       start_date=None, end_date=None,
                       max_retries=3):
        """
        Fetch from Coinbase Advanced Trade API.
        
        Args:
            product_id: Trading pair (e.g., 'BTC-USD')
            granularity: 'minute', 'hour', or 'day'
            start_date: Start datetime
            end_date: End datetime
            max_retries: Maximum retry attempts
        """
        if start_date is None:
            start_date = datetime.now() - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now()
        
        # Coinbase public candle endpoint expects granularity in seconds.
        granularity_sec = {
            'minute': 60,
            'hour': 3600,
            'day': 86400,
        }.get(granularity, 3600)

        # Calculate step size for pagination (max 300 candles per request)
        if granularity == 'minute':
            step_delta = timedelta(minutes=12)  # ~720 minutes
        elif granularity == 'hour':
            step_delta = timedelta(days=1)  # ~24 hours
        else:  # day
            step_delta = timedelta(days=30)  # ~30 days
        
        current_start = start_date
        all_candles = []
        
        while current_start < end_date:
            max_fetch_window = 365 if granularity == 'day' else 120
            current_end = min(
                current_start + step_delta,
                end_date,
                current_start + timedelta(days=max_fetch_window)
            )
            
            # Convert to UNIX timestamps
            params = {
                "start": int(current_start.timestamp()),
                "end": int(current_end.timestamp()),
                "granularity": granularity_sec
            }
            
            logger.info(
                f"Fetching {product_id} [{granularity}] "
                f"{current_start.date()} to {current_end.date()}"
            )
            
            success = False
            for attempt in range(max_retries):
                try:
                    url = self.coinbase_base_url + "/products/" + product_id + "/candles?" + urlencode(params)
                    req = Request(url, headers={
                        'User-Agent': 'HermesPortfolio/1.0 (Backtesting)',
                        'Accept': 'application/json',
                        'Cache-Control': 'no-cache'
                    })
                    
                    with urlopen(req, timeout=30) as response:
                        body = response.read().decode('utf-8')
                        data = json.loads(body)
                        candles = data.get('candles', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

                        normalized = []
                        for candle in candles:
                            try:
                                if isinstance(candle, dict):
                                    ts = int(candle.get('start', candle.get('time', 0)))
                                    normalized.append({
                                        'start': ts,
                                        'open': float(candle.get('open', 0)),
                                        'high': float(candle.get('high', 0)),
                                        'low': float(candle.get('low', 0)),
                                        'close': float(candle.get('close', 0)),
                                        'volume': float(candle.get('volume', 0)),
                                    })
                                elif isinstance(candle, (list, tuple)) and len(candle) >= 6:
                                    ts, low, high, open_, close, volume = candle[:6]
                                    normalized.append({
                                        'start': int(ts),
                                        'open': float(open_),
                                        'high': float(high),
                                        'low': float(low),
                                        'close': float(close),
                                        'volume': float(volume),
                                    })
                            except Exception:
                                continue

                        if normalized:
                            all_candles.extend(normalized)
                            first_ts = min(c['start'] for c in normalized)
                            logger.info(
                                f"Fetched {len(normalized)} candles "
                                f"({current_start.date()} -> {datetime.fromtimestamp(first_ts).date()})"
                            )
                        else:
                            logger.warning("No candles returned from API")
                    
                    success = True
                    break
                    
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = min(2 ** attempt * 2, 30)
                        logger.warning(f"Request failed (attempt {attempt+1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed after {max_retries} attempts: {e}")
            
            if not success and current_start < end_date:
                break
            
            current_start = current_end
            
            # Rate limiting between requests
            if current_start < end_date:
                time.sleep(0.5)
        
        if not all_candles:
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
                - source: 'coinbase', 'yfinance', etc.
                - product_id: Trading pair
                - granularity: Candle interval
                - start_date: Start datetime
                - end_date: End datetime
        """
        results = {}
        total_start = datetime.now()
        
        for i, config in enumerate(assets_config):
            asset_name = config.get('product_id', 'Unknown')
            source = config.get('source', 'coinbase')
            progress = f"[{i+1}/{len(assets_config)}] {asset_name} ({source})"
            print(progress)
            logger.info(progress)
            
            try:
                if source == 'coinbase':
                    candles = self.fetch_coinbase(
                        product_id=config['product_id'],
                        granularity=config.get('granularity', 'hour'),
                        start_date=config.get('start_date', datetime.now() - timedelta(days=365)),
                        end_date=config.get('end_date', datetime.now())
                    )
                else:
                    logger.warning(f"Source '{source}' not yet implemented")
                    candles = []
                
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
        pass  # No session to close


def main():
    """
    Main execution function with default configuration.
    """
    print("="*70)
    print("Multi-Source Historical Data Fetcher")
    print("Coinbase + yfinance + Alpha Vantage + Finnhub")
    print("="*70)
    
    # Default backtest window
    BACKTEST_START = datetime(2024, 1, 1)
    BACKTEST_END = datetime.now()
    
    # Assets to fetch with configuration
    ASSETS_CONFIG = [
        # Crypto - Coinbase Advanced Trade API
        {'source': 'coinbase', 'product_id': 'BTC-USD', 'granularity': 'hour'},
        {'source': 'coinbase', 'product_id': 'ETH-USD', 'granularity': 'hour'},
        {'source': 'coinbase', 'product_id': 'SOL-USD', 'granularity': 'hour'},
        # Stocks - yfinance (to be implemented)
        # {'source': 'yfinance', 'product_id': 'AAPL', 'granularity': 'day'},
        # ETFs
        {'source': 'coinbase', 'product_id': 'SPY-USD', 'granularity': 'hour'},
        {'source': 'coinbase', 'product_id': 'QQQ-USD', 'granularity': 'hour'},
    ]
    
    # Initialize fetcher
    fetcher = MultiSourceDataFetcher()
    
    try:
        # Fetch all data
        results = fetcher.fetch_multiple(ASSETS_CONFIG)
        
        # Print summary
        print("\n" + "="*70)
        print("FETCH SUMMARY")
        print("="*70)
        for asset, candles in sorted(results.items()):
            if candles:
                timestamps = [c['start'] for c in candles]
                start_date = min(timestamps).date()
                end_date = max(timestamps).date()
                low = min(c['low'] for c in candles)
                high = max(c['high'] for c in candles)
                print(f"{asset:15} | {len(candles):6} candles | "
                      f"${low:.2f} -> ${high:.2f}")
            else:
                print(f"{asset:15} | NO DATA")
        
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
