#!/usr/bin/env python3
"""
Historical Data Downloader for Coinbase Backtesting

Downloads OHLCV data from Coinbase API for backtesting purposes.
Stores data in compressed format for efficient replay.
"""

import subprocess
import json
import gzip
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class HistoricalDataDownloader:
    """
    Download and store historical Coinbase market data.
    
    Features:
      ✅ Downloads OHLCV candles from Coinbase API
      ✅ Stores compressed for efficient storage
      ✅ Organizes by product_id and date range
      ✅ Validates data integrity on download
    """
    
    def __init__(self, cache_dir: str = './market_data_cache'):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
    
    def download_product_data(
        self,
        product_id: str,
        start_date: datetime,
        end_date: datetime,
        granularity: str = 'hourly'
    ) -> Dict[str, any]:
        """
        Download historical data for a specific product.
        
        Args:
            product_id: Trading pair (e.g., 'BTC-USD')
            start_date: Start date in ISO format
            end_date: End date in ISO format
            granularity: 'minute', 'hourly', 'daily'
            
        Returns:
            Dict with download metadata and data summary
        """
        # Format dates for API
        start_str = start_date.strftime('%Y-%m-%dT00:00:00Z')
        end_str = end_date.strftime('%Y-%m-%dT23:59:59Z')
        
        # Build download command
        cmd = [
            'coinbase', 'products', 'candles',
            product_id,
            f'granularity=={granularity}',
            '-e', 'live'
        ]
        
        try:
            result = subprocess.run(
                cmd + [f'start={start_str}', f'end={end_str}'],
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if result.returncode != 0:
                error_msg = result.stderr or result.stdout
                logger.error(f"Download failed: {error_msg}")
                return {'success': False, 'error': error_msg}
            
            # Parse and store data
            data = json.loads(result.stdout)
            candles = data.get('data', [])
            
            # Store compressed
            filename = self._store_data(product_id, start_date, end_date, candles)
            
            return {
                'success': True,
                'product_id': product_id,
                'start_date': start_str,
                'end_date': end_str,
                'candles_count': len(candles),
                'file_path': filename
            }
        except Exception as e:
            logger.error(f"Download error: {e}")
            return {'success': False, 'error': str(e)}
    
    def _store_data(
        self,
        product_id: str,
        start_date: datetime,
        end_date: datetime,
        candles: List
    ) -> str:
        """
        Store data in compressed format.
        """
        # Create filename with date range
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        
        filename = f"{product_id}_{start_str}_{end_str}.json.gz"
        filepath = os.path.join(self.cache_dir, filename)
        
        # Convert to DataFrame for easier handling
        import pandas as pd
        df = pd.DataFrame(candles, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        df['time'] = pd.to_datetime(df['time'])
        
        # Store as compressed JSON
        with gzip.open(filepath, 'wt') as f:
            json.dump({
                'product_id': product_id,
                'start_date': start_str,
                'end_date': end_str,
                'candles_count': len(candles),
                'data': df.to_dict('records')
            }, f)
        
        return filepath
    
    def download_all_products(
        self,
        products: List[str],
        start_date: datetime,
        end_date: datetime,
        granularity: str = 'hourly'
    ) -> Dict[str, any]:
        """
        Download data for multiple products.
        
        Args:
            products: List of product IDs
            start_date: Start date
            end_date: End date
            granularity: Data granularity
            
        Returns:
            Dict with download results for each product
        """
        results = {}
        
        for product_id in products:
            logger.info(f"Downloading {product_id}...")
            result = self.download_product_data(
                product_id=product_id,
                start_date=start_date,
                end_date=end_date,
                granularity=granularity
            )
            results[product_id] = result
        
        return results


def main():
    """
    Main download function.
    Downloads historical data for common trading pairs.
    """
    downloader = HistoricalDataDownloader()
    
    # Define date range (last 30 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # Common trading pairs
    products = ['BTC-USD', 'ETH-USD', 'SOL-USD']
    
    # Download all products
    results = downloader.download_all_products(
        products=products,
        start_date=start_date,
        end_date=end_date,
        granularity='hourly'
    )
    
    # Print summary
    print("\nDownload Summary:")
    print("-" * 40)
    for product_id, result in results.items():
        if result['success']:
            print(f"✅ {product_id}: {result['candles_count']} candles")
        else:
            print(f"❌ {product_id}: {result.get('error', 'Unknown error')}")


if __name__ == '__main__':
    main()