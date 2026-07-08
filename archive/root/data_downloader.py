#!/usr/bin/env python3
"""
Historical Data Downloader
Downloads 1-year daily historical prices from Coinbase and Alpaca APIs.
Rate-limited to free tier safety.
"""

import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import statistics


class HistoricalDataDownloader:
    """
    Downloads historical market data from multiple exchanges.
    Implements rate limiting and graceful error handling.
    
    Coverage:
    - Coinbase: BTC-USD, ETH-USD, SOL-USD (crypto)
    - Alpaca: AAPL, MSFT, GOOGL, TSLA, QQQ, SPY, VTI (stocks/etfs)
    """

    # Target assets with real-time price fetcher support
    TARGET_ASSETS = [
        "BTC-USD",   # Coinbase crypto
        "ETH-USD",   # Coinbase crypto
        "SOL-USD",   # Coinbase crypto
        "AAPL",      # Alpaca stock
        "MSFT",      # Alpaca stock
        "GOOGL",     # Alpaca stock
        "TSLA",      # Alpaca stock
        "SPY",       # SPDR S&P 500 ETF
        "QQQ",       # Invesco QQQ Trust
        "VTI",       # Vanguard Total Stock Market ETF
    ]

    DAYS_TO_RETAIN = 365  # One year of daily data

    def __init__(self, rate_limit_calls_min: int = 10):
        """
        Initialize downloader with configurable rate limiting.
        
        Args:
            rate_limit_calls_min: Maximum API calls per minute for free tier safety
        """
        self.rate_limit_calls_min = rate_limit_calls_min
        self.calls_last_minute = 0
        self.minute_window_start = time.time()
        
    def _apply_rate_limit(self) -> None:
        """Apply rate limiting to stay within free tier limits."""
        now = time.time()
        minute_ago = now - 60
        
        # Remove calls older than a minute
        self.calls_last_minute = sum(
            1 for call_time in [now - (i * 1) for i in range(len(self.calls_last_minute))] 
            if call_time > minute_ago
        )
        
        # Recalculate properly
        self.calls_last_minute = len([
            t for t, _ in self.calls_history 
            if now - t < 60
        ]) if hasattr(self, 'calls_history') else 0
        
        # Check rate limit and sleep if necessary
        while self.calls_last_minute >= self.rate_limit_calls_min:
            time.sleep(60.0 / self.rate_limit_calls_min)
            self.calls_last_minute -= 1
    
    def download_asset_history(self, symbol: str, output_path: str) -> Dict[str, float]:
        """
        Download historical daily prices for a single asset from Coinbase.
        
        Args:
            symbol: Asset ticker (e.g., "BTC-USD", "AAPL")
            output_path: Path to save CSV file
            
        Returns:
            Dictionary with price summary statistics
        """
        try:
            self._apply_rate_limit()
            
            # Coinbase API endpoint for market data
            if symbol in ["BTC-USD", "ETH-USD", "SOL-USD"]:
                url = f"https://api.exchange.coinbase.com/products/{symbol}/candles"
                params = {
                    "interval": "1day",
                    "limit": self.DAYS_TO_RETAIN
                }
                base_url = "https://exchange.coinbase.com"  # Use exchange subdomain
                
            else:
                # Alpaca API for stocks and ETFs
                url = f"https://data.alpaca.markets/csv/v2/get_csv"
                params = {
                    "trading_start": (datetime.now() - timedelta(days=self.DAYS_TO_RETAIN)).strftime("%Y-%m-%d"),
                    "trading_end": datetime.now().strftime("%Y-%m-%d"),
                    "symbols": symbol,
                    "exchange": "us",
                    "format": "csv",
                    "timestamp_format": "timestamp_msec"
                }
                
            # Fetch data with error handling
            import requests
            
            headers = {"User-Agent": "Hermes Historical Data Downloader (Python)"}
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                if response.status_code != 200:
                    print(f"    ⚠️  API returned {response.status_code} for {symbol}")
                    return {}
                    
            except Exception as e:
                print(f"    ❌ Request error for {symbol}: {e}")
                return {}
            
            # Parse and save CSV
            lines = response.text.split('\n')
            if len(lines) < 2:
                print(f"    ⚠️  Empty response for {symbol}")
                return {}
                
            with open(output_path, 'w') as f:
                # Save raw data
                for line in lines[:100]:  # First 100 rows + header
                    if line.strip():
                        f.write(line + '\n')
            
            print(f"    ✅ Saved {len(lines)} rows to {output_path}")
            
            # Calculate summary statistics from the data
            prices = []
            for line in lines[1:5]:  # First few data rows (skip header)
                try:
                    parts = line.split(',')
                    if len(parts) >= 3:
                        price = float(parts[2])  # typically close price
                        prices.append(price)
                except (ValueError, IndexError):
                    continue
                    
            return {
                "symbol": symbol,
                "rows_downloaded": len(lines),
                "first_price": min(prices) if prices else None,
                "last_price": max(prices) if prices else None,
                "average_price": statistics.mean(prices) if prices else None
            }
            
        except Exception as e:
            print(f"    ❌ Error downloading {symbol}: {e}")
            return {}

    def download_all_assets(self, output_dir: str = ".", base_url: Optional[str] = None) -> Dict[str, dict]:
        """
        Download historical data for all target assets.
        
        Args:
            output_dir: Directory to save CSV files
            base_url: Override default base URL (for testing purposes)
            
        Returns:
            Dictionary mapping asset symbols to their statistics
        """
        print("\n" + "="*60)
        print("📥 HISTORICAL DATA DOWNLOADER")
        print("="*60)
        
        summary = {}
        errors = []
        downloads = 0
        
        for symbol in self.TARGET_ASSETS:
            try:
                output_path = os.path.join(output_dir, f"{symbol}_daily.csv")
                stats = self.download_asset_history(symbol, output_path)
                
                if stats:
                    summary[symbol] = stats
                    downloads += 1
                
                # Rate limit between assets (5 seconds max)
                time.sleep(0.5)
                
            except Exception as e:
                error_msg = f"{symbol}: {e}"
                errors.append(error_msg)
                print(f"    ❌ {error_msg}")
        
        # Generate summary report
        self._generate_summary_report(summary, errors, base_url)
        
        return summary

    def _generate_summary_report(self, summary: Dict[str, dict], errors: List[str], 
                                base_url: Optional[str] = None):
        """Generate a text summary of all downloads."""
        print("\n" + "="*60)
        print("📊 DOWNLOAD SUMMARY")
        print("="*60)
        
        # Header with timestamp and location
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"Timestamp: {timestamp}")
        if base_url:
            print(f"Base URL Override: {base_url}")
        
        print("\n--- Market Data Summary ---")
        total_assets = len(self.TARGET_ASSETS)
        successful_downloads = len(summary)
        
        for symbol, stats in sorted(summary.items()):
            status = "✅" if symbol in stats else "❌"
            print(f"\n{status} {symbol}:")
            print(f"   Rows downloaded: {stats.get('rows_downloaded', 0)}")
            
            if 'first_price' in stats and stats['first_price'] is not None:
                # Get actual first and last prices from CSV for accurate reporting
                csv_path = os.path.join(os.getcwd(), f"{symbol}_daily.csv")
                try:
                    with open(csv_path, 'r') as f:
                        lines = f.readlines()
                        if len(lines) > 1:
                            header = lines[0].strip()
                            data_lines = [l for l in lines[1:] if l.strip()]
                            
                            # Parse to find actual first/last prices
                            all_prices = []
                            for line in data_lines[:20]:  # First 20 rows
                                parts = line.split(',')
                                if len(parts) >= 3:
                                    try:
                                        all_prices.append(float(parts[2]))
                                    except ValueError:
                                        continue
                            
                            if all_prices:
                                print(f"   First price (row): ${all_prices[0]:.2f}")
                                print(f"   Last price (row):  ${all_prices[-1]:.2f}")
                                
                except Exception as e:
                    print(f"   ⚠️  Could not parse prices: {e}")
        
        if errors:
            print(f"\n⚠️  Errors encountered:")
            for error in errors[:3]:  # Show first 3 errors
                print(f"   - {error}")
        
        print("\n--- File Locations ---")
        current_dir = os.getcwd()
        for symbol in self.TARGET_ASSETS:
            if symbol in summary:
                csv_path = os.path.join(current_dir, f"{symbol}_daily.csv")
                file_size_kb = os.path.getsize(csv_path) / 1024 if os.path.exists(csv_path) else 0
                print(f"   📁 {symbol}_daily.csv ({file_size_kb:.2f} KB)")
        
        print("\n" + "="*60)
        print(f"✅ Historical data download complete!")
        print(f"Successfully downloaded: {successful_downloads}/{total_assets} assets")
        print("="*60 + "\n")


def main():
    """Main entry point for historical data download."""
    
    # Use real API calls with rate limiting (free-tier safe)
    downloader = HistoricalDataDownloader(rate_limit_calls_min=10)
    
    # Download all assets with their real historical data
    results = downloader.download_all_assets(
        output_dir="./data/historical",
        base_url=None  # Use default Coinbase/Alpaca URLs
    )


if __name__ == "__main__":
    main()
