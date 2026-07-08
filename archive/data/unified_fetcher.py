#!/usr/bin/env python3
"""
Unified Backtesting Framework for Portfolio Management
Integrates yfinance, Alpha Vantage, and provides unified data access
"""
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

class UnifiedDataFetcher:
    """Unified interface for fetching market data from multiple sources"""
    
    def __init__(self):
        self.sources = {
            'yfinance': {'enabled': True, 'last_error': None},
            'alphavantage': {'enabled': False, 'api_key': '', 'last_error': None}
        }
        self.cache_dir = os.path.expanduser('~/git/portfolio-management/data')
    
    def fetch_with_fallback(self, symbol: str, start_date: datetime,
                           end_date: datetime, preferred_source: str) -> Optional[pd.DataFrame]:
        """
        Fetch data with source priority fallback.
        Returns price data or None if all sources fail.
        """
        results = {}
        
        # Try sources in priority order
        for source_name in self._get_source_order(preferred_source):
            try:
                result = self._fetch_from_source(symbol, start_date, end_date, source_name)
                if result is not None and len(result) > 0 and 'Close' in result.columns:
                    results[source_name] = result
                    print(f"  ✓ {source_name}: Retrieved data")
                    return result
                else:
                    print(f"  ✗ {source_name}: No valid data returned")
            except Exception as e:
                self.sources[source_name]['last_error'] = str(e)
                print(f"  ! {source_name}: Error - {e}")
        
        # Return best available result
        if results:
            return list(results.values())[0]
        return None
    
    def _get_source_order(self, preferred: str) -> List[str]:
        """Get source order based on preference"""
        priority = ['yfinance', 'alphavantage']  # Default order
        if preferred == 'alphavantage':
            return ['alphavantage', 'yfinance']
        return priority
    
    def _fetch_from_source(self, symbol: str, start_date: datetime,
                          end_date: datetime, source_name: str) -> Optional[pd.DataFrame]:
        """
        Fetch data from a specific source.
        Override in subclasses for yfinance and alphavantage implementations.
        """
        raise NotImplementedError
    
    def fetch_multiple(self, symbols: List[str], start_date: datetime,
                      end_date: datetime) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for multiple symbols using preferred source.
        Returns dict mapping symbol to DataFrame (or empty if failed).
        """
        results = {}
        
        for symbol in symbols:
            try:
                data = self.fetch_with_fallback(
                    symbol, start_date, end_date, 'yfinance'
                )
                if data is not None and len(data) > 0:
                    results[symbol] = data
                else:
                    results[symbol] = pd.DataFrame()
            except Exception as e:
                print(f"  ! Failed to fetch {symbol}: {e}")
                results[symbol] = pd.DataFrame()
        
        return results
    
    def save_to_csv(self, data: Any, filepath: str):
        """Save data to CSV with proper formatting"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        if isinstance(data, pd.DataFrame) and len(data) > 0:
            # Convert datetime index to string for CSV compatibility
            df = data.copy()
            df.index = df.index.strftime('%Y-%m-%d')
            df.to_csv(filepath)
    
    def load_from_csv(self, filepath: str) -> Optional[pd.DataFrame]:
        """Load data from CSV file"""
        if not os.path.exists(filepath):
            return None
        try:
            df = pd.read_csv(filepath)
            # Convert date column back to datetime
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
                df.set_index('Date', inplace=True)
            return df
        except Exception as e:
            print(f"  ! Failed to load {filepath}: {e}")
            return None