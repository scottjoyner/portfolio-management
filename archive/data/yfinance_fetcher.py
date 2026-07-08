#!/usr/bin/env python3
"""
YFinance Fetcher Implementation for Unified Backtesting Framework
"""
import pandas as pd
from datetime import datetime
from typing import Optional

class YFinanceFetcher:
    """YFinance-specific implementation with error handling"""
    
    def __init__(self):
        self.last_error = None
    
    def _fetch_from_source(self, symbol: str, start_date: datetime,
                          end_date: datetime, source_name: str) -> Optional[pd.DataFrame]:
        """
        Fetch data from YFinance with comprehensive error handling.
        """
        try:
            import yfinance as yf
            
            ticker = yf.Ticker(symbol)
            hist = ticker.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d')
            )
            
            if hist.empty:
                return None
            
            # Ensure required columns exist
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            missing = [col for col in required_cols if col not in hist.columns]
            if missing:
                print(f"    Warning: Missing columns {missing}")
                return None
            
            return hist
        except Exception as e:
            self.last_error = str(e)
            print(f"    YFinance error for {symbol}: {e}")
            return None