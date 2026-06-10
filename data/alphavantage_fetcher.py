#!/usr/bin/env python3
"""
Alpha Vantage Fetcher Implementation for Unified Backtesting Framework
"""
import pandas as pd
from datetime import datetime
from typing import Optional

class AlphaVantageFetcher:
    """Alpha Vantage-specific implementation with rate limit handling"""
    
    def __init__(self, api_key: str = ''):
        self.api_key = api_key or ''
        self.last_error = None
    
    def _fetch_from_source(self, symbol: str, start_date: datetime,
                          end_date: datetime, source_name: str) -> Optional[pd.DataFrame]:
        """
        Fetch data from Alpha Vantage with rate limit handling.
        Requires API key to be configured first.
        """
        if not self.api_key:
            print(f"    Alpha Vantage: No API key configured")
            return None
        
        try:
            import requests
            from hermes_tools import json_parse
            
            # Use compact output to save rate limit
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey=***&outputsize=compact"
            r = requests.get(url, timeout=10)
            
            if r.status_code != 200:
                print(f"    Alpha Vantage HTTP {r.status_code}")
                return None
            
            data = json_parse(r.text)
            
            # Extract daily prices
            daily_data = data.get('Time Series (Daily)', {})
            if not daily_data:
                return None
            
            # Convert to DataFrame
            df_dict = {}
            for date_str, values in daily_data.items():
                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    df_dict[date_obj] = {
                        'Open': float(values.get('1. open', 0)),
                        'High': float(values.get('2. high', 0)),
                        'Low': float(values.get('3. low', 0)),
                        'Close': float(values.get('4. close', 0)),
                        'Volume': int(values.get('5. volume', 0))
                    }
                except (ValueError, KeyError):
                    continue
            
            if not df_dict:
                return None
            
            df = pd.DataFrame(df_dict).T
            # Filter to requested date range
            mask = (df.index >= start_date) & (df.index <= end_date)
            df = df.loc[mask]
            
            return df if not df.empty else None
        except Exception as e:
            self.last_error = str(e)
            print(f"    Alpha Vantage error: {e}")
            return None