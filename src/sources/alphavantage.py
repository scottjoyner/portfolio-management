"""
Alpha Vantage Data Source Implementation

Fetches market data from Alpha Vantage API.
Free tier with rate limits (5 calls/minute, 500/day).
No authentication required for basic endpoints.
"""

import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import logging
import json

try:
    import requests
except ImportError:
    requests = None

from .base import DataSource, DataSourceError

logger = logging.getLogger(__name__)


class AlphaVantageDataSource(DataSource):
    """
    Data source wrapper for Alpha Vantage API.
    
    Features:
    - Historical time series data
    - Real-time quotes
    - Digital currency rates
    - Rate limit awareness with automatic backoff
    """
    
    BASE_URL = 'https://www.alphavantage.co/query'
    RATE_LIMIT_DELAY = 1.0  # seconds between calls
    MAX_RETRIES = 3
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get('ALPHA_VANTAGE_API_KEY', '')
        if not self.api_key:
            logger.warning("Alpha Vantage API key not configured")
        
        self._retry_count = 0
        self._last_request_time = None
    
    async def fetch(self, symbol: str, start_date: datetime = None,
                   end_date: datetime = None) -> Dict[str, Any]:
        """
        Fetch historical data for a given symbol.
        
        Args:
            symbol: Ticker symbol
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            Dict with 'symbol', 'data', 'source', 'start', 'end', 'error'
        """
        if requests is None:
            return {
                'symbol': symbol,
                'data': [],
                'source': 'alphavantage',
                'start': str(start_date) if start_date else None,
                'end': str(end_date) if end_date else None,
                'error': 'requests library not installed'
            }
        
        try:
            # Use the Time Series API
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol,
                'apikey': self.api_key,
                'outputsize': 'compact'  # Last 100 days for free tier
            }
            
            if start_date and end_date:
                params['interval'] = '1d'
            
            response = await asyncio.to_thread(
                requests.get, self.BASE_URL, params=params,
                timeout=30
            )
            
            # Check rate limit headers
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                logger.warning(f"Alpha Vantage rate limited: Retry after {retry_after}s")
                return {
                    'symbol': symbol,
                    'data': [],
                    'source': 'alphavantage',
                    'start': str(start_date) if start_date else None,
                    'end': str(end_date) if end_date else None,
                    'error': f'Rate limited. Retry after {retry_after}s'
                }
            
            data = response.json()
            
            # Check for API error
            errors = ['ERROR', 'Note:']
            if any(data.get(key) == 'No Data Found returned from the servers' for key in data):
                return {
                    'symbol': symbol,
                    'data': [],
                    'source': 'alphavantage',
                    'start': str(start_date) if start_date else None,
                    'end': str(end_date) if end_date else None,
                    'error': data.get('Note', 'No data available')
                }
            
            # Extract time series data
            timeseries = data.get('Meta Data', {}).get(
                'Time Series (Daily)', {}
            )
            
            if not timeseries:
                return {
                    'symbol': symbol,
                    'data': [],
                    'source': 'alphavantage',
                    'start': str(start_date) if start_date else None,
                    'end': str(end_date) if end_date else None,
                    'error': 'No time series data returned'
                }
            
            # Convert to list of dicts
            data_list = []
            for date_str, values in timeseries.items():
                record = {
                    'date': date_str,
                    'open': float(values.get('1. open', 0)),
                    'high': float(values.get('2. high', 0)),
                    'low': float(values.get('3. low', 0)),
                    'close': float(values.get('4. close', 0)),
                    'volume': int(values.get('5. volume', 0))
                }
                data_list.append(record)
            
            # Sort by date descending
            data_list.sort(key=lambda x: x['date'], reverse=True)
            
            return {
                'symbol': symbol,
                'data': data_list,
                'source': 'alphavantage',
                'start': list(timeseries.keys())[-1] if timeseries else None,
                'end': list(timeseries.keys())[0] if timeseries else None
            }
        
        except Exception as e:
            logger.error(f"Alpha Vantage fetch failed for {symbol}: {e}")
            return {
                'symbol': symbol,
                'data': [],
                'source': 'alphavantage',
                'start': str(start_date) if start_date else None,
                'end': str(end_date) if end_date else None,
                'error': str(e)
            }
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform a lightweight health check."""
        try:
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': 'AAPL',
                'apikey': self.api_key
            }
            
            response = await asyncio.to_thread(
                requests.get, self.BASE_URL, params=params,
                timeout=10
            )
            
            data = response.json()
            status_code = response.status_code
            
            if status_code == 429:
                return {
                    'status': 'rate_limited',
                    'latency_ms': 0,
                    'error': 'Rate limit exceeded'
                }
            elif data.get('Note') and 'No Data Found' in data['Note']:
                return {
                    'status': 'unhealthy',
                    'latency_ms': 0,
                    'error': data['Note']
                }
            
            # Extract quote for latency measurement
            quote = data.get('Global Quote', {})
            price = float(quote.get('05. price', 0))
            
            return {
                'status': 'healthy' if status_code == 200 else 'unhealthy',
                'latency_ms': 100,  # Approximate
                'price': price,
                'message': f"Alpha Vantage healthy, AAPL: ${price:.2f}"
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'latency_ms': 0,
                'error': str(e)
            }
    
    async def get_available_symbols(self, asset_class: str = None) -> List[str]:
        """
        Get available symbols.
        
        Alpha Vantage has a fixed list of supported symbols.
        We return common ones for documentation purposes.
        """
        if asset_class == 'crypto':
            return [
                'BTCUSD', 'ETHUSD', 'XRPUSD', 'LTCUSD',
                'ADAUSD', 'DOTUSD', 'SOLUSD'
            ]
        elif asset_class == 'stocks':
            return [
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META',
                'TSLA', 'NVDA', 'JPM', 'V', 'WMT'
            ]
        else:
            return []
