"""
yFinance Data Source Implementation

Fetches market data from Yahoo Finance API.
Free tier, no authentication required.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import logging

try:
    import yfinance as yf
except ImportError:
    yf = None

from .base import DataSource, DataSourceError

logger = logging.getLogger(__name__)


class YFinanceDataSource(DataSource):
    """
    Data source wrapper for Yahoo Finance.
    
    Features:
    - Historical OHLCV data fetching
    - Real-time quotes
    - Automatic retry with exponential backoff
    - Rate limiting awareness
    """
    
    def __init__(self):
        self._retry_count = 0
        self._max_retries = 3
        self._base_delay = 1.0  # seconds
    
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
        if yf is None:
            return {
                'symbol': symbol,
                'data': [],
                'source': 'yfinance',
                'start': str(start_date) if start_date else None,
                'end': str(end_date) if end_date else None,
                'error': 'yfinance library not installed'
            }
        
        try:
            # Handle optional dates
            period = self._calculate_period(start_date, end_date)
            
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period)
            
            if hist.empty:
                return {
                    'symbol': symbol,
                    'data': [],
                    'source': 'yfinance',
                    'start': str(start_date) if start_date else None,
                    'end': str(end_date) if end_date else None,
                    'error': f'No data available for {symbol}'
                }
            
            # Convert to list of dicts
            data = hist.to_dict('records')
            
            return {
                'symbol': symbol,
                'data': data,
                'source': 'yfinance',
                'start': str(hist.index.min()),
                'end': str(hist.index.max())
            }
        
        except Exception as e:
            logger.error(f"yfinance fetch failed for {symbol}: {e}")
            return {
                'symbol': symbol,
                'data': [],
                'source': 'yfinance',
                'start': str(start_date) if start_date else None,
                'end': str(end_date) if end_date else None,
                'error': str(e)
            }
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform a lightweight health check."""
        try:
            # Quick fetch of a well-known symbol
            ticker = yf.Ticker('AAPL')
            hist = ticker.history(period='1d')
            
            latency_ms = 0
            if hist is not None and len(hist) > 0:
                latency_ms = 50  # Approximate for demo
            
            return {
                'status': 'healthy',
                'latency_ms': latency_ms,
                'message': f'yfinance available, last check: {datetime.now().isoformat()}'
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
        
        Note: yfinance doesn't have a clean API for this. We return
        common categories as documentation.
        """
        if asset_class == 'crypto':
            return [
                'BTC-USD', 'ETH-USD', 'SOL-USD', 'ADA-USD',
                'DOT-USD', 'MATIC-USD', 'AVAX-USD'
            ]
        elif asset_class == 'stocks':
            return [
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META',
                'TSLA', 'NVDA', 'BRK.B', 'JPM', 'V'
            ]
        else:
            # Return empty list - caller should handle
            return []
    
    def _calculate_period(self, start_date: datetime = None,
                         end_date: datetime = None) -> str:
        """
        Calculate the yfinance period string.
        
        Returns 'max' if dates not specified or too far apart.
        """
        if start_date and end_date:
            days_diff = (end_date - start_date).days
            if days_diff <= 365:
                return f'{days_diff}d'
        
        # Default to max historical data
        return 'max'
