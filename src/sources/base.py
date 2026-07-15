"""
Unified Data Source Module

Provides a consistent interface for fetching market data from multiple sources:
- yfinance (free tier, no auth required)
yfinance Alpha Vantage Free Tier
- Default/Local sources

All sources are unified through the DataSource base class.
"""

import abc
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class DataSourceError(Exception):
    """Base exception for data source errors."""
    pass


class DataSource(abc.ABC):
    """
    Abstract base class for all data sources.
    
    Provides a unified interface for fetching market data regardless of the underlying provider.
    """
    
    @abc.abstractmethod
    async def fetch(self, symbol: str, start_date: datetime = None,
                   end_date: datetime = None) -> Dict[str, Any]:
        """
        Fetch historical market data for a given symbol.
        
        Args:
            symbol: Ticker symbol (e.g., 'AAPL', 'BTC-USD')
            start_date: Start date for the fetch (inclusive)
            end_date: End date for the fetch (inclusive)
            
        Returns:
            Dict containing:
                - 'symbol': The ticker symbol
                - 'data': List of OHLCV data points
                - 'source': Name of the source used
                - 'start': Actual start datetime fetched
                - 'end': Actual end datetime fetched
                - 'error': Error message if fetch failed (None on success)
        """
        pass
    
    @abc.abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform a lightweight health check against the source.
        
        Returns:
            Dict with 'status', 'latency_ms', and optionally 'message'
        """
        pass
    
    @abc.abstractmethod
    async def get_available_symbols(self, asset_class: str = None) -> List[str]:
        """
        Get list of available symbols from the source.
        
        Args:
            asset_class: Optional filter (e.g., 'stocks', 'crypto')
            
        Returns:
            List of ticker symbols
        """
        pass
    
    def _normalize_date(self, dt: datetime) -> str:
        """Convert datetime to YYYY-MM-DD format."""
        return dt.strftime('%Y-%m-%d')
