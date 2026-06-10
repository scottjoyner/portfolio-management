"""
Data Source Factory

Creates and manages instances of various data sources.
Provides a unified interface for source selection and lifecycle management.
"""

import logging
import os
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class DataSourceFactory:
    """
    Factory for creating data source instances.
    
    Supports:
    - yfinance (no auth required)
    - alphavantage (optional API key)
    - Default/local sources
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self._sources: Dict[str, Any] = {}
    
    def create_source(self, name: str) -> Any:
        """
        Create a data source instance by name.
        
        Args:
            name: Source identifier ('yfinance', 'alphavantage')
            
        Returns:
            Configured DataSource instance
        """
        sources = {
            'yfinance': self._create_yfinance,
            'alphavantage': self._create_alphavantage,
            'default': self._create_default
        }
        
        if name not in sources:
            raise ValueError(f"Unknown data source: {name}")
        
        return sources[name]()
    
    def _create_yfinance(self) -> Any:
        """Create yfinance data source."""
        from .yfinance import YFinanceDataSource
        return YFinanceDataSource()
    
    def _create_alphavantage(self) -> Any:
        """Create Alpha Vantage data source."""
        from .alphavantage import AlphaVantageDataSource
        api_key = self.config.get('alphavantage', {}).get(
            'api_key',
            os.environ.get('ALPHA_VANTAGE_API_KEY', '')
        )
        return AlphaVantageDataSource(api_key=api_key)
    
    def _create_default(self) -> Any:
        """
        Create a default/local data source.
        
        Falls back to cached data or mock data for testing.
        """
        from .default import DefaultDataSource
        return DefaultDataSource()
    
    async def health_check_all(self) -> Dict[str, Any]:
        """
        Perform health checks on all configured sources.
        
        Returns:
            Dict mapping source names to their health status
        """
        results = {}
        
        for name in ['yfinance', 'alphavantage']:
            try:
                source = self.create_source(name)
                if hasattr(source, 'health_check'):
                    results[name] = await source.health_check()
                else:
                    results[name] = {'status': 'unknown', 'error': 'No health check method'}
            except Exception as e:
                results[name] = {
                    'status': 'unhealthy',
                    'error': str(e)
                }
        
        return results
    
    async def fetch_with_fallback(self, symbol: str, start_date: datetime,
                                  end_date: datetime,
                                  preferred_sources: List[str] = None) -> Dict[str, Any]:
        """
        Fetch data with automatic fallback to alternative sources.
        
        Args:
            symbol: Ticker symbol
            start_date: Start date
            end_date: End date
            preferred_sources: Ordered list of source names to try
            
        Returns:
            Data from the first successful source, or empty with error info
        """
        if preferred_sources is None:
            # Default order: yfinance -> alphavantage -> default
            preferred_sources = ['yfinance', 'alphavantage', 'default']
        
        for source_name in preferred_sources:
            try:
                source = self.create_source(source_name)
                result = await source.fetch(symbol, start_date, end_date)
                
                if not result.get('error') and result.get('data'):
                    logger.info(f"Successfully fetched {symbol} from {source_name}")
                    return result
                elif result.get('error') == 'yfinance library not installed':
                    # Skip yfinance if not installed, try next source
                    continue
            except Exception as e:
                logger.warning(f"Source {source_name} failed: {e}")
        
        return {
            'symbol': symbol,
            'data': [],
            'sources_tried': preferred_sources,
            'error': f'All sources failed or unavailable'
        }
