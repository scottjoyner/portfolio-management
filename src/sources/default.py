"""
Default Data Source

Provides fallback data for testing and when external sources are unavailable.
Uses cached/mock data for demonstration purposes.
"""

import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import logging
import os

logger = logging.getLogger(__name__)


# Base prices for common symbols (shared by mock generation and symbol listing)
base_prices = {
    'AAPL': 178.50,
    'MSFT': 378.90,
    'GOOGL': 141.80,
    'BTC-USD': 67500.00,
    'ETH-USD': 3450.00,
}


class DefaultDataSource:
    """
    Fallback data source using cached or mock data.
    
    Useful for:
    - Testing without external dependencies
    - Graceful degradation when all sources fail
    - Demonstrating the unified interface
    """
    
    def __init__(self, cache_dir: Optional[str] = None):
        self.cache_dir = cache_dir or os.path.expanduser('~/.hermes/portfolio-cache')
        os.makedirs(self.cache_dir, exist_ok=True)
    
    async def fetch(self, symbol: str, start_date: datetime = None,
                   end_date: datetime = None) -> Dict[str, Any]:
        """
        Fetch mock/cached data.
        
        Returns synthetic OHLCV data for demonstration.
        """
        try:
            # Try to load cached data first
            cache_file = os.path.join(self.cache_dir, f'{symbol}.json')
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    return {
                        'symbol': symbol,
                        'data': json.load(f),
                        'source': 'default-cache',
                        'start': str(start_date) if start_date else None,
                        'end': str(end_date) if end_date else None
                    }
            
            # Generate mock data for demonstration
            mock_data = self._generate_mock_data(symbol, start_date, end_date)
            
            # Cache the result
            with open(cache_file, 'w') as f:
                json.dump(mock_data['data'], f)
            
            return {
                'symbol': symbol,
                'data': mock_data['data'],
                'source': 'default-mock',
                'start': str(start_date) if start_date else None,
                'end': str(end_date) if end_date else None
            }
        except Exception as e:
            logger.error(f"Default data source failed: {e}")
            return {
                'symbol': symbol,
                'data': [],
                'source': 'default',
                'start': str(start_date) if start_date else None,
                'end': str(end_date) if end_date else None,
                'error': str(e)
            }
    
    def _generate_mock_data(self, symbol: str,
                           start_date: datetime = None,
                           end_date: datetime = None) -> Dict[str, Any]:
        """
        Generate synthetic OHLCV data for demonstration.
        """
        # Base prices for common symbols (module-level `base_prices`)
        base_price = base_prices.get(symbol, 100.0)
        
        # Generate 30 days of mock data
        num_days = 30
        current_date = end_date or datetime.now()
        
        data_points = []
        price = base_price * (1 + (hash(symbol) % 100) / 1000)
        
        for i in range(num_days):
            prev_date = current_date - timedelta(days=1)
            date_str = prev_date.strftime('%Y-%m-%d')
            
            # Random walk with slight upward bias
            change = (hash(date_str + symbol) % 20) / 10 - 1
            price *= (1 + change / 100)
            
            data_points.append({
                'date': date_str,
                'open': round(price * (1 + hash(date_str) % 5) / 100, 2),
                'high': round(price * (1 + abs(hash(date_str)) % 3) / 100, 2),
                'low': round(price * (1 - abs(hash(date_str)) % 3) / 100, 2),
                'close': round(price, 2),
                'volume': int(1e6 + hash(date_str) % 500000)
            })
        
        return {
            'symbol': symbol,
            'data': data_points
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Always healthy - no external dependencies."""
        return {
            'status': 'healthy',
            'latency_ms': 1,
            'message': 'Default data source ready (mock mode)'
        }
    
    async def get_available_symbols(self, asset_class: str = None) -> List[str]:
        """Return all symbols that can be mocked."""
        return list(base_prices.keys()) + [
            f'{symbol}-USD' for symbol in base_prices.keys()
        ]
