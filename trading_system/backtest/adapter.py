"""Market Data Adapter - Interface for market data sources

Provides unified interface for connecting to various market data feeds
and performing historical replay operations.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime


class MarketDataAdapter(ABC):
    """Abstract base class for market data adapters."""
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to market data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Close market data connection."""
        pass
    
    @abstractmethod
    def fetch_historical_data(
        self, 
        symbol: str,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Fetch historical OHLCV data.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC-USDT')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of bar objects with open, high, low, close, volume
        """
        pass
    
    @abstractmethod
    def get_current_price(self, symbol: str) -> Optional[float]:
        """Get current mid price for symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Current price or None if unavailable
        """
        pass


class MockMarketDataAdapter(MarketDataAdapter):
    """Mock market data adapter for testing and simulation."""
    
    def __init__(self):
        self.connected = False
        self.prices: Dict[str, float] = {
            "BTC": 69000.0,
            "ETH": 3800.0,
            "SOL": 170.0,
            "AVAX": 40.0,
            "LINK": 18.0,
        }
    
    async def connect(self) -> bool:
        """Mock connection - always succeeds."""
        self.connected = True
        return True
    
    async def disconnect(self):
        """Close mock connection."""
        self.connected = False
    
    def fetch_historical_data(
        self, 
        symbol: str,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Return simulated historical bars for demo."""
        
        if not self.connected:
            raise RuntimeError("Adapter not connected")
        
        # Return sample bars with realistic OHLCV data
        import random
        
        bars = []
        base_price = self.prices.get(symbol.split('-')[0], 5000)
        timestamp_start = datetime.strptime(start_date, "%Y-%m-%d")
        timestamp_end = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Generate ~180 daily bars for 6-month period
        num_bars = int((timestamp_end - timestamp_start).days)
        
        current_price = base_price * (1 + random.uniform(-0.2, 0.3))
        
        for i in range(num_bars):
            # Simulate daily OHLCV with realistic patterns
            volatility = random.uniform(0.015, 0.025)
            daily_move = current_price * volatility * random.uniform(-1, 1)
            
            open_price = current_price
            high_price = max(open_price, daily_move) * (1 + abs(daily_move)/high_price * 0.1)
            low_price = min(open_price, daily_move) * (1 - abs(daily_move)/open_price * 0.1)
            close_price = current_price + daily_move
            
            # Volume with correlation to price movement
            base_volume = random.uniform(100, 500)
            volume_correlation = abs(daily_move) / open_price
            volume = base_volume * (1 + volume_correlation * 2)
            
            bars.append({
                "timestamp": timestamp_start + __import__('datetime').timedelta(days=i),
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": round(volume, 4),
            })
            
            current_price = close_price
        
        return bars
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """Return mock current price."""
        ticker = symbol.split('-')[0] if '-' in symbol else symbol
        return self.prices.get(ticker)
