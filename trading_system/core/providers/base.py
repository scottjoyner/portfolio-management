from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime

class BaseProvider(ABC):
    """Abstract Base Class for all trading platform providers."""
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self._connected = False

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the platform."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the platform."""
        pass

    @abstractmethod
    async def get_current_prices(self, symbols: List[str]) -> Dict[str, float]:
        """Fetch current mid-prices for a list of symbols."""
        pass

    async def get_historical_prices(
        self, 
        symbol: str, 
        start_date: str, 
        end_date: str, 
        granularity: int = 60
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical OHLCV bars. 
        Default implementation returns empty list.
        """
        return []

    @abstractmethod
    def get_name(self) -> str:
        """Return the name of the provider (e.g., 'Coinbase')."""
        pass
