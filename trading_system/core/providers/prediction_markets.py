from __future__ import annotations
from typing import Dict, List, Optional, Any
from trading_system.core.providers.base import BaseProvider

class KalshiProvider(BaseProvider):
    """Kalshi prediction market provider (Mock)."""
    
    def get_name(self) -> str:
        return "Kalshi"

    async def connect(self) -> None:
        print("Connected to Kalshi (Mock)")
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False
        print("Disconnected from Kalshi")

    async def get_current_prices(self, symbols: List[str]) -> Dict[str, float]:
        # Kalshi doesn't have standard symbols, usually market_ids
        return {s: 1.0 for s in symbols}

    async def get_historical_prices(self, symbol: str, start_date: str, end_date: str, granularity: int = 60) -> List[Dict[str, Any]]:
        return []

class PolymarketProvider(BaseProvider):
    """Polymarket prediction market provider (Mock)."""
    
    def get_name(self) -> str:
        return "Polymarket"

    async def connect(self) -> None:
        print("Connected to Polymarket (Mock)")
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False
        print("Disconnected from Polymarket")

    async def get_current_prices(self, symbols: List[str]) -> Dict[str, float]:
        return {s: 0.5 for s in symbols}

    async def get_historical_prices(self, symbol: str, start_date: str, end_date: str, granularity: int = 60) -> List[Dict[str, Any]]:
        return []
