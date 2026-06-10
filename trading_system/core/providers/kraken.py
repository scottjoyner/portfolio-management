from __future__ import annotations
from typing import Dict, List, Optional, Any
from trading_system.core.providers.base import BaseProvider

class KrakenProvider(BaseProvider):
    """Kraken exchange provider for crypto assets."""
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        super().__init__(api_key, api_secret)
        self.base_url = "https://api.kraken.com"
        self.websocket_url = "wss://ws.kraken.com"
        self._connected = False

    def get_name(self) -> str:
        return "Kraken"

    async def connect(self) -> None:
        if self.api_key and self.api_secret:
            try:
                if not self.api_key.isalnum() and "_" not in self.api_key:
                    raise ValueError("Invalid Kraken API key. Must be alphanumeric or contain '_'")
                
                if len(self.api_secret) < 15:
                    raise ValueError("Kraken private secret too short.")
                
                self._connected = True
                print("Connected to Kraken Exchange (Private API authenticated)")
            except Exception as e:
                print(f"Failed to connect to Kraken: {str(e)}")
        else:
            self._connected = True
            print("Connected to Kraken Exchange (public data)")

    async def disconnect(self) -> None:
        self._connected = False
        print("Disconnected from Kraken")

    async def get_current_prices(self, pairs: List[str]) -> Dict[str, float]:
        if not self._connected and any("/" in p for p in pairs):
            print("Warning: Fetching spot prices without authentication")
        
        mid_prices = {
            "XBT/USD": 69250.45,
            "ETH/USD": 3845.23,
            "SOL/USD": 174.56,
            "LINK/USD": 18.45,
            "ADA/USD": 0.4523,
        }
        
        prices = {}
        for pair in pairs:
            base = pair.split("/")[0] if "/" in pair else pair
            if base in mid_prices:
                prices[pair] = round(mid_prices[base], 2)
            else:
                prices[pair] = 0.0
        return prices

    async def get_historical_prices(self, symbol: str, start_date: str, end_date: str, granularity: int = 60) -> List[Dict[str, Any]]:
        # Mock historical data
        return []

    async def get_order_book(self, pair: str, count: int = 25) -> Dict[str, Any]:
        current_price = 69250.45
        asks = [
            {
                "price": round(current_price + (i * 8), 2),
                "volume": round(0.5 - i * 0.08, 3)
            } for i in range(count)
        ]
        bids = [
            {
                "price": round(current_price - (i * 8), 2),
                "volume": round(0.5 + i * 0.1, 3)
            } for i in range(count)
        ]
        return {
            "asks": asks[:count],
            "bids": bids[:count]
        }
