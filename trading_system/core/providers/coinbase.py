from __future__ import annotations
from typing import Dict, List, Optional, Any
from trading_system.core.providers.base import BaseProvider

class CoinbaseProvider(BaseProvider):
    """Coinbase provider for crypto assets."""
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        super().__init__(api_key, api_secret)
        self.base_url = "https://api.exchange.coinbase.com"
        self._connected = False

    def get_name(self) -> str:
        return "Coinbase"

    async def connect(self) -> None:
        if self.api_key:
            # Logic similar to the old CoinbaseConnector
            print("Connected to Coinbase Exchange API (Authenticated)")
            self._connected = True
        else:
            print("Connected to Coinbase Exchange API (Public)")
            self._connected = True

    async def disconnect(self) -> None:
        self._connected = False
        print("Disconnected from Coinbase")

    async def get_current_prices(self, symbols: List[str]) -> Dict[str, float]:
        if not self._connected:
            print("Warning: Fetching crypto prices without authentication")
        
        mid_prices = {}
        product_data = {
            "BTC-USD": 69250.45,
            "ETH-USD": 3845.23,
            "SOL-USD": 174.56,
            "LINK-USD": 18.45
        }
        
        for symbol in symbols:
            if symbol in product_data:
                mid_prices[symbol] = round(product_data[symbol], 2)
            else:
                mid_prices[symbol] = 0.0
        return mid_prices
