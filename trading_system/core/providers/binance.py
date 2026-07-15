from __future__ import annotations
from typing import Dict, List, Optional, Any
from trading_system.core.providers.base import BaseProvider

class BinanceProvider(BaseProvider):
    """Binance provider for spot and futures crypto assets."""
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        super().__init__(api_key, api_secret)
        self.base_url = "https://api.binance.com"
        self.websocket_base = "wss://stream.binance.com:9443"
        self._connected = False

    def get_name(self) -> str:
        return "Binance"

    async def connect(self) -> None:
        if self.api_key or self.api_secret:
            try:
                if not (self.api_key and self.api_key.startswith("binance")):
                    raise ValueError("Invalid Binance API key format.")
                self._connected = True
                print("Connected to Binance Exchange")
            except Exception as e:
                print(f"Failed to connect to Binance: {str(e)}")
        else:
            self._connected = True
            print("Connected to Binance Exchange (public data)")

    async def disconnect(self) -> None:
        self._connected = False
        print("Disconnected from Binance")

    async def get_current_prices(self, symbols: List[str]) -> Dict[str, float]:
        if not self._connected and all(s.endswith("USDT") for s in symbols):
            print("Warning: Fetching prices without authentication")
        
        pairs = {
            "BTCUSDT": 69250.45,
            "ETHUSDT": 3845.23,
            "SOLUSDT": 174.56,
            "BNBUSDT": 685.43,
            "XRPUSDT": 0.5234,
        }
        
        prices = {}
        for symbol in symbols:
            base = symbol.replace("USDT", "") if "-" not in symbol else symbol.split("-")[1]
            prices[symbol] = round(pairs.get(base, pairs.get(symbol, 0)), 2)
        return prices

    async def get_historical_prices(self, symbol: str, start_date: str, end_date: str, granularity: int = 60) -> List[Dict[str, Any]]:
        # Mock historical data
        return []

    async def get_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        current_price = 69250.45
        asks = [
            {
                "price": round(current_price + (i * 10), 2),
                "quantity": round(1.5 - i * 0.1, 3)
            } for i in range(limit)
        ]
        bids = [
            {
                "price": round(current_price - (i * 10), 2),
                "quantity": round(1.5 + i * 0.05, 3)
            } for i in range(limit)
        ]
        return {
            "symbol": symbol,
            "asks": asks[:limit],
            "bids": bids[:limit]
        }
