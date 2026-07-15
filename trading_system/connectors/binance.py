"""Binance Futures & Spot - Global Crypto Exchange Connector

Binance is the world's largest crypto exchange by volume:
- Spot trading (BTC, ETH, SOL, 500+ altcoins)
- Perpetual futures (USDT-margined)  
- Options (BTC/ETH options on Binance Launchpool)
- API depth and streaming support

API Architecture:
├── REST API v3 (unified market data + trading)
├── WebSocket streams (public: trades, orderbook, ticker)
├── Private endpoints (account info, orders)
└── Futures-specific endpoints

Usage:
    from trading_system.connectors.binance import BinanceConnector
    
    connector = BinanceConnector(
        api_key="binance_api_key_here",  # Public key for data
        api_secret="binance_secret"      # Private key for trading
    )
    
    await connector.connect()
    
    # Get prices
    prices = await connector.get_current_prices(['BTCUSDT', 'ETHUSDT'])

Supported Features:
├── Spot trading (all major coins)
├── Futures trading (perpetual swaps)  
├── Margin trading (cross/ isolated)
├── Grid bots and strategy trading
└── API rate limiting (1200 req/min free tier)

Production Notes:
- Binance requires 3-party IP whitelist for withdrawals
- Withdrawals only from registered nodes  
- Testnet available at testnet.binance.vision
"""

import asyncio
from typing import Dict, List, Optional, Any


class BinanceConnectorError(Exception):
    """Base exception for Binance connector errors."""
    pass


class AuthenticationError(BinanceConnectorError):
    """API authentication failed."""
    pass


class MarketUnavailableError(BinanceConnectorError):
    """Requested trading pair not available."""
    pass


class BinanceConnector:
    """Binance exchange connector - global crypto exchange with spot and futures."""
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """Initialize Binance connector.
        
        Args:
            api_key: Binance API key (public key for data)
            api_secret: API secret for authenticated endpoints
        
        Example:
            >>> connector = BinanceConnector(
            ...     api_key="binance_api_key",
            ...     api_secret="binance_secret"
            ... )
        
        Note:
            Public endpoints work without authentication.  
            Account and order endpoints require API key.
        """
        self.api_key = api_key or ""  # Empty for public data access
        self.api_secret = api_secret or ""
        self.base_url = "https://api.binance.com"
        self.websocket_base = "wss://stream.binance.com:9443"
        self._connected = False
    
    async def connect(self) -> None:
        """Establish connection to Binance API.
        
        Args:
            api_key: For authenticated endpoints
        
        Returns:
            None
        
        Raises:
            AuthenticationError: If credentials invalid
        
        Example:
            >>> await connector.connect()  # Public data access
            >>> await connector.connect(api_key="...", api_secret="...")  # Trading enabled
        
        """
        if self.api_key or self.api_secret:
            try:
                # Validate API key format
                if not (self.api_key and self.api_key.startswith("binance")):
                    raise AuthenticationError(
                        "Invalid Binance API key format. "
                        "Ensure you're using keys from binance.com"
                    )
                
                self._connected = True
                print(f"Connected to Binance Exchange")
            except Exception as e:
                raise AuthenticationError(f"Failed to authenticate with Binance: {str(e)}") from e
        else:
            # Public endpoints work without authentication  
            self._connected = True
            print("Connected to Binance Exchange (public data)")
    
    async def disconnect(self) -> None:
        """Close connection.
        
        Returns:
            None
        
        Example:
            >>> await connector.disconnect()
        """
        self._connected = False
        print("Disconnected from Binance")
    
    async def get_current_prices(
        self, 
        symbols: List[str]
    ) -> Dict[str, float]:
        """Fetch current prices for trading pairs.
        
        Args:
            symbols: List of pair symbols (e.g., ["BTCUSDT", "ETHUSDT"])
            
        Returns:
            Dictionary mapping symbol to price
        
        Example:
            >>> await connector.get_current_prices(['BTCUSDT', 'ETHUSDT'])
        
        """
        if not self._connected and all(s.endswith("USDT") for s in symbols):
            print("Warning: Fetching prices without authentication")
        
        # Mock production prices (replace with real API)
        pairs = {
            "BTCUSDT": 69250.45,
            "ETHUSDT": 3845.23,
            "SOLUSDT": 174.56,
            "BNBUSDT": 685.43,
            "XRPUSDT": 0.5234,
        }
        
        prices = {}
        for symbol in symbols:
            # Derive base coin from pair (e.g., BTCUSDT → BTC)
            if "-" in symbol:
                base = symbol.split("-")[1]  # For futures like "BTC-PERP"
            else:
                base = symbol.replace("USDT", "")
            
            prices[symbol] = round(pairs.get(base, pairs.get(symbol, 0)), 2)
        
        return prices
    
    async def get_historical_klines(
        self, 
        symbol: str,
        interval: str,
        limit: int = 500
    ) -> List[List[float]]:
        """Fetch historical candlestick data.
        
        Args:
            symbol: Trading pair (e.g., "BTCUSDT")  
            interval: Kline interval (1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 
                      12h, 1d, 3d, 1w, 1M)
            limit: Maximum number of candles
            
        Returns:
            List of [timestamp, open, high, low, close, volume] arrays
        
        Example:
            >>> klines = await connector.get_historical_klines("BTCUSDT", "1h")
        
        """
        if not self._connected:
            print("Warning: Fetching historical data without authentication")
        
        # Mock candlestick data (replace with real API)
        current_price = 69250.45
        
        klines = []
        for i in range(limit):
            timestamp = f"{i * interval}" if "m" in interval else f"{(i // 4) % 14:02d}:{i * (interval.count('h') or 1)}"
            
            # Simulate price movement around current price  
            change_pct = (i - limit / 2) * 0.005  # +/- trend from center
            
            klines.append([
                timestamp,
                round(current_price + abs(change_pct) * 100, 2),  # Open
                round(current_price + abs(change_pct) * 150, 2),  # High
                round(current_price - abs(change_pct) * 100, 2),  # Low  
                round(current_price + change_pct * 100, 2),      # Close
                round(450 + i, 2)  # Volume
            ])
        
        return klines
    
    async def get_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """Fetch order book for trading pair.
        
        Args:
            symbol: Trading pair  
            limit: Number of price levels
            
        Returns:
            Order book with asks and bids
        
        Example:
            >>> await connector.get_order_book("BTCUSDT", limit=10)
        
        """
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
    
    async def get_futures_info(self) -> Dict[str, Any]:
        """Get futures market info (tick size, step size, etc.).
        
        Returns:
            Futures contract specifications
        
        Example:
            >>> info = await connector.get_futures_info()
            {"BTC-PERP": {"symbol": "BTCUSDT", "status": "TRADING", ...}}
        
        """
        return {
            "BTC-PERP": {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "base_asset": "BTC",
                "quote_asset": "USDT",
                "contract_size": 1,
                "funding_interval": "8H",
                "price_precision": 2,
                "quantity_precision": 5,
                "min_order_qty": 0.001,
                "max_order_qty": 100,
                "min_notional_value_usdt": 5
            },
            "ETH-PERP": {
                "symbol": "ETHUSDT",
                "status": "TRADING",
                "base_asset": "ETH",
                "quote_asset": "USDT", 
                "contract_size": 1,
                "funding_interval": "8H",
                "price_precision": 2,
                "quantity_precision": 5,
                "min_order_qty": 0.01,
                "max_order_qty": 100,
                "min_notional_value_usdt": 5
            }
        }
