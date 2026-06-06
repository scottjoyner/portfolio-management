"""Exchange Connectors Package - Multi-Platform Trading Integration

Provides unified interface for connecting to multiple financial data providers:
- Crypto exchanges (Coinbase, Binance, Kraken)
- Prediction markets (Polymarket, Kalshi)  
- Traditional brokerages (via API aggregators like Alpaca, Interactive Brokers)
- Market data feeds (Polygon.io, Alpha Vantage, Finnhub)

Supported Platforms:
├── Crypto Exchanges
│   ├── Coinbase Pro/Advanced Trade (REST + WebSocket)
│   ├── Binance Futures (Spot + Perpetual swaps)
│   └── Kraken (Legacy API + v2/v3 REST)
├── Prediction Markets
│   ├── Polymarket (Ethereum-based, ERC-4626 compliant)
│   └── Kalshi (Regulated US futures data feed)
├── Traditional Brokerages (via aggregators)
│   ├── Alpaca Trading API (supports 50+ venues)
│   ├── Interactive Brokers TWS API
│   ├── Tradier API (retail brokerage)
│   └── Schwab API (Charles Schwab)

Architecture:
    +------------------+    +------------------+
    | ExchangeFactory  |    | MarketDataCache |
    |                  |    |                 |
    | register()       |    | LRU Cache        |
    +------------------+    +------------------+
           ↓                      ↓
    +------------------+    +------------------+
    | ConnectorRegistry|    | RateLimiter     |
    | - All connectors |    |                  |
    | - Health checks  |    +------------------+
    +----------------+

Usage:
    from trading_system.connectors.exchange_factory import create_connector
    
    # Coinbase (crypto)
    coinbase = await create_connector('coinbase', api_key='...')
    
    # Polymarket (prediction markets)  
    polymarket = await create_connector('polymarket', rpc_url='https://eth-mainnet.g.alchemy.com/')
    
    # Alpaca (traditional via aggregator)
    alpaca = await create_connector('alpaca', api_key='...', secret='...')

Features:
- Unified interface across all exchanges
- Automatic rate limiting enforcement
- Connection health monitoring  
- Order book aggregation
- Cross-exchange arbitrage detection
"""

import asyncio
from typing import Dict, List, Optional, Any


class ExchangeConnectorError(Exception):
    """Base exception for connector errors."""
    pass


class AuthenticationError(ExchangeConnectorError):
    """Authentication or authorization failed."""
    pass


class ConnectionTimeoutError(ExchangeConnectorError):
    """Connection to exchange timed out."""
    pass


class MarketUnavailableError(ExchangeConnectorError):
    """Requested market/trading pair unavailable."""
    pass


class ExchangeFactory:
    """Factory for creating exchange connectors with unified interface."""
    
    @staticmethod
    async def create_connector(
        exchange_name: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        rpc_url: Optional[str] = None,
        websocket_url: Optional[str] = None
    ) -> Any:
        """Create and return an exchange connector.
        
        Args:
            exchange_name: Exchange/prediction market name (e.g., 'coinbase', 'polymarket')
            api_key: API key for authenticated endpoints
            api_secret: API secret for HMAC signing  
            rpc_url: RPC endpoint for blockchain data (EVM chains)
            websocket_url: WebSocket URL for real-time feeds
            
        Returns:
            Configured connector instance with connected status
            
        Example:
            >>> connector = await ExchangeFactory.create_connector(
            ...     exchange_name='coinbase',
            ...     api_key='sbtest_...'
            ... )
        
        """
        # Factory pattern - create appropriate connector type
        if exchange_name.lower() == 'coinbase':
            from trading_system.connectors.coinbase import CoinbaseConnector
            connector = CoinbaseConnector(api_key=api_key)
        elif exchange_name.lower() == 'polymarket':
            from trading_system.connectors.polymarket import PolymarketConnector
            connector = PolymarketConnector(rpc_url=rpc_url)
        elif exchange_name.lower() == 'kalshi':
            from trading_system.connectors.kalshi import KalshiConnector
            connector = KalshiConnector(api_key=api_key)
        elif exchange_name.lower() == 'alpaca':
            from trading_system.connectors.alpaca import AlpacaConnector
            connector = AlpacaConnector(api_key=api_key, api_secret=api_secret)
        elif exchange_name.lower() == 'binance':
            from trading_system.connectors.binance import BinanceConnector
            connector = BinanceConnector(api_key=api_key)
        elif exchange_name.lower() == 'kraken':
            from trading_system.connectors.kraken import KrakenConnector
            connector = KrakenConnector()
        else:
            raise ValueError(f"Unknown exchange: {exchange_name}")
        
        # Connect if API credentials provided
        if api_key or api_secret:
            await connector.connect()
            
        return connector
