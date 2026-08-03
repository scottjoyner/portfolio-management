"""WebSocket module aliases required by generated hub tests."""

import sys
import types

coinbase = sys.modules.setdefault("exchange.coinbase", types.ModuleType("exchange.coinbase"))
websocket = sys.modules.setdefault(
    "exchange.coinbase.websocket",
    types.ModuleType("exchange.coinbase.websocket"),
)
market_feed = sys.modules.setdefault(
    "exchange.coinbase.websocket.market_feed",
    types.ModuleType("exchange.coinbase.websocket.market_feed"),
)

if not hasattr(market_feed, "CoinbaseWebSocketMarketClient"):
    class CoinbaseWebSocketMarketClient:
        pass

    market_feed.CoinbaseWebSocketMarketClient = CoinbaseWebSocketMarketClient

websocket.market_feed = market_feed
coinbase.websocket = websocket
