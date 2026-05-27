from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Coroutine, List

from websockets import WebSocketClientProtocol
import websockets

from exchange.coinbase.auth.jwt import build_jwt_token

log = logging.getLogger(__name__)

MessageHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


class CoinbaseWebSocketMarketClient:
    """
    WebSocket client for real-time market data feeds (Candles & OrderBook).
    
    Subscribes to price updates and publishes to hub for worker consumption.
    All endpoints are READ-ONLY — no order placement capability exposed.
    """

    WS_URL = "wss://advanced-trade-ws.coinbase.com"

    def __init__(self, api_key: str | None = None, api_secret: str | None = None) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self._ws: WebSocketClientProtocol | None = None
        self._handlers: dict[str, list[MessageHandler]] = {}
        self._running = False
        self._subscribed_channels: list[dict[str, Any]] = []
        self._auth_token: str | None = None
        self._connection_lock = asyncio.Lock()

    def on(self, channel: str, handler: MessageHandler) -> None:
        """Register a message handler for a specific channel."""
        self._handlers.setdefault(channel, []).append(handler)

    def subscribe(self, product_id: str, channel_type: str = "snapshot_and_updates") -> dict[str, Any]:
        """
        Build subscription object for specified product and channel type.
        
        Args:
            product_id: e.g., "BTC-USD", "ETH-USD"
            channel_type: "snapshot_and_updates", "candles", or "orderbook"
            
        Returns:
            Subscription dict ready to send to Coinbase WS
        """
        if self.api_key and self.api_secret:
            token = build_jwt_token(self.api_key, self.api_secret, "GET", "/ws")
        else:
            # No auth for public market data (candles, orderbook)
            return {
                "type": "subscribe",
                "channel": channel_type,
                "product_ids": [product_id],
                "token": "",  # Empty for public endpoints
            }

        return {
            "type": "subscribe",
            "channel": channel_type,
            "product_ids": [product_id],
            "token": self._auth_token or "",
        }

    async def _ensure_connected(self) -> None:
        """Ensure websocket is connected."""
        async with self._connection_lock:
            if not self._ws or self._ws.closed:
                log.info("connecting to Coinbase WS...")
                token = build_jwt_token(
                    self.api_key or "", 
                    self.api_secret or "", 
                    "GET", "/ws"
                ) if self.api_key else ""
                uri = f"{self.WS_URL}?token={token}"
                self._ws = await websockets.connect(uri, ping_interval=20, ping_timeout=10)
            self._auth_token = build_jwt_token(
                self.api_key or "", 
                self.api_secret or "", 
                "GET", "/ws"
            ) if self.api_key else None

    async def _emit(self, channel: str, message: dict[str, Any]) -> None:
        """Emit message to registered handlers."""
        handlers = self._handlers.get(channel, [])
        for handler in handlers:
            try:
                await handler(message)
            except Exception as e:
                log.exception("handler error on channel %s: %s", channel, e)

    async def connect(self) -> None:
        """Connect to Coinbase WebSocket."""
        async with self._connection_lock:
            if not self._ws or self._ws.closed:
                token = build_jwt_token(
                    self.api_key or "", 
                    self.api_secret or "", 
                    "GET", "/ws"
                ) if self.api_key else ""
                uri = f"{self.WS_URL}?token={token}"
                self._ws = await websockets.connect(uri, ping_interval=20, ping_timeout=10)
                log.info("connected to %s", uri)

    async def run(self) -> None:
        """Run the websocket client loop."""
        self._running = True
        while self._running:
            try:
                await self._ensure_connected()
                ws = self._ws
                
                # Check for subscription messages from API (via hub integration)
                if self._subscribed_channels and not self._subscribed_channels[-1].get("token"):
                    log.warning("public channel subscriptions detected, no auth token")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        
                        # Emit to all channels
                        channel = msg.get("channel", msg.get("type", "unknown"))
                        await self._emit(channel, msg)
                        
                        # Also emit type-specific channel
                        for ch_name in list(self._handlers.keys()):
                            if ch_name not in [msg.get("channel"), msg.get("type")]:
                                continue
                            # Check if it's a broadcast to this handler (not direct sub)
                            handlers = self._handlers.get(ch_name, [])
                            if len(handlers) > 1:  # Multiple subscribers = hub pattern
                                await self._emit(ch_name, msg)
                                
                    except json.JSONDecodeError as e:
                        log.warning("invalid ws message: %s", raw[:200])

            except websockets.ConnectionClosed:
                log.info("ws disconnected, will reconnect...")
                self._ws = None
                await asyncio.sleep(2)
                
    async def stop(self) -> None:
        """Stop the websocket client."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                log.exception("error closing ws")
            self._ws = None

    async def close(self) -> None:
        """Close the websocket client."""
        await self.stop()
