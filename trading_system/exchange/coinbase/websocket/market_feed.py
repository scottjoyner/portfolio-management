from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Coroutine

try:
    from websockets import WebSocketClientProtocol
    import websockets
except ImportError:
    WebSocketClientProtocol = Any  # type: ignore[assignment]
    websockets = None

try:
    from ..auth.jwt import build_jwt_token
except ImportError:
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
        self._any_handlers: list[MessageHandler] = []
        self._running = False
        self._subscribed_channels: list[dict[str, Any]] = []
        self._auth_token: str | None = None
        self._connection_lock = asyncio.Lock()

    def on(self, channel: str, handler: MessageHandler) -> None:
        """Register a message handler for a specific channel."""
        self._handlers.setdefault(channel, []).append(handler)

    def on_any(self, handler: MessageHandler) -> None:
        """Register a handler for every decoded websocket message."""
        self._any_handlers.append(handler)

    def subscribe(self, product_id: str, channel: str = "ticker") -> dict[str, Any]:
        """
        Build subscription object for specified product and channel.

        Args:
            product_id: e.g., "BTC-USD", "ETH-USD"
            channel: "ticker", "level2", "heartbeats", or "candles"

        Returns:
            Subscription dict ready to send to Coinbase WS
        """
        if self.api_key and self.api_secret:
            token = build_jwt_token(self.api_key, self.api_secret, "GET", "/ws")
        else:
            token = ""

        sub = {
            "type": "subscribe",
            "channel": channel,
            "product_ids": [product_id],
        }
        if token:
            sub["token"] = token
        self._subscribed_channels.append(sub)
        return sub

    async def _ensure_connected(self) -> None:
        """Ensure websocket is connected."""
        if websockets is None:
            raise RuntimeError("websockets package is not installed")
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

    async def _emit_any(self, message: dict[str, Any]) -> None:
        """Emit message to raw-message handlers."""
        for handler in self._any_handlers:
            try:
                await handler(message)
            except Exception as e:
                log.exception("raw handler error: %s", e)

    async def connect(self) -> None:
        """Connect to Coinbase WebSocket."""
        if websockets is None:
            raise RuntimeError("websockets package is not installed")
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

                for sub in self._subscribed_channels:
                    try:
                        await ws.send(json.dumps(sub))
                    except Exception:
                        log.exception("failed to send subscription")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        await self._emit_any(msg)
                        
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
                                
                    except json.JSONDecodeError:
                        log.warning("invalid ws message: %s", raw[:200])

            except Exception as e:
                if websockets is not None and isinstance(e, websockets.ConnectionClosed):
                    log.info("ws disconnected, will reconnect...")
                    self._ws = None
                    await asyncio.sleep(2)
                    continue
                raise
                
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
