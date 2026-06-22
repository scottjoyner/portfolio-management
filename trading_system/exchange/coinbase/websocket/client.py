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


class CoinbaseWebSocketClient:
    WS_URL = "wss://advanced-trade-ws.coinbase.com"

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self._ws: WebSocketClientProtocol | None = None
        self._handlers: dict[str, list[MessageHandler]] = {}
        self._running = False
        self._subscribed_channels: list[dict[str, Any]] = []

    def on(self, channel: str, handler: MessageHandler) -> None:
        self._handlers.setdefault(channel, []).append(handler)

    def _build_token(self) -> str:
        return build_jwt_token(self.api_key, self.api_secret, "GET", "/ws")

    async def connect(self) -> None:
        if websockets is None:
            raise RuntimeError("websockets package is not installed")
        token = self._build_token()
        uri = f"{self.WS_URL}?token={token}"
        self._ws = await websockets.connect(uri, ping_interval=20, ping_timeout=10)

    async def _emit(self, channel: str, message: dict[str, Any]) -> None:
        handlers = self._handlers.get(channel, [])
        for handler in handlers:
            try:
                await handler(message)
            except Exception:
                log.exception("handler error on channel %s", channel)

    async def subscribe(self, channels: list[dict[str, Any]]) -> None:
        if not self._ws:
            raise RuntimeError("not connected")
        msg = {
            "type": "subscribe",
            "channel": channels[0]["name"],
            "product_ids": channels[0].get("product_ids", []),
            "token": self._build_token(),
        }
        await self._ws.send(json.dumps(msg))
        self._subscribed_channels = channels

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                if not self._ws:
                    await self.connect()
                    for ch in self._subscribed_channels:
                        await self.subscribe([ch])
                ws = self._ws
                if ws is None:
                    continue
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        channel = msg.get("channel", msg.get("type", "unknown"))
                        await self._emit(channel, msg)
                    except json.JSONDecodeError:
                        log.warning("invalid ws message: %s", raw[:200])
            except Exception as e:
                if websockets is not None and isinstance(e, websockets.ConnectionClosed):
                    log.info("ws disconnected, reconnecting...")
                    self._ws = None
                    await asyncio.sleep(2)
                    continue
                raise

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def close(self) -> None:
        await self.stop()
