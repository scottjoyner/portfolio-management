from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import WebSocket

log = logging.getLogger(__name__)


class PubSubHub:
    def __init__(self) -> None:
        self._channels: dict[str, set[WebSocket]] = {}

    async def subscribe(self, channel: str, ws: WebSocket) -> None:
        self._channels.setdefault(channel, set()).add(ws)

    async def unsubscribe(self, channel: str, ws: WebSocket) -> None:
        self._channels.get(channel, set()).discard(ws)

    async def publish(self, channel: str, message: dict[str, Any]) -> None:
        payload = json.dumps(message, default=str)
        for ws in list(self._channels.get(channel, set())):
            try:
                await ws.send_text(payload)
            except Exception:
                self._channels.get(channel, set()).discard(ws)

    def publish_sync(self, channel: str, message: dict[str, Any]) -> None:
        payload = json.dumps(message, default=str)
        for ws in list(self._channels.get(channel, set())):
            try:
                import asyncio
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    return
                if loop.is_running():
                    asyncio.ensure_future(ws.send_text(payload))
                else:
                    loop.run_until_complete(ws.send_text(payload))
            except Exception:
                self._channels.get(channel, set()).discard(ws)


hub = PubSubHub()
