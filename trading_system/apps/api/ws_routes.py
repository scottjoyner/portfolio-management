from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

log = logging.getLogger(__name__)

router = APIRouter()


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


hub = PubSubHub()


@router.websocket("/ws/orders")
async def ws_orders(websocket: WebSocket) -> None:
    await websocket.accept()
    await hub.subscribe("orders", websocket)
    log.info("ws_client_connected channel=orders")
    try:
        while True:
            msg = await websocket.receive_text()
            data = json.loads(msg)
            await hub.publish("orders", {"event": "client_message", "data": data, "client_id": id(websocket)})
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("ws_orders_error")
    finally:
        await hub.unsubscribe("orders", websocket)
        log.info("ws_client_disconnected channel=orders")


@router.websocket("/ws/market/{product_id}")
async def ws_market(websocket: WebSocket, product_id: str) -> None:
    await websocket.accept()
    channel = f"market:{product_id}"
    await hub.subscribe(channel, websocket)
    log.info("ws_client_connected channel=%s", channel)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("ws_market_error")
    finally:
        await hub.unsubscribe(channel, websocket)
        log.info("ws_client_disconnected channel=%s", channel)
