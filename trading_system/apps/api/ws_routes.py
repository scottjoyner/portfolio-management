from __future__ import annotations

import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from core.events.ws_hub import hub

log = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/ws/orders")
async def ws_orders(websocket: WebSocket) -> None:
    await websocket.accept()
    await hub.subscribe("orders", websocket)
    log.info("ws_client_connected channel=orders")
    try:
        while True:
            msg = await websocket.receive_text()
            data = json.loads(msg)
            from core.events.ws_hub import hub as h
            await h.publish("orders", {"event": "client_message", "data": data, "client_id": id(websocket)})
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
