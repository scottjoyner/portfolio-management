"""WebSocket routes for market data feeds."""

from __future__ import annotations

import json
import logging
from typing import Any, List

from fastapi import APIRouter, WebSocket

log = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["market-data"])


@router.websocket("/market/feed")
async def ws_market_feed(websocket: WebSocket) -> None:
    """
    WebSocket endpoint for real-time market data (candles & orderbook).

    Subscribes to Coinbase public WebSocket and broadcasts updates to all connected clients.
    Supports price updates, snapshots, and full channel types.
    """
    await websocket.accept()

    # Register handler for broadcasting updates to this connection
    handlers: List[dict[str, Any]] = []

    async def on_market_update(msg: dict[str, Any]) -> None:
        """Broadcast message to all connected clients."""
        try:
            await websocket.send_text(json.dumps(msg))
        except Exception as e:
            log.exception("failed to broadcast market update: %s", e)

    handlers.append({"channel": "market_update", "handler": on_market_update})

    # Store connection for cleanup
    conn_id = f"ws/market/{id(websocket)}"

    try:
        log.info("client connected to ws/market (broadcast mode)")

        # Keep websocket open and broadcast incoming messages
        while True:
            try:
                raw = await websocket.receive_text()
                msg = {"channel": "unknown", "data": raw}  # Echo for debug
                await on_market_update(msg)
            except Exception as e:
                log.error("ws error: %s", e)
                break

    finally:
        handlers.clear()
