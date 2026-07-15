import asyncio
import json

from unittest.mock import AsyncMock, MagicMock

from fastapi import WebSocketDisconnect

from trading_system.apps.api import ws_routes, ws_market_routes


def run(coro):
    return asyncio.run(coro)


class FakeWS:
    def __init__(self, recv_side_effects):
        self._recv = iter(recv_side_effects)
        self.sent = []
        self.accepted = False

    async def accept(self):
        self.accepted = True

    async def receive_text(self):
        try:
            val = next(self._recv)
        except StopIteration:
            raise RuntimeError("closed")
        if isinstance(val, BaseException):
            raise val
        return val

    async def send_text(self, msg):
        self.sent.append(msg)


def _patch_hub(monkeypatch):
    hub = MagicMock()
    hub.subscribe = AsyncMock()
    hub.publish = AsyncMock()
    hub.unsubscribe = AsyncMock()
    # The module binds `hub` at import time AND re-imports it as `h` inside the
    # handler, so patch both the module global and the source module.
    monkeypatch.setattr(ws_routes, "hub", hub)
    import core.events.ws_hub as _wh

    monkeypatch.setattr(_wh, "hub", hub)
    return hub


def test_ws_orders_publish_and_disconnect(monkeypatch):
    hub = _patch_hub(monkeypatch)
    ws = FakeWS(['{"event": "hi"}', WebSocketDisconnect()])
    run(ws_routes.ws_orders(ws))
    assert ws.accepted is True
    hub.subscribe.assert_awaited_once_with("orders", ws)
    hub.publish.assert_awaited_once()
    hub.unsubscribe.assert_awaited_once_with("orders", ws)


def test_ws_orders_generic_exception(monkeypatch):
    hub = _patch_hub(monkeypatch)
    ws = FakeWS([ValueError("boom")])
    run(ws_routes.ws_orders(ws))
    hub.unsubscribe.assert_awaited_once_with("orders", ws)


def test_ws_market_disconnect(monkeypatch):
    hub = _patch_hub(monkeypatch)
    ws = FakeWS([WebSocketDisconnect()])
    run(ws_routes.ws_market(ws, "BTC-USD"))
    hub.subscribe.assert_awaited_once_with("market:BTC-USD", ws)
    hub.unsubscribe.assert_awaited_once_with("market:BTC-USD", ws)


def test_ws_market_generic_exception(monkeypatch):
    hub = _patch_hub(monkeypatch)
    ws = FakeWS([RuntimeError("x")])
    run(ws_routes.ws_market(ws, "ETH-USD"))
    hub.unsubscribe.assert_awaited_once_with("market:ETH-USD", ws)


def test_ws_market_feed_broadcast_and_disconnect(monkeypatch):
    ws = FakeWS(["hello", WebSocketDisconnect()])
    run(ws_market_routes.ws_market_feed(ws))
    assert ws.accepted is True
    # The first received text is echoed as a market update broadcast.
    assert ws.sent and json.loads(ws.sent[0])["data"] == "hello"


def test_ws_market_feed_send_failure(monkeypatch):
    ws = FakeWS(["hello", WebSocketDisconnect()])

    async def boom(msg):
        raise RuntimeError("send failed")

    ws.send_text = boom
    # Should not raise; the broadcast failure is logged and the loop continues
    # until the disconnect.
    run(ws_market_routes.ws_market_feed(ws))
    assert ws.accepted is True
