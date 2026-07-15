"""Tests for trading_system.exchange.coinbase.websocket.market_feed."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization

from trading_system.exchange.coinbase.websocket.market_feed import (
    CoinbaseWebSocketMarketClient,
)


def make_ec_pem() -> str:
    key = ec.generate_private_key(ec.SECP256R1())
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


class FakeWS:
    def __init__(self, client, raw="valid", closed=False):
        self.client = client
        self.raw = raw
        self.closed = closed
        self.sent = []

    async def send(self, msg):
        self.sent.append(msg)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.client._running:
            self.client._running = False
            if self.raw == "bad":
                return "not json"
            return json.dumps({"channel": "ticker", "type": "ticker",
                                "price": "1"})
        raise StopAsyncIteration

    async def close(self):
        pass


class FakeWSSendError:
    def __init__(self, client):
        self.client = client

    async def send(self, msg):
        raise RuntimeError("send fail")

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.client._running:
            self.client._running = False
            return json.dumps({"channel": "ticker", "type": "ticker"})
        raise StopAsyncIteration

    async def close(self):
        pass


class FakeWSConnClosed:
    def __init__(self, client):
        self.client = client

    async def send(self, msg):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise fwm.ConnectionClosed("closed")

    async def close(self):
        pass


def fake_ws_module(ws_factory):
    m = MagicMock()
    m.connect = AsyncMock(side_effect=lambda *a, **k: ws_factory())
    m.ConnectionClosed = type("ConnectionClosed", (Exception,), {})
    return m


class TestMarketFeed(unittest.IsolatedAsyncioTestCase):
    def test_on(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        h = MagicMock()
        c.on("ticker", h)
        self.assertIn(h, c._handlers["ticker"])

    def test_on_any(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        h = MagicMock()
        c.on_any(h)
        self.assertIn(h, c._any_handlers)

    def test_subscribe_with_auth(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        sub = c.subscribe("BTC-USD", "ticker")
        self.assertEqual(sub["channel"], "ticker")
        self.assertEqual(sub["product_ids"], ["BTC-USD"])
        self.assertIn("token", sub)

    def test_subscribe_no_auth(self):
        c = CoinbaseWebSocketMarketClient(api_key="k", api_secret=None)
        sub = c.subscribe("BTC-USD", "ticker")
        self.assertNotIn("token", sub)

    async def test_emit(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        received = []
        c.on("ticker", lambda m: received.append(m))
        await c._emit("ticker", {"channel": "ticker"})
        self.assertEqual(received, [{"channel": "ticker"}])

    async def test_emit_handler_error(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        c.on("ticker", lambda m: (_ for _ in ()).throw(ValueError("x")))
        await c._emit("ticker", {"channel": "ticker"})  # no propagate

    async def test_emit_any(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        received = []
        c.on_any(lambda m: received.append(m))
        await c._emit_any({"channel": "ticker"})
        self.assertEqual(received, [{"channel": "ticker"}])

    async def test_emit_any_error(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        c.on_any(lambda m: (_ for _ in ()).throw(ValueError("x")))
        await c._emit_any({"channel": "ticker"})  # no propagate

    async def test_ensure_connected_no_websockets(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        with patch("trading_system.exchange.coinbase.websocket.market_feed.websockets", None):
            with self.assertRaises(RuntimeError):
                await c._ensure_connected()

    async def test_connect_success(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        fwm = fake_ws_module(lambda: FakeWS(c))
        with patch("trading_system.exchange.coinbase.websocket.market_feed.websockets", fwm):
            await c.connect()
            self.assertIsNotNone(c._ws)

    async def test_connect_no_websockets(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        with patch("trading_system.exchange.coinbase.websocket.market_feed.websockets", None):
            with self.assertRaises(RuntimeError):
                await c.connect()

    async def test_run(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        c.subscribe("BTC-USD", "ticker")
        c.on("ticker", MagicMock())
        c.on("ticker", MagicMock())
        c.on("level2", MagicMock())
        c.on_any(MagicMock())
        fwm = fake_ws_module(lambda: FakeWS(c))
        with patch("trading_system.exchange.coinbase.websocket.market_feed.websockets", fwm):
            await c.run()
            self.assertFalse(c._running)

    async def test_run_public_warning(self):
        c = CoinbaseWebSocketMarketClient(api_key="k", api_secret=None)
        c.subscribe("BTC-USD", "ticker")
        fwm = fake_ws_module(lambda: FakeWS(c))
        with patch("trading_system.exchange.coinbase.websocket.market_feed.websockets", fwm):
            await c.run()
            self.assertFalse(c._running)

    async def test_run_bad_json(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        c.subscribe("BTC-USD", "ticker")
        fwm = fake_ws_module(lambda: FakeWS(c, raw="bad"))
        with patch("trading_system.exchange.coinbase.websocket.market_feed.websockets", fwm):
            await c.run()
            self.assertFalse(c._running)

    async def test_run_send_error(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        c.subscribe("BTC-USD", "ticker")
        fwm = fake_ws_module(lambda: FakeWSSendError(c))
        with patch("trading_system.exchange.coinbase.websocket.market_feed.websockets", fwm):
            await c.run()
            self.assertFalse(c._running)

    async def test_run_conn_closed(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        c.subscribe("BTC-USD", "ticker")
        state = {"n": 0}

        def factory():
            state["n"] += 1
            if state["n"] == 1:
                return FakeWSConnClosed(c)
            return FakeWS(c)

        fwm_local = fake_ws_module(factory)
        with patch("trading_system.exchange.coinbase.websocket.market_feed.websockets", fwm_local), \
             patch("trading_system.exchange.coinbase.websocket.market_feed.asyncio.sleep", new=AsyncMock()):
            await c.run()
            self.assertFalse(c._running)

    async def test_run_ws_closed_reconnect(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        c.subscribe("BTC-USD", "ticker")
        c._ws = FakeWS(c, closed=True)
        fwm = fake_ws_module(lambda: FakeWS(c))
        with patch("trading_system.exchange.coinbase.websocket.market_feed.websockets", fwm):
            await c.run()
            self.assertFalse(c._running)

    async def test_stop(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        c._ws = FakeWS(c)
        await c.stop()
        self.assertFalse(c._running)
        self.assertIsNone(c._ws)

    async def test_stop_no_ws(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        await c.stop()
        self.assertIsNone(c._ws)

    async def test_close(self):
        c = CoinbaseWebSocketMarketClient("k", make_ec_pem())
        c._ws = FakeWS(c)
        await c.close()
        self.assertIsNone(c._ws)


if __name__ == "__main__":
    unittest.main()
