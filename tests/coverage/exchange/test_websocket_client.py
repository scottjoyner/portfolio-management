"""Tests for trading_system.exchange.coinbase.websocket.client."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization

from trading_system.exchange.coinbase.websocket.client import CoinbaseWebSocketClient


def make_ec_pem() -> str:
    key = ec.generate_private_key(ec.SECP256R1())
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


class FakeWS:
    def __init__(self, client):
        self.client = client
        self.sent = []

    async def send(self, msg):
        self.sent.append(msg)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.client._running:
            self.client._running = False
            return json.dumps({"channel": "ticker", "type": "ticker"})
        raise StopAsyncIteration

    async def close(self):
        pass


class FakeWSRaise:
    def __init__(self, client):
        self.client = client

    async def send(self, msg):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise RuntimeError("boom")

    async def close(self):
        pass


def fake_ws_module(ws_factory):
    m = MagicMock()
    m.connect = AsyncMock(side_effect=lambda *a, **k: ws_factory())
    m.ConnectionClosed = type("ConnectionClosed", (Exception,), {})
    return m


class TestWebSocketClient(unittest.IsolatedAsyncioTestCase):
    def test_on(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        h = MagicMock()
        c.on("ticker", h)
        self.assertIn(h, c._handlers["ticker"])

    def test_build_token(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        tok = c._build_token()
        self.assertIsInstance(tok, str)
        self.assertIn(".", tok)

    async def test_connect_success(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        fwm = fake_ws_module(lambda: FakeWS(c))
        with patch("trading_system.exchange.coinbase.websocket.client.websockets", fwm):
            await c.connect()
            self.assertIsNotNone(c._ws)

    async def test_connect_no_websockets(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        with patch("trading_system.exchange.coinbase.websocket.client.websockets", None):
            with self.assertRaises(RuntimeError):
                await c.connect()

    async def test_emit_success(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        received = []
        await c._emit("ticker", {"channel": "ticker"})
        h = lambda m: received.append(m)
        c.on("ticker", h)
        await c._emit("ticker", {"channel": "ticker"})
        self.assertEqual(received, [{"channel": "ticker"}])

    async def test_emit_handler_error(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        def boom(m):
            raise ValueError("x")
        c.on("ticker", boom)
        # should not propagate
        await c._emit("ticker", {"channel": "ticker"})

    async def test_subscribe(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        fwm = fake_ws_module(lambda: FakeWS(c))
        with patch("trading_system.exchange.coinbase.websocket.client.websockets", fwm):
            await c.connect()
            await c.subscribe([{"name": "ticker", "product_ids": ["BTC-USD"]}])
            self.assertEqual(c._subscribed_channels[0]["name"], "ticker")

    async def test_subscribe_not_connected(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        with self.assertRaises(RuntimeError):
            await c.subscribe([{"name": "ticker"}])

    async def test_run(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        c._subscribed_channels = [{"name": "ticker", "product_ids": ["BTC-USD"]}]
        c.on("ticker", MagicMock())
        c.on("ticker", MagicMock())
        c.on("level2", MagicMock())
        fwm = fake_ws_module(lambda: FakeWS(c))
        with patch("trading_system.exchange.coinbase.websocket.client.websockets", fwm):
            await c.run()
            self.assertFalse(c._running)

    async def test_run_ws_preset(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        c._subscribed_channels = [{"name": "ticker", "product_ids": ["BTC-USD"]}]
        c._ws = FakeWS(c)
        fwm = fake_ws_module(lambda: FakeWS(c))
        with patch("trading_system.exchange.coinbase.websocket.client.websockets", fwm):
            await c.run()
            self.assertFalse(c._running)

    async def test_run_raises(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        c._subscribed_channels = [{"name": "ticker", "product_ids": ["BTC-USD"]}]
        c._ws = FakeWSRaise(c)
        fwm = fake_ws_module(lambda: FakeWSRaise(c))
        with patch("trading_system.exchange.coinbase.websocket.client.websockets", fwm):
            with self.assertRaises(RuntimeError):
                await c.run()

    async def test_run_bad_json(self):
        class FakeWSBadJson:
            def __init__(self, client):
                self.client = client

            async def send(self, msg):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.client._running:
                    self.client._running = False
                    return "this is not json"
                raise StopAsyncIteration

            async def close(self):
                pass

        c = CoinbaseWebSocketClient("k", make_ec_pem())
        c._subscribed_channels = [{"name": "ticker", "product_ids": ["BTC-USD"]}]
        fwm = fake_ws_module(lambda: FakeWSBadJson(c))
        with patch("trading_system.exchange.coinbase.websocket.client.websockets", fwm):
            await c.run()
            self.assertFalse(c._running)

    async def test_run_conn_closed(self):
        class FakeWSConnClosed:
            def __init__(self, client):
                self.client = client

            async def send(self, msg):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                raise fwm2.ConnectionClosed("closed")

            async def close(self):
                pass

        c = CoinbaseWebSocketClient("k", make_ec_pem())
        c._subscribed_channels = [{"name": "ticker", "product_ids": ["BTC-USD"]}]
        state = {"n": 0}

        def factory():
            state["n"] += 1
            if state["n"] == 1:
                return FakeWSConnClosed(c)
            return FakeWS(c)

        fwm2 = fake_ws_module(factory)
        with patch("trading_system.exchange.coinbase.websocket.client.websockets", fwm2), \
             patch("trading_system.exchange.coinbase.websocket.client.asyncio.sleep",
                   new=AsyncMock()):
            await c.run()
            self.assertFalse(c._running)

    async def test_run_ws_none(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        c._subscribed_channels = [{"name": "ticker", "product_ids": ["BTC-USD"]}]
        state = {"n": 0}

        def factory():
            state["n"] += 1
            if state["n"] == 1:
                return None
            return FakeWS(c)

        fwm2 = fake_ws_module(factory)
        with patch("trading_system.exchange.coinbase.websocket.client.websockets", fwm2):
            with self.assertRaises(RuntimeError):
                await c.run()

    async def test_stop_no_ws(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        await c.stop()
        self.assertFalse(c._running)
        self.assertIsNone(c._ws)

    async def test_close_no_ws(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        await c.close()
        self.assertIsNone(c._ws)

    async def test_stop(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        c._ws = FakeWS(c)
        await c.stop()
        self.assertFalse(c._running)
        self.assertIsNone(c._ws)

    async def test_close(self):
        c = CoinbaseWebSocketClient("k", make_ec_pem())
        c._ws = FakeWS(c)
        await c.close()
        self.assertIsNone(c._ws)


if __name__ == "__main__":
    unittest.main()
