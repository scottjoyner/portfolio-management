"""Tests for trading_system.connectors.unified (UnifiedExchangeConnector)."""
import asyncio
import importlib
import os
import sys
import unittest
from types import ModuleType
from unittest.mock import patch

from trading_system.connectors.unified import (
    UnifiedExchangeConnector,
    ConnectionMode,
    create_exchange_connector,
)


class FakeConn:
    def __init__(self, *a, **k):
        self.calls = []
        self.raise_on_list = False

    async def list_accounts(self):
        if self.raise_on_list:
            raise RuntimeError("boom")
        return []

    async def connect(self):
        pass


def fake_mock_client_module():
    mod = ModuleType("trading_system.connectors.coinbase.mock_client")
    mod.CoinbaseRestClient = FakeConn
    mod.create_default_client = lambda: FakeConn()
    return mod


class TestUnified(unittest.IsolatedAsyncioTestCase):
    async def test_connect_not_called_health(self):
        c = UnifiedExchangeConnector("coinbase", mock_mode=ConnectionMode.UNKNOWN)
        h = await c.get_health_status()
        self.assertEqual(h["status"], "disconnected")

    async def test_coinbase_mock(self):
        fake = fake_mock_client_module()
        parent = ModuleType("trading_system.connectors.coinbase")
        parent.mock_client = fake
        with patch.dict(os.environ, {}, clear=False), \
             patch.dict(sys.modules, {"trading_system.connectors.coinbase": parent, "trading_system.connectors.coinbase.mock_client": fake}):
            c = UnifiedExchangeConnector("coinbase", mock_mode=ConnectionMode.UNKNOWN)
            rep = await c.connect()
            self.assertEqual(rep["mode"], "mock")
            self.assertTrue(c.is_connected)
            self.assertTrue(c.is_mock)
            h = await c.get_health_status()
            self.assertEqual(h["status"], "healthy")
            self.assertEqual(await c.list_accounts(), [])
            self.assertEqual(await c.get_current_prices(["BTC-USD"]), {"BTC-USD": None})
            await c.disconnect()
            self.assertFalse(c.is_connected)

    async def test_coinbase_live(self):
        fake = fake_mock_client_module()
        live = type("LiveRest", (FakeConn,), {})
        parent = ModuleType("trading_system.connectors.coinbase")
        rest = ModuleType("trading_system.connectors.coinbase.rest")
        rest_client = ModuleType("trading_system.connectors.coinbase.rest.client")
        rest_client.CoinbaseRestClient = live
        parent.rest = rest
        rest.client = rest_client
        with patch.dict(os.environ, {"COINBASE_API_KEY": "x" * 12}, clear=False), \
             patch.dict(sys.modules, {
                 "trading_system.connectors.coinbase": parent,
                 "trading_system.connectors.coinbase.mock_client": fake,
                 "trading_system.connectors.coinbase.rest": rest,
                 "trading_system.connectors.coinbase.rest.client": rest_client,
             }):
            c = UnifiedExchangeConnector("coinbase", mock_mode=ConnectionMode.UNKNOWN)
            rep = await c.connect()
            self.assertEqual(rep["mode"], "live")
            self.assertTrue(c.is_live)
            h = await c.get_health_status()
            self.assertEqual(h["status"], "healthy")
            self.assertEqual(await c.list_accounts(), [])

    async def test_alpaca_live(self):
        with patch.dict(os.environ, {"ALPACA_API_KEY": "realkey"}, clear=False), \
             patch("trading_system.connectors.alpaca_real.AlpacaRealConnector", FakeConn, create=True):
            c = UnifiedExchangeConnector("alpaca", mock_mode=ConnectionMode.UNKNOWN)
            rep = await c.connect()
            self.assertEqual(rep["mode"], "live")
            h = await c.get_health_status()
            self.assertEqual(h["status"], "healthy")
            self.assertEqual(await c.list_accounts(), [])

    async def test_alpaca_mock(self):
        with patch.dict(os.environ, {}, clear=False), \
             patch("trading_system.connectors.alpaca_real.AlpacaRealConnector", FakeConn, create=True):
            c = UnifiedExchangeConnector("alpaca", mock_mode=ConnectionMode.UNKNOWN)
            rep = await c.connect()
            self.assertEqual(rep["mode"], "mock")

    async def test_binance_live(self):
        with patch.dict(os.environ, {"BINANCE_API_KEY": "k"}, clear=False):
            c = UnifiedExchangeConnector("binance", mock_mode=ConnectionMode.UNKNOWN)
            rep = await c.connect()
            self.assertEqual(rep["mode"], "live")
            # no connector built -> health unknown
            h = await c.get_health_status()
            self.assertEqual(h["status"], "unknown")
            self.assertEqual(await c.list_accounts(), [])
            self.assertEqual(await c.get_current_prices(["BTC-USD"]), {"BTC-USD": None})

    async def test_kraken_mock(self):
        with patch.dict(os.environ, {}, clear=False):
            c = UnifiedExchangeConnector("kraken")
            await c.connect()
            h = await c.get_health_status()
            self.assertEqual(h["status"], "unknown")

    async def test_kalshi(self):
        c = UnifiedExchangeConnector("kalshi")
        await c.connect()
        self.assertEqual(await c.list_accounts(), [])

    async def test_polymarket_mock(self):
        with patch.dict(os.environ, {}, clear=False):
            c = UnifiedExchangeConnector("polymarket", mock_mode=ConnectionMode.UNKNOWN)
            await c.connect()
            self.assertIsNone(c._connector)

    async def test_polymarket_live(self):
        with patch.dict(os.environ, {"POLYGONZ_RPC_URL": "https://eth.alchemy.com"}, clear=False), \
             patch("trading_system.connectors.polymarket.PolymarketConnector", FakeConn):
            c = UnifiedExchangeConnector("polymarket", mock_mode=ConnectionMode.UNKNOWN)
            await c.connect()
            self.assertIsNotNone(c._connector)

    async def test_unknown_mode_autodetect(self):
        c = UnifiedExchangeConnector("binance", mock_mode=ConnectionMode.UNKNOWN)
        await c.connect()
        self.assertEqual(c.mode, ConnectionMode.MOCK)

    async def test_health_error(self):
        c = UnifiedExchangeConnector("coinbase", mock_mode=ConnectionMode.UNKNOWN)
        c._connector = FakeConn()
        c._connector.raise_on_list = True
        c.is_connected = True
        h = await c.get_health_status()
        self.assertEqual(h["status"], "error")

    async def test_health_no_list_accounts(self):
        c = UnifiedExchangeConnector("coinbase", mock_mode=ConnectionMode.UNKNOWN)
        c._connector = object()
        c.is_connected = True
        h = await c.get_health_status()
        self.assertEqual(h["status"], "unknown")
        self.assertEqual(await c.list_accounts(), [])

    async def test_list_accounts_no_connector(self):
        c = UnifiedExchangeConnector("coinbase", mock_mode=ConnectionMode.UNKNOWN)
        self.assertEqual(await c.list_accounts(), [])

    async def test_create_exchange_connector(self):
        c = create_exchange_connector("alpaca")
        self.assertIsInstance(c, UnifiedExchangeConnector)


class TestMainBlock(unittest.IsolatedAsyncioTestCase):
    async def test_main_block(self):
        import trading_system.connectors.unified as umod
        umod.__name__ = "__main__"
        try:
            importlib.reload(umod)
        finally:
            umod.__name__ = "trading_system.connectors.unified"


if __name__ == "__main__":
    unittest.main()
