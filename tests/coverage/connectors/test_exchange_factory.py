"""Tests for trading_system/connectors/exchange_factory.py.

All connector classes are replaced with lightweight fakes so the test exercises
only the factory dispatch logic (no real imports / network). The `coinbase`
submodule does not exist in this checkout, so it is injected via sys.modules.
"""

import asyncio
import sys
import unittest
from unittest.mock import patch, MagicMock

from trading_system.connectors import exchange_factory
from trading_system.connectors.exchange_factory import (
    ExchangeFactory,
    ExchangeConnectorError,
    AuthenticationError,
    ConnectionTimeoutError,
    MarketUnavailableError,
)


def run(coro):
    return asyncio.run(coro)


class FakeConnector:
    def __init__(self, *args, **kwargs):
        self.args = (args, kwargs)
        self.connected = False

    async def connect(self):
        self.connected = True


class TestFactoryExceptions(unittest.TestCase):
    def test_exception_hierarchy(self):
        for exc in (AuthenticationError, ConnectionTimeoutError,
                    MarketUnavailableError):
            self.assertTrue(issubclass(exc, ExchangeConnectorError))


class TestFactoryDispatch(unittest.TestCase):
    def setUp(self):
        self.fake_coinbase_mod = MagicMock()
        self.fake_coinbase_mod.CoinbaseConnector = FakeConnector

    def _patch(self):
        return (
            patch.dict(
                sys.modules,
                {"trading_system.connectors.coinbase": self.fake_coinbase_mod},
            ),
            patch("trading_system.connectors.polymarket.PolymarketConnector", FakeConnector),
            patch("trading_system.connectors.kalshi.KalshiConnector", FakeConnector),
            patch("trading_system.connectors.alpaca.AlpacaConnector", FakeConnector),
            patch("trading_system.connectors.binance.BinanceConnector", FakeConnector),
            patch("trading_system.connectors.kraken.KrakenConnector", FakeConnector),
        )

    def test_coinbase_with_creds(self):
        p = self._patch()
        with p[0], p[1], p[2], p[3], p[4], p[5]:
            conn = run(ExchangeFactory.create_connector("coinbase", api_key="k"))
        self.assertIsInstance(conn, FakeConnector)
        self.assertTrue(conn.connected)
        self.assertEqual(conn.args[1]["api_key"], "k")

    def test_polymarket(self):
        p = self._patch()
        with p[0], p[1], p[2], p[3], p[4], p[5]:
            conn = run(ExchangeFactory.create_connector(
                "polymarket", api_key="k", rpc_url="http://x"))
        self.assertIsInstance(conn, FakeConnector)
        self.assertTrue(conn.connected)
        self.assertEqual(conn.args[1]["rpc_url"], "http://x")

    def test_kalshi(self):
        p = self._patch()
        with p[0], p[1], p[2], p[3], p[4], p[5]:
            conn = run(ExchangeFactory.create_connector("kalshi", api_key="k"))
        self.assertIsInstance(conn, FakeConnector)
        self.assertTrue(conn.connected)

    def test_alpaca(self):
        p = self._patch()
        with p[0], p[1], p[2], p[3], p[4], p[5]:
            conn = run(ExchangeFactory.create_connector("alpaca", api_key="k", api_secret="s"))
        self.assertIsInstance(conn, FakeConnector)
        self.assertTrue(conn.connected)
        self.assertEqual(conn.args[1]["api_secret"], "s")

    def test_binance(self):
        p = self._patch()
        with p[0], p[1], p[2], p[3], p[4], p[5]:
            conn = run(ExchangeFactory.create_connector("binance", api_key="k"))
        self.assertIsInstance(conn, FakeConnector)
        self.assertTrue(conn.connected)

    def test_kraken_no_creds(self):
        p = self._patch()
        with p[0], p[1], p[2], p[3], p[4], p[5]:
            conn = run(ExchangeFactory.create_connector("kraken"))
        self.assertIsInstance(conn, FakeConnector)
        self.assertFalse(conn.connected)

    def test_case_insensitive(self):
        p = self._patch()
        with p[0], p[1], p[2], p[3], p[4], p[5]:
            conn = run(ExchangeFactory.create_connector("BINANCE", api_key="k"))
        self.assertIsInstance(conn, FakeConnector)
        self.assertTrue(conn.connected)

    def test_unknown_exchange_raises(self):
        p = self._patch()
        with p[0], p[1], p[2], p[3], p[4], p[5]:
            with self.assertRaises(ValueError):
                run(ExchangeFactory.create_connector("bitmex", api_key="k"))


if __name__ == "__main__":
    unittest.main()
