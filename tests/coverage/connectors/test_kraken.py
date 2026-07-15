"""Tests for trading_system/connectors/kraken.py (mock-data Kraken connector)."""

import asyncio
import unittest

from trading_system.connectors.kraken import (
    KrakenConnector,
    KrakenConnectorError,
    AuthenticationError,
    MarketUnavailableError,
)


def run(coro):
    return asyncio.run(coro)


class TestKrakenInit(unittest.TestCase):
    def test_init_defaults(self):
        c = KrakenConnector()
        self.assertEqual(c.api_key, "")
        self.assertEqual(c.api_secret, "")
        self.assertEqual(c.base_url, "https://api.kraken.com")

    def test_init_with_keys(self):
        c = KrakenConnector(api_key="KrakenAPIkey", api_secret="x" * 15)
        self.assertEqual(c.api_key, "KrakenAPIkey")

    def test_exception_hierarchy(self):
        self.assertTrue(issubclass(AuthenticationError, KrakenConnectorError))
        self.assertTrue(issubclass(MarketUnavailableError, KrakenConnectorError))


class TestKrakenConnect(unittest.TestCase):
    def test_connect_public(self):
        c = KrakenConnector()
        run(c.connect())
        self.assertTrue(c._connected)

    def test_connect_valid(self):
        c = KrakenConnector(api_key="KrakenAPIkey", api_secret="x" * 15)
        run(c.connect())
        self.assertTrue(c._connected)

    def test_connect_invalid_key(self):
        c = KrakenConnector(api_key="bad key", api_secret="x" * 15)
        with self.assertRaises(AuthenticationError):
            run(c.connect())

    def test_connect_short_secret(self):
        c = KrakenConnector(api_key="KrakenAPIkey", api_secret="short")
        with self.assertRaises(AuthenticationError):
            run(c.connect())

    def test_disconnect(self):
        c = KrakenConnector()
        c._connected = True
        run(c.disconnect())
        self.assertFalse(c._connected)


class TestKrakenPrices(unittest.TestCase):
    def test_prices_not_connected_warning(self):
        c = KrakenConnector()
        prices = run(c.get_current_prices(["XBT/USD"]))
        self.assertEqual(prices["XBT/USD"], 69250.45)

    def test_prices_connected(self):
        c = KrakenConnector()
        c._connected = True
        prices = run(c.get_current_prices(["XBT/USD", "ETH/USD", "SOL/USD"]))
        self.assertEqual(prices["XBT/USD"], 69250.45)
        self.assertEqual(prices["ETH/USD"], 3845.23)
        self.assertEqual(prices["SOL/USD"], 174.56)

    def test_prices_slash_base(self):
        c = KrakenConnector()
        c._connected = True
        prices = run(c.get_current_prices(["LINK/USD"]))
        self.assertEqual(prices["LINK/USD"], 18.45)

    def test_prices_no_slash(self):
        c = KrakenConnector()
        c._connected = True
        prices = run(c.get_current_prices(["XBTUSD"]))
        self.assertEqual(prices["XBTUSD"], 0.0)

    def test_prices_unknown(self):
        c = KrakenConnector()
        c._connected = True
        prices = run(c.get_current_prices(["ZZZ/USD"]))
        self.assertEqual(prices["ZZZ/USD"], 0.0)

    def test_prices_empty(self):
        c = KrakenConnector()
        c._connected = True
        self.assertEqual(run(c.get_current_prices([])), {})


class TestKrakenTrades(unittest.TestCase):
    def test_historical_trades_not_connected(self):
        c = KrakenConnector()
        trades = run(c.get_historical_trades("XBT/USD"))
        self.assertEqual(len(trades), 20)

    def test_historical_trades(self):
        c = KrakenConnector()
        c._connected = True
        trades = run(c.get_historical_trades("XBT/USD", since="2024-01-01"))
        self.assertEqual(len(trades), 20)
        self.assertIn("price", trades[0])


class TestKrakenOrderBook(unittest.TestCase):
    def test_order_book_default(self):
        c = KrakenConnector()
        ob = run(c.get_order_book("XBT/USD"))
        self.assertEqual(len(ob["asks"]), 25)
        self.assertEqual(len(ob["bids"]), 25)

    def test_order_book_custom(self):
        c = KrakenConnector()
        ob = run(c.get_order_book("ETH/USD", count=5))
        self.assertEqual(len(ob["asks"]), 5)


if __name__ == "__main__":
    unittest.main()
