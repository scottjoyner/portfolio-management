"""Tests for trading_system/connectors/binance.py (mock-data Binance connector)."""

import asyncio
import unittest

from trading_system.connectors.binance import (
    BinanceConnector,
    BinanceConnectorError,
    AuthenticationError,
    MarketUnavailableError,
)


def run(coro):
    return asyncio.run(coro)


class TestBinanceInit(unittest.TestCase):
    def test_init_defaults(self):
        c = BinanceConnector()
        self.assertEqual(c.api_key, "")
        self.assertEqual(c.api_secret, "")
        self.assertEqual(c.base_url, "https://api.binance.com")

    def test_init_with_keys(self):
        c = BinanceConnector(api_key="binance123", api_secret="s")
        self.assertEqual(c.api_key, "binance123")
        self.assertEqual(c.api_secret, "s")

    def test_exception_hierarchy(self):
        self.assertTrue(issubclass(AuthenticationError, BinanceConnectorError))
        self.assertTrue(issubclass(MarketUnavailableError, BinanceConnectorError))


class TestBinanceConnect(unittest.TestCase):
    def test_connect_public(self):
        c = BinanceConnector()
        run(c.connect())
        self.assertTrue(c._connected)

    def test_connect_valid_key(self):
        c = BinanceConnector(api_key="binance_abc")
        run(c.connect())
        self.assertTrue(c._connected)

    def test_connect_invalid_key_format(self):
        c = BinanceConnector(api_key="notbinance")
        with self.assertRaises(AuthenticationError):
            run(c.connect())

    def test_connect_secret_only_raises(self):
        c = BinanceConnector(api_secret="somesecret")
        with self.assertRaises(AuthenticationError):
            run(c.connect())

    def test_disconnect(self):
        c = BinanceConnector()
        c._connected = True
        run(c.disconnect())
        self.assertFalse(c._connected)


class TestBinancePrices(unittest.TestCase):
    def test_prices_not_connected_warning(self):
        c = BinanceConnector()
        prices = run(c.get_current_prices(["BTCUSDT"]))
        self.assertEqual(prices["BTCUSDT"], 69250.45)

    def test_prices_not_connected_no_warning(self):
        c = BinanceConnector()
        prices = run(c.get_current_prices(["BTCUSDT", "WEIRD"]))
        self.assertIn("BTCUSDT", prices)

    def test_prices_known(self):
        c = BinanceConnector()
        c._connected = True
        prices = run(c.get_current_prices(["BTCUSDT", "ETHUSDT", "SOLUSDT"]))
        self.assertEqual(prices["BTCUSDT"], 69250.45)
        self.assertEqual(prices["ETHUSDT"], 3845.23)
        self.assertEqual(prices["SOLUSDT"], 174.56)

    def test_prices_futures_dash(self):
        c = BinanceConnector()
        c._connected = True
        prices = run(c.get_current_prices(["BTC-PERP"]))
        self.assertEqual(prices["BTC-PERP"], 0)

    def test_prices_unknown(self):
        c = BinanceConnector()
        c._connected = True
        prices = run(c.get_current_prices(["ZZZUSDT"]))
        self.assertEqual(prices["ZZZUSDT"], 0)

    def test_prices_empty(self):
        c = BinanceConnector()
        c._connected = True
        self.assertEqual(run(c.get_current_prices([])), {})


class TestBinanceKlines(unittest.TestCase):
    def test_klines_not_connected(self):
        c = BinanceConnector()
        kl = run(c.get_historical_klines("BTCUSDT", "1h", 2))
        self.assertEqual(len(kl), 2)

    def test_klines(self):
        c = BinanceConnector()
        c._connected = True
        kl = run(c.get_historical_klines("BTCUSDT", "1h", 10))
        self.assertEqual(len(kl), 10)
        self.assertEqual(len(kl[0]), 6)

    def test_klines_minutes(self):
        c = BinanceConnector()
        c._connected = True
        kl = run(c.get_historical_klines("BTCUSDT", "5m", 3))
        self.assertEqual(len(kl), 3)


class TestBinanceOrderBook(unittest.TestCase):
    def test_order_book_default(self):
        c = BinanceConnector()
        ob = run(c.get_order_book("BTCUSDT"))
        self.assertEqual(len(ob["asks"]), 20)
        self.assertEqual(len(ob["bids"]), 20)

    def test_order_book_custom(self):
        c = BinanceConnector()
        ob = run(c.get_order_book("ETHUSDT", limit=5))
        self.assertEqual(len(ob["asks"]), 5)


class TestBinanceFuturesInfo(unittest.TestCase):
    def test_futures_info(self):
        c = BinanceConnector()
        info = run(c.get_futures_info())
        self.assertIn("BTC-PERP", info)
        self.assertIn("ETH-PERP", info)


if __name__ == "__main__":
    unittest.main()
