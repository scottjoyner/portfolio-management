"""Tests for trading_system/connectors/alpaca.py (mock-data Alpaca connector)."""

import asyncio
import unittest
from unittest.mock import patch

from trading_system.connectors.alpaca import (
    AlpacaConnector,
    AlpacaConnectorError,
    AuthenticationError,
    OrderError,
    AssetNotFoundError,
)


def run(coro):
    return asyncio.run(coro)


class TestAlpacaConnectorInit(unittest.TestCase):
    def test_init_defaults(self):
        c = AlpacaConnector()
        self.assertEqual(c.api_key, "pk_test_placeholder")
        self.assertEqual(c.api_secret, "")
        self.assertTrue(c.paper_trading)
        self.assertEqual(c.base_url, "https://paper-api.alpaca.markets")

    def test_init_with_params(self):
        c = AlpacaConnector(api_key="pk_live_x", api_secret="sec", paper_trading=False)
        self.assertEqual(c.api_key, "pk_live_x")
        self.assertEqual(c.api_secret, "sec")
        self.assertFalse(c.paper_trading)
        self.assertEqual(c.base_url, "https://api.alpaca.markets")

    def test_exception_hierarchy(self):
        self.assertTrue(issubclass(AuthenticationError, AlpacaConnectorError))
        self.assertTrue(issubclass(OrderError, AlpacaConnectorError))
        self.assertTrue(issubclass(AssetNotFoundError, AlpacaConnectorError))


class TestAlpacaConnect(unittest.TestCase):
    def test_connect_paper(self):
        c = AlpacaConnector()
        run(c.connect())
        self.assertTrue(c._connected)

    def test_connect_live_with_secret(self):
        c = AlpacaConnector(api_key="pk_live_abc", api_secret="secret", paper_trading=False)
        run(c.connect())
        self.assertTrue(c._connected)

    def test_connect_live_no_secret_raises(self):
        c = AlpacaConnector(api_key="pk_live_abc", paper_trading=False)
        with self.assertRaises(AuthenticationError):
            run(c.connect())
        self.assertFalse(c._connected)

    def test_disconnect(self):
        c = AlpacaConnector()
        c._connected = True
        run(c.disconnect())
        self.assertFalse(c._connected)


class TestAlpacaPrices(unittest.TestCase):
    def test_current_prices_not_connected(self):
        c = AlpacaConnector()
        prices = run(c.get_current_prices(["AAPL"]))
        self.assertIn("AAPL", prices)

    def test_current_prices_stocks(self):
        c = AlpacaConnector()
        c._connected = True
        prices = run(c.get_current_prices(["AAPL", "msft", "TSLA"]))
        self.assertEqual(prices["AAPL"], 175.43)
        self.assertEqual(prices["msft"], 420.22)
        self.assertEqual(prices["TSLA"], 198.45)

    def test_current_prices_crypto(self):
        c = AlpacaConnector()
        c._connected = True
        prices = run(c.get_current_prices(["BTCUSD", "ETHUSD"]))
        self.assertEqual(prices["BTCUSD"], 69250.45)
        self.assertEqual(prices["ETHUSD"], 3845.23)

    def test_current_prices_unknown(self):
        c = AlpacaConnector()
        c._connected = True
        prices = run(c.get_current_prices(["FOOCOIN"]))
        self.assertEqual(prices["FOOCOIN"], 0.0)

    def test_current_prices_empty(self):
        c = AlpacaConnector()
        c._connected = True
        self.assertEqual(run(c.get_current_prices([])), {})


class TestAlpacaHistorical(unittest.TestCase):
    def test_historical_prices(self):
        c = AlpacaConnector()
        c._connected = True
        bars = run(c.get_historical_prices("AAPL", "2024-01-01", "2024-01-05"))
        self.assertEqual(len(bars), 5)
        for b in bars:
            self.assertIn("open", b)
            self.assertIn("close", b)
            self.assertIn("volume", b)

    def test_historical_prices_not_connected(self):
        c = AlpacaConnector()
        bars = run(c.get_historical_prices("AAPL", "2024-01-01", "2024-01-01"))
        self.assertEqual(len(bars), 1)


class TestAlpacaMarketStatus(unittest.TestCase):
    def test_market_status(self):
        c = AlpacaConnector()
        status = run(c.get_market_status())
        self.assertEqual(status["market_state"], "open")
        self.assertFalse(status["trading_halted"])


if __name__ == "__main__":
    unittest.main()
