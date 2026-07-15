"""Tests for trading_system.core.providers.alpaca (AlpacaProvider)."""
import unittest
from unittest.mock import patch

from trading_system.core.providers.alpaca import AlpacaProvider


class TestAlpacaProvider(unittest.IsolatedAsyncioTestCase):
    def test_get_name(self):
        self.assertEqual(AlpacaProvider().get_name(), "Alpaca")

    def test_init_defaults(self):
        p = AlpacaProvider()
        self.assertTrue(p.paper_trading)
        self.assertIn("paper-api", p.base_url)

    def test_init_live(self):
        p = AlpacaProvider(paper_trading=False)
        self.assertIn("api.alpaca", p.base_url)

    async def test_connect_paper(self):
        p = AlpacaProvider(api_key="pk_test_xxx", paper_trading=True)
        await p.connect()
        self.assertTrue(p._connected)

    async def test_connect_key_endswith_test(self):
        p = AlpacaProvider(api_key="pk_xxx_test", paper_trading=False)
        await p.connect()
        self.assertTrue(p._connected)

    async def test_connect_live_with_secret(self):
        p = AlpacaProvider(api_key="pk_live", api_secret="sec", paper_trading=False)
        await p.connect()
        self.assertTrue(p._connected)

    async def test_connect_live_no_secret(self):
        # Source swallows the ValueError (prints) and leaves disconnected.
        p = AlpacaProvider(api_key="pk_live", paper_trading=False)
        await p.connect()
        self.assertFalse(p._connected)

    async def test_disconnect(self):
        p = AlpacaProvider()
        await p.connect()
        await p.disconnect()
        self.assertFalse(p._connected)

    async def test_get_current_prices_connected(self):
        p = AlpacaProvider()
        await p.connect()
        prices = await p.get_current_prices(["AAPL", "BTC-USD", "ETH-USD", "FOO"])
        self.assertEqual(prices["AAPL"], 175.43)
        self.assertEqual(prices["BTC-USD"], 69250.45)
        self.assertEqual(prices["ETH-USD"], 3845.23)
        self.assertEqual(prices["FOO"], 0.0)

    async def test_get_current_prices_not_connected(self):
        p = AlpacaProvider()
        prices = await p.get_current_prices(["MSFT"])
        self.assertEqual(prices["MSFT"], 420.22)

    async def test_get_historical_prices_connected(self):
        p = AlpacaProvider()
        await p.connect()
        bars = await p.get_historical_prices("AAPL", "2024-01-01", "2024-01-03")
        self.assertEqual(len(bars), 3)
        self.assertEqual(bars[0]["open"], 175.43)
        self.assertEqual(bars[0]["datetime"], "2024-01-01")
        self.assertEqual(bars[-1]["datetime"], "2024-01-03")

    async def test_get_historical_prices_not_connected(self):
        p = AlpacaProvider()
        bars = await p.get_historical_prices("AAPL", "2024-01-01", "2024-01-02")
        self.assertEqual(len(bars), 2)

    async def test_get_historical_prices_default_granularity(self):
        p = AlpacaProvider()
        await p.connect()
        bars = await p.get_historical_prices("AAPL", "2024-01-01", "2024-01-01")
        self.assertEqual(len(bars), 1)


if __name__ == "__main__":
    unittest.main()
