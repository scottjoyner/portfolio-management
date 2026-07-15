"""Tests for trading_system.core.providers.kraken (KrakenProvider)."""
import unittest

from trading_system.core.providers.kraken import KrakenProvider


class TestKrakenProvider(unittest.IsolatedAsyncioTestCase):
    def test_get_name(self):
        self.assertEqual(KrakenProvider().get_name(), "Kraken")

    async def test_connect_public(self):
        p = KrakenProvider()
        await p.connect()
        self.assertTrue(p._connected)

    async def test_connect_valid(self):
        p = KrakenProvider(api_key="abc_def", api_secret="x" * 20)
        await p.connect()
        self.assertTrue(p._connected)

    async def test_connect_invalid_key_format(self):
        p = KrakenProvider(api_key="abc-def", api_secret="x" * 20)
        await p.connect()
        self.assertFalse(p._connected)

    async def test_connect_short_secret(self):
        p = KrakenProvider(api_key="abcdef", api_secret="short")
        await p.connect()
        self.assertFalse(p._connected)

    async def test_disconnect(self):
        p = KrakenProvider()
        await p.connect()
        await p.disconnect()
        self.assertFalse(p._connected)

    async def test_get_current_prices_connected(self):
        p = KrakenProvider()
        await p.connect()
        prices = await p.get_current_prices(["XBT/USD", "ETH/USD", "FOO"])
        # Resolves by full pair first, then by base, then 0.0 for unknown.
        self.assertEqual(prices["XBT/USD"], 69250.45)
        self.assertEqual(prices["ETH/USD"], 3845.23)
        self.assertEqual(prices["FOO"], 0.0)

    async def test_get_current_prices_not_connected_slash(self):
        p = KrakenProvider()
        prices = await p.get_current_prices(["XBT/USD"])
        self.assertEqual(prices["XBT/USD"], 69250.45)

    async def test_get_current_prices_not_connected_no_slash(self):
        p = KrakenProvider()
        prices = await p.get_current_prices(["FOO"])
        self.assertEqual(prices["FOO"], 0.0)

    async def test_get_historical_prices(self):
        p = KrakenProvider()
        self.assertEqual(await p.get_historical_prices("XBT/USD", "2024-01-01", "2024-01-02"), [])

    async def test_get_order_book(self):
        p = KrakenProvider()
        ob = await p.get_order_book("XBT/USD", count=4)
        self.assertEqual(ob["asks"][0]["price"], 69250.45)
        self.assertEqual(len(ob["bids"]), 4)


if __name__ == "__main__":
    unittest.main()
