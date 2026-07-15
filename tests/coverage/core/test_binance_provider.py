"""Tests for trading_system.core.providers.binance (BinanceProvider)."""
import unittest

from trading_system.core.providers.binance import BinanceProvider


class TestBinanceProvider(unittest.IsolatedAsyncioTestCase):
    def test_get_name(self):
        self.assertEqual(BinanceProvider().get_name(), "Binance")

    async def test_connect_public(self):
        p = BinanceProvider()
        await p.connect()
        self.assertTrue(p._connected)

    async def test_connect_valid_key(self):
        p = BinanceProvider(api_key="binance123", api_secret="s")
        await p.connect()
        self.assertTrue(p._connected)

    async def test_connect_invalid_key(self):
        # ValueError swallowed; stays disconnected.
        p = BinanceProvider(api_key="badkey", api_secret="s")
        await p.connect()
        self.assertFalse(p._connected)

    async def test_connect_secret_only(self):
        # Without a valid binance-key format the live branch fails (swallowed).
        p = BinanceProvider(api_secret="s")
        await p.connect()
        self.assertFalse(p._connected)

    async def test_disconnect(self):
        p = BinanceProvider()
        await p.connect()
        await p.disconnect()
        self.assertFalse(p._connected)

    async def test_get_current_prices_all_usdt_connected(self):
        p = BinanceProvider()
        await p.connect()
        prices = await p.get_current_prices(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        self.assertEqual(prices["BTCUSDT"], 69250.45)
        self.assertEqual(prices["ETHUSDT"], 3845.23)
        self.assertEqual(prices["SOLUSDT"], 174.56)

    async def test_get_current_prices_not_connected_usdt(self):
        p = BinanceProvider()
        prices = await p.get_current_prices(["BTCUSDT"])
        self.assertEqual(prices["BTCUSDT"], 69250.45)

    async def test_get_current_prices_not_connected_non_usdt(self):
        p = BinanceProvider()
        prices = await p.get_current_prices(["FOO"])
        self.assertEqual(prices["FOO"], 0.0)

    async def test_get_current_prices_dash_symbol(self):
        p = BinanceProvider()
        await p.connect()
        prices = await p.get_current_prices(["BTC-PERP"])
        self.assertEqual(prices["BTC-PERP"], 0.0)

    async def test_get_current_prices_unknown(self):
        p = BinanceProvider()
        await p.connect()
        prices = await p.get_current_prices(["XYZUSDT"])
        self.assertEqual(prices["XYZUSDT"], 0.0)

    async def test_get_historical_prices(self):
        p = BinanceProvider()
        self.assertEqual(await p.get_historical_prices("BTCUSDT", "2024-01-01", "2024-01-02"), [])

    async def test_get_order_book(self):
        p = BinanceProvider()
        ob = await p.get_order_book("BTCUSDT", limit=3)
        self.assertEqual(ob["symbol"], "BTCUSDT")
        self.assertEqual(len(ob["asks"]), 3)
        self.assertEqual(len(ob["bids"]), 3)


if __name__ == "__main__":
    unittest.main()
