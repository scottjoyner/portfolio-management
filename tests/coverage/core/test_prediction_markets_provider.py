"""Tests for trading_system.core.providers.prediction_markets."""
import unittest

from trading_system.core.providers.prediction_markets import (
    KalshiProvider,
    PolymarketProvider,
)


class TestKalshiProvider(unittest.IsolatedAsyncioTestCase):
    def test_get_name(self):
        self.assertEqual(KalshiProvider().get_name(), "Kalshi")

    async def test_lifecycle(self):
        p = KalshiProvider()
        self.assertFalse(p._connected)
        await p.connect()
        self.assertTrue(p._connected)
        await p.disconnect()
        self.assertFalse(p._connected)

    async def test_get_current_prices(self):
        p = KalshiProvider()
        prices = await p.get_current_prices(["m1", "m2"])
        self.assertEqual(prices, {"m1": 1.0, "m2": 1.0})

    async def test_get_historical_prices(self):
        p = KalshiProvider()
        self.assertEqual(await p.get_historical_prices("m1", "2024-01-01", "2024-01-02"), [])


class TestPolymarketProvider(unittest.IsolatedAsyncioTestCase):
    def test_get_name(self):
        self.assertEqual(PolymarketProvider().get_name(), "Polymarket")

    async def test_lifecycle(self):
        p = PolymarketProvider()
        self.assertFalse(p._connected)
        await p.connect()
        self.assertTrue(p._connected)
        await p.disconnect()
        self.assertFalse(p._connected)

    async def test_get_current_prices(self):
        p = PolymarketProvider()
        prices = await p.get_current_prices(["m1"])
        self.assertEqual(prices, {"m1": 0.5})

    async def test_get_historical_prices(self):
        p = PolymarketProvider()
        self.assertEqual(await p.get_historical_prices("m1", "2024-01-01", "2024-01-02"), [])


if __name__ == "__main__":
    unittest.main()
