import asyncio
import unittest

from trading_system.backtest.adapter import MarketDataAdapter, MockMarketDataAdapter


class TestAdapter(unittest.TestCase):
    def test_abstract_cannot_instantiate(self):
        with self.assertRaises(TypeError):
            MarketDataAdapter()

    def test_mock_connect_disconnect(self):
        a = MockMarketDataAdapter()
        self.assertFalse(a.connected)
        self.assertTrue(asyncio.get_event_loop().run_until_complete(a.connect()))
        self.assertTrue(a.connected)
        asyncio.get_event_loop().run_until_complete(a.disconnect())
        self.assertFalse(a.connected)

    def test_get_current_price(self):
        a = MockMarketDataAdapter()
        self.assertEqual(a.get_current_price("BTC-USD"), 69000.0)
        self.assertEqual(a.get_current_price("SOL"), 170.0)

    def test_fetch_requires_connect(self):
        a = MockMarketDataAdapter()
        with self.assertRaises(RuntimeError):
            a.fetch_historical_data("BTC-USD", "2024-01-01", "2024-01-02")

    def test_fetch_returns_bars(self):
        a = MockMarketDataAdapter()
        asyncio.get_event_loop().run_until_complete(a.connect())
        bars = a.fetch_historical_data("BTC-USD", "2024-01-01", "2024-01-02")
        self.assertEqual(len(bars), 1)
        self.assertIn("close", bars[0])
        # unknown symbol falls back to default base price
        bars2 = a.fetch_historical_data("ZZZ-USD", "2024-01-01", "2024-01-03")
        self.assertEqual(len(bars2), 2)


if __name__ == "__main__":
    unittest.main()
