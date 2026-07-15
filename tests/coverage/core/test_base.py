"""Tests for trading_system.core.providers.base (BaseProvider ABC)."""
import unittest

from trading_system.core.providers.base import BaseProvider


class _Concrete(BaseProvider):
    async def connect(self):
        await super().connect()

    async def disconnect(self):
        await super().disconnect()

    async def get_current_prices(self, symbols):
        await super().get_current_prices(symbols)

    def get_name(self):
        return "concrete"


class TestBaseProvider(unittest.IsolatedAsyncioTestCase):
    def test_init(self):
        p = _Concrete("k", "s")
        self.assertEqual(p.api_key, "k")
        self.assertEqual(p.api_secret, "s")
        self.assertFalse(p._connected)

    def test_init_defaults(self):
        p = _Concrete()
        self.assertIsNone(p.api_key)
        self.assertIsNone(p.api_secret)

    async def test_abstract_bodies(self):
        p = _Concrete()
        await p.connect()
        await p.disconnect()
        await p.get_current_prices(["X"])
        self.assertEqual(p.get_name(), "concrete")

    async def test_get_historical_prices_default(self):
        p = _Concrete()
        self.assertEqual(
            await p.get_historical_prices("X", "2024-01-01", "2024-01-02"), []
        )
        self.assertEqual(
            await p.get_historical_prices("X", "2024-01-01", "2024-01-02", granularity=60),
            [],
        )


if __name__ == "__main__":
    unittest.main()
