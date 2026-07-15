"""Tests for trading_system.core.providers.coinbase (CoinbaseProvider)."""
import unittest
from unittest.mock import MagicMock, patch

from trading_system.core.providers.coinbase import CoinbaseProvider


class _FakeCb:
    def __init__(self, *a, **k):
        pass

    def get_price(self, product_id):
        return {"price": 123.0}


class TestCoinbaseProvider(unittest.IsolatedAsyncioTestCase):
    def test_get_name(self):
        self.assertEqual(CoinbaseProvider().get_name(), "Coinbase")

    async def test_connect_with_key(self):
        p = CoinbaseProvider(api_key="k")
        await p.connect()
        self.assertTrue(p._connected)

    async def test_connect_public(self):
        p = CoinbaseProvider()
        await p.connect()
        self.assertTrue(p._connected)

    async def test_disconnect(self):
        p = CoinbaseProvider()
        await p.connect()
        await p.disconnect()
        self.assertFalse(p._connected)

    async def test_get_current_prices_not_connected(self):
        p = CoinbaseProvider()
        with patch(
            "trading_system.connectors.coinbase_v3.CoinbaseConnectorV3",
            _FakeCb,
        ):
            prices = await p.get_current_prices(["BTC-USD"])
        self.assertEqual(prices["BTC-USD"], 123.0)

    async def test_get_current_prices_connected(self):
        p = CoinbaseProvider()
        await p.connect()
        with patch(
            "trading_system.connectors.coinbase_v3.CoinbaseConnectorV3",
            _FakeCb,
        ):
            prices = await p.get_current_prices(["ETH-USD", "FOO"])
        self.assertEqual(prices["ETH-USD"], 123.0)
        self.assertEqual(prices["FOO"], 123.0)

    async def test_get_current_prices_fallback(self):
        p = CoinbaseProvider()
        await p.connect()

        class _Boom:
            def __init__(self, *a, **k):
                pass

            def get_price(self, product_id):
                raise RuntimeError("no cli")

        with patch(
            "trading_system.connectors.coinbase_v3.CoinbaseConnectorV3", _Boom
        ):
            prices = await p.get_current_prices(["BTC-USD", "LINK-USD", "ZZZ"])
        self.assertEqual(prices["BTC-USD"], 69250.45)
        self.assertEqual(prices["LINK-USD"], 18.45)
        self.assertEqual(prices["ZZZ"], 0.0)


if __name__ == "__main__":
    unittest.main()
