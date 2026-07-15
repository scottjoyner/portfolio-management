"""Tests for trading_system.exchange.coinbase.rest.client (CoinbaseRestClient)."""
import unittest

from trading_system.exchange.coinbase.rest.client import CoinbaseRestClient


class TestCoinbaseRestClient(unittest.IsolatedAsyncioTestCase):
    async def test_init_and_list_products(self):
        c = CoinbaseRestClient("k", "s", "p")
        self.assertEqual(await c.list_products(), {"products": []})

    async def test_close(self):
        c = CoinbaseRestClient("k", "s", "p")
        await c.close()


if __name__ == "__main__":
    unittest.main()
