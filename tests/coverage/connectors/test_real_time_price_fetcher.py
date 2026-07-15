"""Tests for trading_system.connectors.real_time_price_fetcher."""
import asyncio
import os
import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock

from trading_system.connectors.real_time_price_fetcher import (
    RateLimiter,
    CoinbaseLiveConnector,
    AlpacaLiveConnector,
    LivePriceFetcher,
    main as rt_main,
)


class TestRateLimiter(unittest.IsolatedAsyncioTestCase):
    async def test_wait_no_sleep(self):
        rl = RateLimiter(requests_per_second=1000)
        with patch("trading_system.connectors.real_time_price_fetcher.asyncio.sleep") as sl:
            await rl.wait_if_needed()
            sl.assert_not_called()

    async def test_wait_sleeps(self):
        rl = RateLimiter(requests_per_second=1.0)
        rl.last_request_time = datetime.now().timestamp()
        with patch("trading_system.connectors.real_time_price_fetcher.asyncio.sleep") as sl:
            await rl.wait_if_needed()
            sl.assert_called_once()


class TestCoinbaseLiveConnector(unittest.IsolatedAsyncioTestCase):
    async def test_get_current_prices_known(self):
        c = CoinbaseLiveConnector()
        prices = await c.get_current_prices(["BTC-USD", "ETH-USD"])
        self.assertEqual(prices["BTC-USD"]["price"], 43500.25)

    async def test_get_current_prices_unknown(self):
        c = CoinbaseLiveConnector()
        prices = await c.get_current_prices(["FOO-USD"])
        self.assertIsNone(prices["FOO-USD"]["price"])
        self.assertIn("error", prices["FOO-USD"])

    async def test_get_current_prices_exception(self):
        # RuntimeError inside the try (join on a non-iterable) hits the except.
        c = CoinbaseLiveConnector()
        self.assertEqual(await c.get_current_prices(None), {})

    async def test_get_account_balances(self):
        c = CoinbaseLiveConnector()
        bal = await c.get_account_balances()
        self.assertIn("BTC-USD", bal)


class TestAlpacaLiveConnector(unittest.IsolatedAsyncioTestCase):
    async def test_get_current_prices_known(self):
        c = AlpacaLiveConnector()
        prices = await c.get_current_prices(["AAPL", "MSFT"])
        self.assertEqual(prices["AAPL"], 184.69)

    async def test_get_current_prices_unknown(self):
        c = AlpacaLiveConnector()
        prices = await c.get_current_prices(["FOO"])
        self.assertIsNone(prices["FOO"])

    async def test_get_positions(self):
        c = AlpacaLiveConnector()
        pos = await c.get_positions()
        self.assertEqual(len(pos), 2)


class TestLivePriceFetcher(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_all_prices(self):
        f = LivePriceFetcher()
        prices = await f.fetch_all_prices(
            ["BTC-USD", "ETH-USD", "AAPL", "MSFT", "FOO-USD"])
        self.assertIn("BTC-USD", prices)
        self.assertIn("AAPL", prices)
        self.assertNotIn("FOO-USD", prices)

    async def test_fetch_all_prices_exception(self):
        f = LivePriceFetcher()
        with patch.object(f.coinbase, "get_current_prices",
                          side_effect=RuntimeError("boom")):
            self.assertEqual(await f.fetch_all_prices(["BTC-USD"]), {})


class TestMain(unittest.IsolatedAsyncioTestCase):
    async def test_main(self):
        await rt_main()

    async def test_main_empty(self):
        orig = LivePriceFetcher.fetch_all_prices

        async def _empty(self, symbols):
            return {}

        LivePriceFetcher.fetch_all_prices = _empty
        try:
            await rt_main()
        finally:
            LivePriceFetcher.fetch_all_prices = orig


if __name__ == "__main__":
    unittest.main()
