import asyncio
import unittest
from decimal import Decimal
from unittest import mock

import trading_system.unified_execution.adapters.coinbase as cb_mod
from trading_system.unified_execution.adapters.coinbase import CoinbaseAdapter
from trading_system.unified_execution.models import (
    UniversalAsset, UniversalOrder, OrderSide, OrderType, OrderStatus, HealthStatus,
)


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestCoinbaseAdapter(unittest.TestCase):
    def setUp(self):
        self.asset = UniversalAsset(asset_id="BTC-USD", symbol="BTC",
                                    base_currency="BTC", quote_currency="USD")

    def test_init_with_client(self):
        fake_cls = mock.MagicMock(name="CBClient")
        with mock.patch.object(cb_mod, "CBClient", fake_cls):
            a = CoinbaseAdapter(api_key="k", api_secret="s", timeout=5)
        fake_cls.assert_called_once_with(api_key="k", api_secret="s", timeout=5)
        self.assertIsNotNone(a._client)
        self.assertEqual(a.venue_name, "Coinbase")
        self.assertEqual(a.supported_chains, {"base", "ethereum"})

    def test_init_without_client(self):
        with mock.patch.object(cb_mod, "CBClient", None):
            a = CoinbaseAdapter()
        self.assertIsNone(a._client)

    def _adapter_no_client(self):
        with mock.patch.object(cb_mod, "CBClient", None):
            return CoinbaseAdapter()

    def _adapter_with_client(self, client):
        with mock.patch.object(cb_mod, "CBClient", None):
            a = CoinbaseAdapter()
        a._client = client
        return a

    def test_get_accounts_no_client(self):
        a = self._adapter_no_client()
        self.assertEqual(run(a.get_accounts()), [])

    def test_get_accounts_success(self):
        client = mock.MagicMock()
        client.list_accounts.return_value = {
            "accounts": [
                {"id": "1", "name": "Main", "currency": "USD"},
                {},  # exercises defaults
            ]
        }
        a = self._adapter_with_client(client)
        accts = run(a.get_accounts())
        self.assertEqual(len(accts), 2)
        self.assertEqual(accts[0].account_id, "1")
        self.assertEqual(accts[1].currency, "USD")

    def test_get_accounts_exception(self):
        client = mock.MagicMock()
        client.list_accounts.side_effect = RuntimeError("boom")
        a = self._adapter_with_client(client)
        self.assertEqual(run(a.get_accounts()), [])

    def test_get_balances(self):
        a = self._adapter_no_client()
        self.assertEqual(run(a.get_balances("acc")), [])

    def test_get_ticker_success(self):
        client = mock.MagicMock()
        client.best_bid_ask.return_value = {"pricebooks": []}
        a = self._adapter_with_client(client)
        t = run(a.get_ticker(self.asset))
        self.assertEqual(t.bid_price, Decimal("0"))
        client.best_bid_ask.assert_called_once()

    def test_get_ticker_exception(self):
        client = mock.MagicMock()
        client.best_bid_ask.side_effect = ValueError("bad")
        a = self._adapter_with_client(client)
        t = run(a.get_ticker(self.asset))
        self.assertEqual(t.ask_price, Decimal("0"))

    def test_get_orderbook(self):
        a = self._adapter_no_client()
        ob = run(a.get_orderbook(self.asset, depth=3))
        self.assertEqual(ob.bids, [])
        self.assertEqual(ob.asks, [])

    def test_execute_order_no_client(self):
        a = self._adapter_no_client()
        order = UniversalOrder(order_id="o", asset=self.asset, side=OrderSide.BUY,
                               order_type=OrderType.MARKET, size=Decimal("1"))
        with self.assertRaises(RuntimeError):
            run(a.execute_order(order))

    def test_execute_order_success_buy(self):
        client = mock.MagicMock()
        client.market_order.return_value = {"id": "venue123"}
        a = self._adapter_with_client(client)
        order = UniversalOrder(order_id="o", asset=self.asset, side=OrderSide.BUY,
                               order_type=OrderType.MARKET, size=Decimal("1.5"))
        res = run(a.execute_order(order))
        self.assertEqual(res.status, OrderStatus.FILLED)
        self.assertEqual(res.venue_order_id, "venue123")
        _, kwargs = client.market_order.call_args
        self.assertEqual(kwargs["side"], "buy")

    def test_execute_order_success_sell(self):
        client = mock.MagicMock()
        client.market_order.return_value = {"id": "v2"}
        a = self._adapter_with_client(client)
        order = UniversalOrder(order_id="o", asset=self.asset, side=OrderSide.SELL,
                               order_type=OrderType.MARKET, size=Decimal("2"))
        res = run(a.execute_order(order))
        _, kwargs = client.market_order.call_args
        self.assertEqual(kwargs["side"], "sell")
        self.assertEqual(res.status, OrderStatus.FILLED)

    def test_execute_order_exception(self):
        client = mock.MagicMock()
        client.market_order.side_effect = RuntimeError("rejected")
        a = self._adapter_with_client(client)
        order = UniversalOrder(order_id="o", asset=self.asset, side=OrderSide.BUY,
                               order_type=OrderType.MARKET, size=Decimal("1"))
        res = run(a.execute_order(order))
        self.assertEqual(res.status, OrderStatus.REJECTED)

    def test_lifecycle_methods(self):
        a = self._adapter_no_client()
        self.assertTrue(run(a.cancel_order("v")))
        self.assertEqual(run(a.get_order_status("v")), OrderStatus.FILLED)
        self.assertEqual(run(a.health_check()), HealthStatus.HEALTHY)


if __name__ == "__main__":
    unittest.main()
