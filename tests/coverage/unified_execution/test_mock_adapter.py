import asyncio
import unittest
from decimal import Decimal

from trading_system.unified_execution.adapters.mock import MockCoinbaseAdapter
from trading_system.unified_execution.models import (
    UniversalAsset, UniversalOrder, OrderSide, OrderType, OrderStatus, HealthStatus,
)


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestMockAdapter(unittest.TestCase):
    def setUp(self):
        self.a = MockCoinbaseAdapter()
        self.asset = UniversalAsset(asset_id="BTC-USD", symbol="BTC",
                                    base_currency="BTC", quote_currency="USD")

    def test_properties(self):
        self.assertEqual(self.a.venue_name, "Mock Coinbase")
        self.assertEqual(self.a.supported_chains, {"base", "ethereum"})

    def test_accounts_and_balances(self):
        accts = run(self.a.get_accounts())
        self.assertEqual(len(accts), 1)
        self.assertEqual(accts[0].account_id, "mock_acc_1")
        self.assertEqual(len(accts[0].balances), 2)
        self.assertEqual(run(self.a.get_balances("mock_acc_1")), [])

    def test_ticker_orderbook(self):
        t = run(self.a.get_ticker(self.asset))
        self.assertEqual(t.bid_price, Decimal("60000.0"))
        ob = run(self.a.get_orderbook(self.asset, depth=5))
        self.assertEqual(len(ob.bids), 1)
        self.assertEqual(len(ob.asks), 1)

    def test_execute_and_lifecycle(self):
        order = UniversalOrder(order_id="o1", asset=self.asset, side=OrderSide.BUY,
                               order_type=OrderType.MARKET, size=Decimal("1"))
        res = run(self.a.execute_order(order))
        self.assertEqual(res.status, OrderStatus.FILLED)
        self.assertEqual(res.venue_order_id, "mock_order_123")
        self.assertTrue(run(self.a.cancel_order("v")))
        self.assertEqual(run(self.a.get_order_status("v")), OrderStatus.FILLED)
        self.assertEqual(run(self.a.health_check()), HealthStatus.HEALTHY)


if __name__ == "__main__":
    unittest.main()
