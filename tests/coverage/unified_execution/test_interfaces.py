import asyncio
import unittest
from decimal import Decimal
from datetime import datetime

from trading_system.unified_execution.interfaces import (
    ExchangeAdapter, UniversalAsset, UniversalOrder, OrderSide, OrderType,
    OrderStatus, HealthStatus, TickerInfo, Orderbook, UniversalAccount,
    UniversalBalance,
)


class ConcreteAdapter(ExchangeAdapter):
    """Concrete implementation that also invokes the abstract bodies via super()."""

    @property
    def venue_name(self):
        super().venue_name  # exercise abstract `...`
        return "Concrete"

    @property
    def supported_chains(self):
        super().supported_chains
        return set()

    async def get_accounts(self):
        await super().get_accounts()
        return []

    async def get_balances(self, account_id):
        await super().get_balances(account_id)
        return []

    async def get_ticker(self, asset):
        await super().get_ticker(asset)
        return None

    async def get_orderbook(self, asset, depth=10):
        await super().get_orderbook(asset, depth)
        return None

    async def execute_order(self, order):
        await super().execute_order(order)
        return order

    async def cancel_order(self, venue_order_id):
        await super().cancel_order(venue_order_id)
        return True

    async def get_order_status(self, venue_order_id):
        await super().get_order_status(venue_order_id)
        return OrderStatus.FILLED

    async def health_check(self):
        await super().health_check()
        return HealthStatus.HEALTHY


class TestInterfaces(unittest.TestCase):
    def setUp(self):
        self.a = ConcreteAdapter()
        self.asset = UniversalAsset(asset_id="BTC-USD", symbol="BTC",
                                    base_currency="BTC", quote_currency="USD")

    def test_cannot_instantiate_abstract(self):
        with self.assertRaises(TypeError):
            ExchangeAdapter()

    def test_properties(self):
        self.assertEqual(self.a.venue_name, "Concrete")
        self.assertEqual(self.a.supported_chains, set())

    def test_async_methods(self):
        loop = asyncio.new_event_loop()
        try:
            self.assertEqual(loop.run_until_complete(self.a.get_accounts()), [])
            self.assertEqual(loop.run_until_complete(self.a.get_balances("x")), [])
            self.assertIsNone(loop.run_until_complete(self.a.get_ticker(self.asset)))
            self.assertIsNone(loop.run_until_complete(self.a.get_orderbook(self.asset, 5)))
            order = UniversalOrder(order_id="o", asset=self.asset, side=OrderSide.BUY,
                                   order_type=OrderType.MARKET, size=Decimal("1"))
            self.assertIs(loop.run_until_complete(self.a.execute_order(order)), order)
            self.assertTrue(loop.run_until_complete(self.a.cancel_order("v")))
            self.assertEqual(loop.run_until_complete(self.a.get_order_status("v")), OrderStatus.FILLED)
            self.assertEqual(loop.run_until_complete(self.a.health_check()), HealthStatus.HEALTHY)
        finally:
            loop.close()


if __name__ == "__main__":
    unittest.main()
