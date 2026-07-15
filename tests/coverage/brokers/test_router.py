import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock

from trading_system.brokers.router import BrokerRouter, BrokerRoutingDecision
from trading_system.brokers.base import (
    BrokerAdapter,
    BrokerOrder,
    BrokerAccount,
    BrokerFill,
    BrokerPosition,
)


class _FakeBroker(BrokerAdapter):
    def broker_name(self) -> str:
        return "fake"

    async def get_accounts(self):
        return [BrokerAccount("a", "n", "USD", Decimal("1"))]

    async def get_account(self, account_id):
        return BrokerAccount(account_id, "n", "USD", Decimal("1"))

    async def preview_order(self, order):
        return True, "ok"

    async def submit_order(self, order):
        return order

    async def cancel_order(self, broker_order_id):
        return True

    async def get_order(self, broker_order_id):
        return None

    async def list_orders(self, product_id=None, status=None):
        return []

    async def get_fills(self, broker_order_id):
        return [BrokerFill("f", broker_order_id, "BTC-USD", "buy", Decimal("1"), Decimal("1"), Decimal("1"))]

    async def get_positions(self, product_id=None):
        return [BrokerPosition("BTC-USD", "long", Decimal("1"), Decimal("1"))]

    async def list_products(self):
        return [{"product_id": "BTC-USD"}]

    async def get_product(self, product_id):
        return {"product_id": product_id}

    async def get_market_price(self, product_id):
        return Decimal("1")

    async def health_check(self):
        return {"status": "ok"}


def _mk_order():
    return BrokerOrder(
        broker_order_id="b1",
        client_order_id="c1",
        account_id="a1",
        product_id="BTC-USD",
        side="buy",
        order_type="market",
        size=Decimal("1.0"),
    )


class _FailingBroker(_FakeBroker):
    async def preview_order(self, order):
        return False, "rejected"


class TestBrokerRouter(unittest.TestCase):
    def test_register_and_get_and_list(self):
        r = BrokerRouter()
        b = _FakeBroker()
        r.register("coinbase", b, preferred=True)
        self.assertIs(r.get("coinbase"), b)
        self.assertIsNone(r.get("missing"))
        self.assertEqual(r.list_brokers(), ["coinbase"])
        # also exercise base non-abstract methods
        self.assertEqual(asyncio.run(b.get_market_price("BTC-USD")), Decimal("1"))
        self.assertEqual(asyncio.run(b.health_check()), {"status": "ok"})

    def test_route_paper_preferred(self):
        r = BrokerRouter()
        r.register("cbpaper", _FakeBroker(), preferred=True)
        r.register("cblive", _FakeBroker())
        d = r.route("BTC-USD", mode="paper")
        self.assertEqual(d.broker, "cbpaper")
        self.assertEqual(d.confidence, 1.0)

    def test_route_paper_preferred_skips_nonpaper(self):
        # preferred list has a non-paper broker first -> must skip to paper broker
        r = BrokerRouter()
        r.register("cblive", _FakeBroker(), preferred=True)
        r.register("cbpaper", _FakeBroker(), preferred=True)
        d = r.route("BTC-USD", mode="paper")
        self.assertEqual(d.broker, "cbpaper")
        self.assertEqual(d.confidence, 1.0)

    def test_route_live_preferred(self):
        r = BrokerRouter()
        r.register("cblive", _FakeBroker(), preferred=True)
        d = r.route("BTC-USD", mode="live")
        self.assertEqual(d.broker, "cblive")
        self.assertEqual(d.confidence, 0.95)

    def test_route_live_preferred_skips_paper(self):
        r = BrokerRouter()
        r.register("cbpaper", _FakeBroker(), preferred=True)
        r.register("cblive", _FakeBroker(), preferred=True)
        d = r.route("BTC-USD", mode="live")
        self.assertEqual(d.broker, "cblive")
        self.assertEqual(d.confidence, 0.95)

    def test_route_fallback_to_paper(self):
        r = BrokerRouter()
        r.register("onlypaper", _FakeBroker())
        d = r.route("BTC-USD", mode="live")
        self.assertEqual(d.broker, "onlypaper")
        self.assertEqual(d.confidence, 0.5)
        self.assertIn("falling back", d.reason)

    def test_route_no_broker_raises(self):
        r = BrokerRouter()
        with self.assertRaises(ValueError):
            r.route("BTC-USD", mode="paper")

    def test_route_no_paper_fallback_raises(self):
        r = BrokerRouter()
        r.register("cblive", _FakeBroker())
        with self.assertRaises(ValueError):
            r.route("BTC-USD", mode="live")

    def test_route_and_submit(self):
        r = BrokerRouter()
        r.register("cbpaper", _FakeBroker(), preferred=True)
        order = _mk_order()
        res = asyncio.run(r.route_and_submit(order, mode="paper"))
        self.assertIs(res, order)

    def test_route_and_submit_fails_preview(self):
        r = BrokerRouter()
        r.register("cbpaper", _FailingBroker(), preferred=True)
        order = _mk_order()
        with self.assertRaises(ValueError):
            asyncio.run(r.route_and_submit(order, mode="paper"))


if __name__ == "__main__":
    unittest.main()
