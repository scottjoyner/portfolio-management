from __future__ import annotations

import asyncio
import datetime
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock

from brokers.router import BrokerRouter, BrokerRoutingDecision
from brokers.base import (
    BrokerAccount,
    BrokerAdapter,
    BrokerFill,
    BrokerOrder,
    BrokerPosition,
    OrderStatus,
    TimeInForce,
)
from brokers.coinbase import CoinbaseBrokerAdapter
from brokers.paper import PaperBrokerAdapter


class FakeAdapter(BrokerAdapter):
    def __init__(self, name: str, preview_ok: bool = True, submit_raises: bool = False) -> None:
        self._name = name
        self.preview_ok = preview_ok
        self.submit_raises = submit_raises

    def broker_name(self) -> str:
        return self._name

    async def get_accounts(self):
        return []

    async def get_account(self, account_id):
        return None

    async def preview_order(self, order):
        return (self.preview_ok, "ok" if self.preview_ok else "bad")

    async def submit_order(self, order):
        if self.submit_raises:
            raise ValueError("submit failed")
        return order

    async def cancel_order(self, broker_order_id):
        return True

    async def get_order(self, broker_order_id):
        return None

    async def list_orders(self, product_id=None, status=None):
        return []

    async def get_fills(self, broker_order_id):
        return []

    async def get_positions(self, product_id=None):
        return []

    async def list_products(self):
        return []

    async def get_product(self, product_id):
        return None


def make_order(product_id="BTC-USD"):
    return BrokerOrder(
        broker_order_id="", client_order_id="c", account_id="acc",
        product_id=product_id, side="buy", order_type="limit",
        size=Decimal("1"), price=Decimal("100"),
    )


class TestBrokerBase(TestCase):
    def test_import_and_enums(self):
        self.assertEqual(OrderStatus.FILLED.value, "filled")
        self.assertEqual(TimeInForce.GTC.value, "GTC")
        o = BrokerOrder(broker_order_id="b", client_order_id="c", account_id="a",
                        product_id="BTC-USD", side="buy", order_type="limit", size=Decimal("1"))
        self.assertEqual(o.status, OrderStatus.PENDING)
        self.assertEqual(BrokerAdapter.get_market_price.__name__, "get_market_price")
        self.assertEqual(BrokerAdapter.health_check.__name__, "health_check")

    def test_default_implementations(self):
        inst = FakeAdapter("x")
        self.assertIsNone(asyncio.run(BrokerAdapter.get_market_price(inst, "X")))
        self.assertEqual(asyncio.run(BrokerAdapter.health_check(inst))["status"], "unknown")


class TestBrokerRouter(TestCase):
    def test_register_and_get_list(self):
        r = BrokerRouter()
        a = FakeAdapter("live")
        r.register("live", a, preferred=True)
        self.assertIs(r.get("live"), a)
        self.assertIsNone(r.get("missing"))
        self.assertEqual(r.list_brokers(), ["live"])

    def test_route_paper_preferred(self):
        r = BrokerRouter()
        r.register("live", FakeAdapter("live"))
        r.register("paper", FakeAdapter("paper"), preferred=True)
        d = r.route("BTC-USD", mode="paper")
        self.assertEqual(d.broker, "paper")
        self.assertEqual(d.confidence, 1.0)

    def test_route_live_preferred(self):
        r = BrokerRouter()
        r.register("live", FakeAdapter("live"), preferred=True)
        r.register("paper", FakeAdapter("paper"))
        d = r.route("BTC-USD", mode="live")
        self.assertEqual(d.broker, "live")
        self.assertAlmostEqual(d.confidence, 0.95)

    def test_route_falls_back_to_paper(self):
        r = BrokerRouter()
        r.register("paper", FakeAdapter("paper"))
        d = r.route("BTC-USD", mode="live")
        self.assertEqual(d.broker, "paper")
        self.assertAlmostEqual(d.confidence, 0.5)
        self.assertIn("falling back", d.reason)

    def test_route_no_broker_raises(self):
        r = BrokerRouter()
        with self.assertRaises(ValueError):
            r.route("BTC-USD", mode="live")

    def test_route_and_submit_ok(self):
        r = BrokerRouter()
        r.register("paper", FakeAdapter("paper"), preferred=True)
        result = asyncio.run(r.route_and_submit(make_order(), mode="paper"))
        self.assertIsInstance(result, BrokerOrder)

    def test_route_and_submit_preview_fails(self):
        r = BrokerRouter()
        r.register("paper", FakeAdapter("paper", preview_ok=False), preferred=True)
        with self.assertRaises(ValueError):
            asyncio.run(r.route_and_submit(make_order(), mode="paper"))

    def test_route_and_submit_submit_fails(self):
        r = BrokerRouter()
        r.register("paper", FakeAdapter("paper", submit_raises=True), preferred=True)
        with self.assertRaises(ValueError):
            asyncio.run(r.route_and_submit(make_order(), mode="paper"))


class TestCoinbaseBroker(IsolatedAsyncioTestCase):
    def make_client(self):
        c = MagicMock()
        c.get_accounts = AsyncMock(return_value=[])
        c.get_accounts = AsyncMock(return_value={"accounts": []})
        c.get_product = AsyncMock(return_value={"base_increment": "0.00000001", "quote_increment": "0.01"})
        c.create_order = AsyncMock(return_value={"order_id": "o1"})
        c.cancel_order = AsyncMock(return_value=True)
        c.get_order = AsyncMock(return_value=None)
        c.list_orders = AsyncMock(return_value={"orders": []})
        c.get_fills = AsyncMock(return_value={"fills": []})
        c.get_positions = AsyncMock(return_value={"positions": []})
        c.list_products = AsyncMock(return_value={"products": []})
        c.get_product_ticker = AsyncMock(return_value={})
        return c

    def make_adapter(self, client=None):
        if client is None:
            client = self.make_client()
        return CoinbaseBrokerAdapter(client=client), client

    async def test_broker_name(self):
        a, _ = self.make_adapter()
        self.assertEqual(a.broker_name(), "coinbase")

    async def test_get_accounts_list(self):
        a, c = self.make_adapter()
        c.get_accounts = AsyncMock(return_value=[
            {"uuid": "u1", "name": "n", "currency": "USD",
             "available_balance": {"value": "100"}, "hold": {"value": "10"}},
        ])
        accs = await a.get_accounts()
        self.assertEqual(len(accs), 1)
        self.assertEqual(accs[0].account_id, "u1")
        self.assertEqual(accs[0].available_balance, Decimal("100"))
        self.assertEqual(accs[0].hold_balance, Decimal("10"))
        self.assertEqual(accs[0].total_balance, Decimal("110"))

    async def test_get_account_found_and_missing(self):
        a, c = self.make_adapter()
        c.get_accounts = AsyncMock(return_value=[
            {"uuid": "u1", "name": "n", "currency": "USD",
             "available_balance": {"value": "100"}, "hold": {"value": "0"}},
        ])
        acc = await a.get_account("u1")
        self.assertEqual(acc.account_id, "u1")
        with self.assertRaises(ValueError):
            await a.get_account("nope")

    async def test_preview_order_ok(self):
        a, _ = self.make_adapter()
        ok, msg = await a.preview_order(make_order())
        self.assertTrue(ok)

    async def test_preview_order_bad_size(self):
        a, _ = self.make_adapter()
        order = make_order()
        order.size = Decimal("0")
        ok, msg = await a.preview_order(order)
        self.assertFalse(ok)
        self.assertIn("positive", msg)

    async def test_preview_order_bad_increment(self):
        a, c = self.make_adapter()
        c.get_product = AsyncMock(return_value={"base_increment": "0.5", "quote_increment": "0.01"})
        order = make_order()
        order.size = Decimal("0.7")  # not multiple of 0.5
        ok, msg = await a.preview_order(order)
        self.assertFalse(ok)
        self.assertIn("multiple", msg)

    async def test_preview_order_bad_price_increment(self):
        a, c = self.make_adapter()
        c.get_product = AsyncMock(return_value={"base_increment": "0.00000001", "quote_increment": "0.5"})
        order = make_order()
        order.price = Decimal("100.1")  # not multiple of 0.5
        ok, msg = await a.preview_order(order)
        self.assertFalse(ok)
        self.assertIn("price must be multiple", msg)

    async def test_submit_order(self):
        a, c = self.make_adapter()
        c.create_order = AsyncMock(return_value={"order_id": "o9"})
        order = await a.submit_order(make_order())
        self.assertEqual(order.broker_order_id, "o9")
        self.assertEqual(order.status, OrderStatus.OPEN)

    async def test_cancel_order(self):
        a, c = self.make_adapter()
        self.assertTrue(await a.cancel_order("x"))

    async def test_get_order_none(self):
        a, c = self.make_adapter()
        c.get_order = AsyncMock(return_value=None)
        self.assertIsNone(await a.get_order("x"))

    async def test_get_order_raw(self):
        a, c = self.make_adapter()
        c.get_order = AsyncMock(return_value={"order_id": "o1", "product_id": "BTC-USD", "side": "BUY"})
        o = await a.get_order("o1")
        self.assertEqual(o.broker_order_id, "o1")

    async def test_list_orders_filters(self):
        a, c = self.make_adapter()
        c.list_orders = AsyncMock(return_value={"orders": [
            {"order_id": "o1", "product_id": "BTC-USD"},
            {"order_id": "o2", "product_id": "ETH-USD"},
        ]})
        res = await a.list_orders(product_id="BTC-USD", status=OrderStatus.OPEN)
        c.list_orders.assert_awaited_with(product_id="BTC-USD", order_status="OPEN")
        self.assertEqual(len(res), 2)

    async def test_get_fills_list_and_dict(self):
        a, c = self.make_adapter()
        c.get_fills = AsyncMock(return_value={"fills": [
            {"fill_id": "f1", "product_id": "BTC-USD", "side": "BUY", "size": "1",
             "price": "100", "fee": "0.1", "liquidity": "MAKER", "timestamp": "2024-01-01T00:00:00Z"},
        ]})
        fills = await a.get_fills("o1")
        self.assertEqual(fills[0].fill_id, "f1")
        self.assertEqual(fills[0].notional, Decimal("100"))

    async def test_get_fills_no_timestamp(self):
        a, c = self.make_adapter()
        c.get_fills = AsyncMock(return_value=[{"fill_id": "f1", "size": "1", "price": "100"}])
        fills = await a.get_fills("o1")
        self.assertEqual(fills[0].fill_id, "f1")

    async def test_get_positions_list_and_dict(self):
        a, c = self.make_adapter()
        c.get_positions = AsyncMock(return_value={"positions": [
            {"product_id": "BTC-USD", "position_size": "2", "entry_price": "100",
             "current_price": "110", "unrealized_pnl": "20", "realized_pnl": "0"},
            {"product_id": "ETH-USD", "position_size": "-1", "entry_price": "50"},
        ]})
        pos = await a.get_positions()
        self.assertEqual(pos[0].side, "long")
        self.assertEqual(pos[1].side, "short")
        self.assertEqual(len(pos), 2)

    async def test_get_positions_filtered(self):
        a, c = self.make_adapter()
        c.get_positions = AsyncMock(return_value=[{"product_id": "BTC-USD", "position_size": "2"}])
        pos = await a.get_positions(product_id="ETH-USD")
        self.assertEqual(pos, [])

    async def test_list_products_caches(self):
        a, c = self.make_adapter()
        c.list_products = AsyncMock(return_value={"products": [{"product_id": "BTC-USD"}]})
        prods = await a.list_products()
        self.assertEqual(prods[0]["product_id"], "BTC-USD")
        self.assertIn("BTC-USD", a._capability_cache)

    async def test_get_product(self):
        a, c = self.make_adapter()
        c.get_product = AsyncMock(return_value={"product_id": "BTC-USD"})
        self.assertEqual(await a.get_product("BTC-USD"), {"product_id": "BTC-USD"})

    async def test_get_market_price_from_product(self):
        a, c = self.make_adapter()
        c.get_product = AsyncMock(return_value={"price": "123.5"})
        self.assertEqual(await a.get_market_price("BTC-USD"), Decimal("123.5"))

    async def test_get_market_price_from_ticker(self):
        a, c = self.make_adapter()
        c.get_product = AsyncMock(return_value={})
        c.get_product_ticker = AsyncMock(return_value={"price": "99.0"})
        self.assertEqual(await a.get_market_price("BTC-USD"), Decimal("99.0"))

    async def test_get_market_price_none(self):
        a, c = self.make_adapter()
        c.get_product = AsyncMock(return_value={})
        c.get_product_ticker = AsyncMock(return_value={})
        self.assertIsNone(await a.get_market_price("BTC-USD"))

    async def test_health_check_ok(self):
        a, c = self.make_adapter()
        c.list_products = AsyncMock(return_value={"products": [{"product_id": "BTC-USD"}]})
        h = await a.health_check()
        self.assertEqual(h["status"], "healthy")

    async def test_health_check_unhealthy(self):
        a, c = self.make_adapter()
        c.list_products = AsyncMock(side_effect=RuntimeError("boom"))
        h = await a.health_check()
        self.assertEqual(h["status"], "unhealthy")

    async def test_get_capability(self):
        a, _ = self.make_adapter()
        a._capability_cache["BTC-USD"] = {"base_increment": "0.00000001"}
        self.assertEqual(a.get_capability("BTC-USD"), {"base_increment": "0.00000001"})
        self.assertEqual(a.get_capability("ETH-USD"), {})

    async def test_capability_matrix(self):
        a, c = self.make_adapter()
        c.list_products = AsyncMock(return_value={"products": [{
            "product_id": "BTC-USD", "base_currency": "BTC", "quote_currency": "USD",
            "base_increment": "0.00000001", "quote_increment": "0.01",
            "base_min_size": "0.0001", "base_max_size": "1000", "quote_decimals": 2,
            "supported_order_types": ["LIMIT"], "maker_fee_rate": "0.001",
            "taker_fee_rate": "0.002", "trading_disabled": False,
        }]})
        matrix = await a.get_exchange_capability_matrix()
        self.assertEqual(matrix[0]["status"], "active")
        self.assertEqual(matrix[0]["fees"]["maker_rate"], "0.001")

    async def test_capability_matrix_disabled(self):
        a, c = self.make_adapter()
        c.list_products = AsyncMock(return_value={"products": [
            {"product_id": "BTC-USD", "trading_disabled": True},
        ]})
        matrix = await a.get_exchange_capability_matrix()
        self.assertEqual(matrix[0]["status"], "disabled")

    async def test_build_order_payload(self):
        a, _ = self.make_adapter()
        payload = a._build_order_payload(make_order())
        self.assertEqual(payload["product_id"], "BTC-USD")
        self.assertEqual(payload["order_configuration"]["limit_limit_gtc"]["base_size"], "1")

    async def test_raw_to_order(self):
        a, _ = self.make_adapter()
        o = a._raw_to_order({"order_id": "o1", "client_order_id": "c", "account_id": "a",
                             "product_id": "BTC-USD", "side": "BUY", "status": "OPEN",
                             "size": "2", "price": "100", "filled_size": "1", "fees": "0.1",
                             "created_at": "2024-01-01T00:00:00Z"})
        self.assertEqual(o.broker_order_id, "o1")
        self.assertEqual(o.side, "buy")
        self.assertEqual(o.status, OrderStatus.OPEN)

    async def test_raw_to_order_no_price(self):
        a, _ = self.make_adapter()
        o = a._raw_to_order({"product_id": "BTC-USD", "side": "buy"})
        self.assertIsNone(o.price)
        self.assertEqual(o.status, OrderStatus.OPEN)

    async def test_construct_with_credentials(self):
        a = CoinbaseBrokerAdapter(credentials={"api_key": "k", "api_secret": "s", "passphrase": "p"})
        self.assertIsNotNone(a._client)


class TestPaperBroker(IsolatedAsyncioTestCase):
    async def test_broker_name(self):
        a = PaperBrokerAdapter()
        self.assertEqual(a.broker_name(), "paper")

    async def test_engine_property(self):
        a = PaperBrokerAdapter()
        self.assertIsNotNone(a.engine)

    async def test_get_accounts(self):
        a = PaperBrokerAdapter()
        accs = await a.get_accounts()
        self.assertEqual(accs[0].account_id, "paper-001")
        self.assertEqual(accs[0].currency, "USD")

    async def test_get_account(self):
        a = PaperBrokerAdapter()
        acc = await a.get_account("paper-001")
        self.assertEqual(acc.account_id, "paper-001")

    async def test_preview_order(self):
        a = PaperBrokerAdapter()
        ok, msg = await a.preview_order(make_order())
        self.assertTrue(ok)
        bad = make_order()
        bad.size = Decimal("0")
        ok2, msg2 = await a.preview_order(bad)
        self.assertFalse(ok2)
        self.assertIn("positive", msg2)

    async def test_submit_order(self):
        a = PaperBrokerAdapter()
        order = await a.submit_order(make_order())
        self.assertIsNotNone(order.broker_order_id)
        self.assertEqual(order.status, OrderStatus.OPEN)

    async def test_cancel_order(self):
        a = PaperBrokerAdapter()
        a.engine.orders["manual"] = MagicMock(status="open")
        self.assertTrue(await a.cancel_order("manual"))
        self.assertFalse(await a.cancel_order("unknown"))

    async def test_get_order_missing(self):
        a = PaperBrokerAdapter()
        self.assertIsNone(await a.get_order("nope"))

    async def test_list_orders(self):
        a = PaperBrokerAdapter()
        await a.submit_order(make_order())
        orders = await a.list_orders()
        self.assertEqual(len(orders), 1)

    async def test_list_orders_filtering(self):
        a = PaperBrokerAdapter()
        po = MagicMock()
        po.order_id = "o1"
        po.product_id = "ETH-USD"
        po.status = "open"
        po.strategy_id = "c"
        po.portfolio_id = "acc"
        po.side = "buy"
        po.order_type = "limit"
        po.size = Decimal("1")
        po.price = Decimal("100")
        po.filled_size = Decimal("0")
        po.remaining_size = Decimal("1")
        po.fee = Decimal("0")
        po.created_at = 1.0
        a.engine.orders = {"o1": po}
        kept = await a.list_orders(product_id="ETH-USD", status=OrderStatus.OPEN)
        self.assertEqual(len(kept), 1)
        excluded = await a.list_orders(product_id="BTC-USD")
        self.assertEqual(len(excluded), 0)

    async def test_get_fills(self):
        a = PaperBrokerAdapter()
        self.assertEqual(await a.get_fills("x"), [])

    async def test_get_positions(self):
        a = PaperBrokerAdapter()
        pos = await a.get_positions()
        self.assertIsInstance(pos, list)
        self.assertTrue(any(p.product_id == "BTC-USD" for p in pos))

    async def test_list_products(self):
        a = PaperBrokerAdapter()
        prods = await a.list_products()
        self.assertEqual(prods[0]["product_id"].split("-")[0], "BTC")

    async def test_get_product(self):
        a = PaperBrokerAdapter()
        a.engine.products = ["BTC-USD"]
        a.engine.mid_prices["BTC-USD"] = Decimal("100")
        self.assertEqual(await a.get_product("BTC-USD"), {"product_id": "BTC-USD", "price": 100.0})
        self.assertIsNone(await a.get_product("ETH-USD"))

    async def test_get_market_price(self):
        a = PaperBrokerAdapter()
        a.engine.mid_prices["BTC-USD"] = Decimal("50")
        self.assertEqual(await a.get_market_price("BTC-USD"), Decimal("50"))

    async def test_health_check(self):
        a = PaperBrokerAdapter()
        h = await a.health_check()
        self.assertEqual(h["status"], "healthy")
        self.assertEqual(h["broker"], "paper")


if __name__ == "__main__":
    import unittest

    unittest.main()
