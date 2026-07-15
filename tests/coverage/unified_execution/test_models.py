import unittest
from decimal import Decimal
from datetime import datetime

from trading_system.unified_execution.models import (
    OrderSide, OrderType, OrderStatus, HealthStatus,
    UniversalAsset, UniversalOrder, UniversalFill, UniversalPosition,
    UniversalBalance, UniversalAccount, TickerInfo, OrderbookLevel, Orderbook,
)


class TestEnums(unittest.TestCase):
    def test_order_side(self):
        self.assertEqual(OrderSide.BUY.value, "BUY")
        self.assertEqual(OrderSide.SELL.value, "SELL")

    def test_order_type(self):
        for name in ("MARKET", "LIMIT", "STOP", "TWAP", "ICEBERG"):
            self.assertEqual(OrderType[name].value, name)

    def test_order_status(self):
        for name in ("PENDING", "OPEN", "FILLED", "PARTIAL", "CANCELLED", "REJECTED"):
            self.assertEqual(OrderStatus[name].value, name)

    def test_health_status(self):
        for name in ("HEALTHY", "DEGRADED", "UNHEALTHY"):
            self.assertEqual(HealthStatus[name].value, name)


class TestDataclasses(unittest.TestCase):
    def _asset(self):
        return UniversalAsset(
            asset_id="BTC-USD", symbol="BTC",
            base_currency="BTC", quote_currency="USD",
            chain_id="1", contract_address="0xabc", decimals=8,
        )

    def test_asset_defaults(self):
        a = UniversalAsset(asset_id="X", symbol="X", base_currency="X", quote_currency="USD")
        self.assertEqual(a.decimals, 18)
        self.assertIsNone(a.chain_id)

    def test_order(self):
        a = self._asset()
        o = UniversalOrder(
            order_id="o1", asset=a, side=OrderSide.BUY, order_type=OrderType.MARKET,
            size=Decimal("1.0"), price=Decimal("100"), gas_limit=21000,
            venue_order_id="v1", metadata={"k": "v"},
        )
        self.assertEqual(o.order_id, "o1")
        self.assertEqual(o.status, OrderStatus.PENDING)
        self.assertIsInstance(o.timestamp, datetime)
        # default factory metadata
        o2 = UniversalOrder(order_id="o2", asset=a, side=OrderSide.SELL,
                            order_type=OrderType.LIMIT, size=Decimal("2"))
        self.assertEqual(o2.metadata, {})

    def test_fill(self):
        a = self._asset()
        f = UniversalFill(
            fill_id="f1", order_id="o1", venue_fill_id="vf1", asset=a,
            size=Decimal("1"), price=Decimal("100"), fee=Decimal("0.5"),
            timestamp=datetime.now(),
        )
        self.assertEqual(f.fill_id, "f1")

    def test_position(self):
        p = UniversalPosition(
            asset=self._asset(), size=Decimal("1"), avg_entry_price=Decimal("100"),
            unrealized_pnl=Decimal("5"), realized_pnl=Decimal("2"),
        )
        self.assertEqual(p.size, Decimal("1"))

    def test_balance_and_account(self):
        a = self._asset()
        b = UniversalBalance(asset=a, amount=Decimal("10"), available=Decimal("8"), locked=Decimal("2"))
        acct = UniversalAccount(
            account_id="acc", venue_name="Coinbase", name="Main",
            currency="USD", balances=[b],
        )
        self.assertEqual(acct.balances[0].locked, Decimal("2"))

    def test_ticker_and_orderbook(self):
        a = self._asset()
        t = TickerInfo(asset=a, bid_price=Decimal("1"), ask_price=Decimal("2"),
                       last_price=Decimal("1.5"), volume_24h=Decimal("100"),
                       timestamp=datetime.now())
        self.assertEqual(t.ask_price, Decimal("2"))
        lvl = OrderbookLevel(price=Decimal("1"), amount=Decimal("5"))
        ob = Orderbook(asset=a, bids=[lvl], asks=[lvl], timestamp=datetime.now())
        self.assertEqual(ob.bids[0].amount, Decimal("5"))


if __name__ == "__main__":
    unittest.main()
