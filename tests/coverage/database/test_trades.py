import unittest
from datetime import datetime

from db_helpers import install_fakes, make_db, QueryStub, _Row

spm, _pm, _pd = install_fakes()

from trading_system.database.queries import trades as tr

Order = spm.Order
Fill = spm.Fill
Portfolio = spm.Portfolio


def order_row(**kw):
    base = dict(order_id="o1", preview_id=None, strategy_id="s", portfolio_id="pf1",
                sleeve_id=None, product_id="BTC-USD", side="buy", size=1.0,
                remaining_size=1.0, price=100.0, notional=100.0, order_type="limit",
                status="open", maker_taker_expectation=None, queue_age_s=0,
                risk_mode="NORMAL", reduce_only=False,
                created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 1))
    base.update(kw)
    return _Row(**base)


def fill_row(**kw):
    base = dict(fill_id="f1", order_id="o1", product_id="BTC-USD", side="buy",
                size=1.0, price=100.0, notional=100.0, slippage_bps=1.0, fee=0.5,
                fee_currency="USD", liquidity="maker", created_at=datetime(2024, 1, 1))
    base.update(kw)
    return _Row(**base)


class TestOrderOps(unittest.TestCase):
    def test_create_order_full(self):
        db = make_db()
        repo = tr.TradesRepository(db)
        out = repo.create_order({"order_id": "myid", "product_id": "BTC-USD",
                                 "side": "buy", "size": 2, "price": 100, "notional": 200})
        self.assertEqual(out["order_id"], "myid")
        db.add.assert_called_once()

    def test_create_order_generated_id(self):
        db = make_db()
        repo = tr.TradesRepository(db)
        out = repo.create_order({"product_id": "ETH-USD", "side": "sell", "size": 1,
                                 "price": 50, "notional": 50})
        self.assertTrue(out["order_id"])
        self.assertIsNotNone(out["price"])

    def test_get_order_found(self):
        db = make_db({Order: QueryStub(first=order_row())})
        repo = tr.TradesRepository(db)
        out = repo.get_order("o1")
        self.assertEqual(out["order_id"], "o1")
        self.assertEqual(out["price"], 100.0)

    def test_get_order_null_price_notional(self):
        db = make_db({Order: QueryStub(first=order_row(price=None, notional=None))})
        repo = tr.TradesRepository(db)
        out = repo.get_order("o1")
        self.assertIsNone(out["price"])
        self.assertIsNone(out["notional"])

    def test_get_order_missing(self):
        db = make_db({Order: QueryStub(first=None)})
        repo = tr.TradesRepository(db)
        self.assertIsNone(repo.get_order("x"))

    def test_list_orders(self):
        rows = [order_row(), order_row(order_id="o2", price=None, notional=None)]
        db = make_db({Order: QueryStub(rows=rows)})
        repo = tr.TradesRepository(db)
        out = repo.list_orders()
        self.assertEqual(len(out), 2)
        out2 = repo.list_orders(portfolio_id="pf1", status="open")
        self.assertEqual(len(out2), 2)

    def test_update_order_status_found(self):
        db = make_db({Order: QueryStub(first=order_row(status="pending"))})
        repo = tr.TradesRepository(db)
        out = repo.update_order_status("o1", "open")
        self.assertEqual(out["old_status"], "pending")
        self.assertEqual(out["new_status"], "open")

    def test_update_order_status_missing(self):
        db = make_db({Order: QueryStub(first=None)})
        repo = tr.TradesRepository(db)
        self.assertIsNone(repo.update_order_status("x", "open"))

    def test_cancel_order_open(self):
        db = make_db({Order: QueryStub(first=order_row(status="open"))})
        repo = tr.TradesRepository(db)
        out = repo.cancel_order("o1")
        self.assertTrue(out["success"])
        self.assertEqual(out["new_status"], "cancelled")

    def test_cancel_order_not_open(self):
        db = make_db({Order: QueryStub(first=order_row(status="closed"))})
        repo = tr.TradesRepository(db)
        out = repo.cancel_order("o1")
        self.assertFalse(out["success"])

    def test_cancel_order_missing(self):
        db = make_db({Order: QueryStub(first=None)})
        repo = tr.TradesRepository(db)
        out = repo.cancel_order("x")
        self.assertFalse(out["success"])
        self.assertIn("not found", out["error"])

    def test_partially_fill_order_partial(self):
        db = make_db({Order: QueryStub(first=order_row(remaining_size=2.0))})
        repo = tr.TradesRepository(db)
        out = repo.partially_fill_order("o1", 0.5, 100.0)
        self.assertEqual(out["new_status"], "partial")

    def test_partially_fill_order_closed(self):
        db = make_db({Order: QueryStub(first=order_row(remaining_size=1.0))})
        repo = tr.TradesRepository(db)
        out = repo.partially_fill_order("o1", 1.0, 100.0)
        self.assertEqual(out["new_status"], "closed")

    def test_partially_fill_order_missing(self):
        db = make_db({Order: QueryStub(first=None)})
        repo = tr.TradesRepository(db)
        out = repo.partially_fill_order("x", 1.0, 100.0)
        self.assertIn("error", out)


class TestFillOps(unittest.TestCase):
    def test_get_fills_for_order(self):
        db = make_db({Fill: QueryStub(rows=[fill_row(), {"raw": 1}])})
        repo = tr.TradesRepository(db)
        out = repo.get_fills_for_order("o1")
        self.assertEqual(len(out), 2)
        # dict without __dict__ path
        self.assertEqual(out[1], {"raw": 1})

    def test_create_fill(self):
        db = make_db()
        repo = tr.TradesRepository(db)
        out = repo.create_fill({"order_id": "o1", "product_id": "BTC-USD",
                                "size": 1, "price": 100, "notional": 100})
        self.assertTrue(out["fill_id"])
        db.add.assert_called_once()

    def test_create_fill_with_id_null_notional(self):
        db = make_db()
        repo = tr.TradesRepository(db)
        out = repo.create_fill({"fill_id": "myf", "order_id": "o1",
                                "product_id": "BTC-USD", "size": 1, "price": 100,
                                "notional": 0})
        self.assertEqual(out["fill_id"], "myf")
        self.assertIsNone(out["notional"])

    def test_list_fills(self):
        db = make_db({Fill: QueryStub(rows=[fill_row()])})
        repo = tr.TradesRepository(db)
        self.assertEqual(len(repo.list_fills()), 1)
        self.assertEqual(len(repo.list_fills(product_id="BTC-USD", order_id="o1")), 1)


class TestPnlAndSummary(unittest.TestCase):
    def test_get_trade_pnl_none(self):
        db = make_db({Fill: QueryStub(rows=[])})
        repo = tr.TradesRepository(db)
        out = repo.get_trade_pnl("BTC-USD")
        self.assertEqual(out["total_fills"], 0)

    def test_get_trade_pnl_with_fills(self):
        fills = [fill_row(size=2, price=100, side="buy", fee=0.5),
                 fill_row(size=1, price=110, side="sell", fee=0.3)]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = tr.TradesRepository(db)
        out = repo.get_trade_pnl("BTC-USD", portfolio_id="pf1")
        self.assertIn("realized_pnl", out)
        self.assertEqual(out["total_fills"], 2)

    def test_get_trade_pnl_zero_size(self):
        fills = [fill_row(size=0, price=100, fee=0.0)]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = tr.TradesRepository(db)
        out = repo.get_trade_pnl("BTC-USD")
        self.assertEqual(out["unrealized_pnl"], 0)

    def test_get_trade_history(self):
        rows = [order_row()]
        db = make_db({Order: QueryStub(rows=rows)})
        repo = tr.TradesRepository(db)
        out = repo.get_trade_history("BTC-USD", portfolio_id="pf1", limit=10, offset=0)
        self.assertEqual(len(out), 1)
        out2 = repo.get_trade_history("BTC-USD")
        self.assertEqual(len(out2), 1)

    def test_calculate_fill_metrics_none(self):
        db = make_db({Fill: QueryStub(rows=[])})
        repo = tr.TradesRepository(db)
        out = repo.calculate_fill_metrics("BTC-USD")
        self.assertEqual(out["fill_count"], 0)

    def test_calculate_fill_metrics_with_fills(self):
        fills = [fill_row(size=2, notional=200, slippage_bps=1, fee=0.5)]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = tr.TradesRepository(db)
        out = repo.calculate_fill_metrics("BTC-USD", portfolio_id="pf1")
        self.assertEqual(out["fill_count"], 1)


class TestModuleHelpers(unittest.TestCase):
    def test_get_trades_overview(self):
        portfolios = [_Row(id="pf1", name="Main")]
        orders = [order_row(status="open")]

        def _query(model, *a, **k):
            if model is Portfolio:
                return QueryStub(rows=portfolios)
            return QueryStub(rows=orders)

        import unittest.mock as m
        db = m.MagicMock()
        db.query.side_effect = _query
        out = tr.get_trades_overview(db)
        self.assertEqual(len(out["portfolios"]), 1)

    def test_get_order_status_feed(self):
        orders = [order_row(order_id="o1", status="open"),
                  order_row(order_id="o2", status="partial")]
        db = make_db({Order: QueryStub(rows=orders)})
        out = tr.get_order_status_feed(db)
        self.assertEqual(out["o1"], "open")


if __name__ == "__main__":
    unittest.main()
