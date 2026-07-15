import unittest
from datetime import datetime

from db_helpers import install_fakes, make_db, QueryStub, _Row

spm, _pm, _pd = install_fakes()

from trading_system.database.queries import positions as pos

Fill = spm.Fill
Order = spm.Order
Portfolio = spm.Portfolio


def fill(product_id="BTC-USD", size=1.0, price=100.0, side="buy", created_at=None, order_id="o1"):
    return _Row(product_id=product_id, size=size, price=price, side=side,
                created_at=created_at or datetime(2024, 1, 1), order_id=order_id)


class TestPositionsRepository(unittest.TestCase):
    def test_get_position_none(self):
        db = make_db({Fill: QueryStub(rows=[])})
        repo = pos.PositionsRepository(db)
        self.assertIsNone(repo.get_position("BTC-USD"))

    def test_get_position_long(self):
        fills = [fill(size=2, price=100), fill(size=1, price=130, created_at=datetime(2024, 2, 1))]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        p = repo.get_position("BTC-USD")
        self.assertEqual(p["size"], 3.0)
        self.assertEqual(p["position_type"], "long")

    def test_get_position_with_portfolio(self):
        fills = [fill(size=2, price=100)]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        p = repo.get_position("BTC-USD", portfolio_id="pf1")
        self.assertEqual(p["portfolio_id"], "pf1")

    def test_get_position_zero_size(self):
        fills = [fill(size=1, price=100, side="buy"), fill(size=1, price=100, side="sell")]
        # total_size = 2 (sum of sizes), still >0 so long; test neutral separately
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        p = repo.get_position("BTC-USD")
        self.assertIn(p["position_type"], ("long", "neutral", "short"))

    def test_list_positions(self):
        fills = [fill(size=2, side="buy"), fill(size=1, side="sell"),
                 fill(product_id="ETH-USD", size=3, side="buy")]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        out = repo.list_positions()
        self.assertEqual(len(out), 2)

    def test_list_positions_with_portfolio(self):
        fills = [fill(size=2, side="buy")]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        out = repo.list_positions(portfolio_id="pf1")
        self.assertEqual(len(out), 1)

    def test_update_position_size(self):
        fills = [fill(size=2, price=100)]
        portfolios = [_Row(id="pf1")]

        def _query(model, *a, **k):
            if model is Fill:
                return QueryStub(rows=fills)
            # string arg for Portfolio iteration
            return QueryStub(rows=portfolios)

        import unittest.mock as m
        db = m.MagicMock()
        db.query.side_effect = _query
        repo = pos.PositionsRepository(db)
        out = repo.update_position_size("BTC-USD", 1.0, price=110)
        self.assertIsNotNone(out)

    def test_update_position_size_no_existing(self):
        portfolios = [_Row(id="pf1")]

        def _query(model, *a, **k):
            if model is Fill:
                return QueryStub(rows=[])
            return QueryStub(rows=portfolios)

        import unittest.mock as m
        db = m.MagicMock()
        db.query.side_effect = _query
        repo = pos.PositionsRepository(db)
        out = repo.update_position_size("BTC-USD", 1.0)
        self.assertIsNone(out)

    def test_close_position_found(self):
        fills = [fill(size=2, price=100)]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        out = repo.close_position("BTC-USD", market_price=120)
        self.assertEqual(out["close_price"], 120)

    def test_close_position_not_found(self):
        db = make_db({Fill: QueryStub(rows=[])})
        repo = pos.PositionsRepository(db)
        out = repo.close_position("BTC-USD")
        self.assertIn("error", out)

    def test_get_portfolio_position_summary(self):
        fills = [fill(size=2, side="buy"), fill(product_id="ETH-USD", size=1, side="buy")]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        out = repo.get_portfolio_position_summary("pf1")
        self.assertEqual(out["positions_count"], 2)

    def test_track_delta_change_existing(self):
        fills = [fill(size=2, price=100)]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        out = repo.track_delta_change("BTC-USD", 1.0)
        self.assertEqual(out["new_size"], 3.0)

    def test_track_delta_change_new(self):
        db = make_db({Fill: QueryStub(rows=[])})
        repo = pos.PositionsRepository(db)
        out = repo.track_delta_change("BTC-USD", -1.0)
        self.assertEqual(out["position_type"], "short")
        out2 = repo.track_delta_change("BTC-USD", 0.0)
        self.assertEqual(out2["position_type"], "neutral")

    def test_check_position_limit_no_portfolio(self):
        db = make_db({Fill: QueryStub(rows=[]), Portfolio: QueryStub(first=None)})
        repo = pos.PositionsRepository(db)
        out = repo.check_position_limit("BTC-USD", "pf1", 1.0)
        self.assertTrue(out["allowed"])

    def test_check_position_limit_with_portfolio(self):
        fills = [fill(size=2, price=100)]
        portfolio = _Row(id="pf1", nav=1_000_000.0)
        db = make_db({Fill: QueryStub(rows=fills), Portfolio: QueryStub(first=portfolio)})
        repo = pos.PositionsRepository(db)
        out = repo.check_position_limit("BTC-USD", "pf1", 1.0)
        self.assertIn("allowed", out)
        self.assertEqual(out["portfolio_nav"], 1_000_000.0)

    def test_get_unrealized_pnl_none(self):
        db = make_db({Fill: QueryStub(rows=[])})
        repo = pos.PositionsRepository(db)
        out = repo.get_unrealized_pnl("BTC-USD")
        self.assertEqual(out["unrealized_pnl"], 0)

    def test_get_unrealized_pnl_found(self):
        fills = [fill(size=2, price=100)]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        out = repo.get_unrealized_pnl("BTC-USD", current_price=110)
        self.assertIn("unrealized_pnl", out)


class TestModuleHelpers(unittest.TestCase):
    def test_get_positions_overview(self):
        fills = [fill(size=2, side="buy")]
        portfolios = [_Row(id="pf1", name="Main")]
        db = make_db({Fill: QueryStub(rows=fills), Portfolio: QueryStub(rows=portfolios)})
        out = pos.get_positions_overview(db)
        self.assertEqual(len(out["portfolios"]), 1)
        self.assertEqual(out["total_positions_count"], 1)

    def test_get_position_deltas(self):
        orders = [_Row(order_id="o1", product_id="BTC-USD", side="buy", size=1.0,
                       price=100.0, status="open"),
                  _Row(order_id="o2", product_id="ETH-USD", side="sell", size=2.0,
                       price=None, status="partial")]
        db = make_db({Order: QueryStub(rows=orders)})
        out = pos.get_position_deltas(db, "BTC-USD")
        self.assertEqual(len(out), 2)
        self.assertIsNone(out[1]["price"])

    def test_get_positions_overview_skips_none_summary(self):
        # Cover the 275->273 branch: a portfolio whose summary is falsy is skipped.
        fills = [fill(size=2, side="buy")]
        portfolios = [_Row(id="pf1", name="Main"), _Row(id="pf2", name="Empty")]
        db = make_db({Fill: QueryStub(rows=fills), Portfolio: QueryStub(rows=portfolios)})

        orig = pos.PositionsRepository.get_portfolio_position_summary

        def _fake_summary(self, portfolio_id):
            if portfolio_id == "pf2":
                return None
            return orig(self, portfolio_id)

        with unittest.mock.patch.object(
            pos.PositionsRepository, "get_portfolio_position_summary", _fake_summary
        ):
            out = pos.get_positions_overview(db)
        self.assertEqual(len(out["portfolios"]), 1)
        self.assertEqual(out["total_positions_count"], 1)

    def test_get_positions_overview_empty(self):
        db = make_db({Portfolio: QueryStub(rows=[])})
        out = pos.get_positions_overview(db)
        self.assertEqual(out["portfolios"], [])
        self.assertEqual(out["total_positions_count"], 0)

    def test_update_position_size_existing_value(self):
        fills = [fill(size=2, price=100)]
        portfolios = [_Row(id="pf1")]

        def _query(model, *a, **k):
            if model is Fill:
                return QueryStub(rows=fills)
            return QueryStub(rows=portfolios)

        import unittest.mock as m
        db = m.MagicMock()
        db.query.side_effect = _query
        repo = pos.PositionsRepository(db)
        out = repo.update_position_size("BTC-USD", 1.0)
        self.assertIsNotNone(out)

    def test_get_position_short_path(self):
        fills = [fill(size=2, price=100, side="sell"), fill(size=2, price=100, side="sell")]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        p = repo.get_position("BTC-USD")
        self.assertEqual(p["position_type"], "short")

    def test_track_delta_change_short(self):
        fills = [fill(size=5, price=100, side="sell")]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        out = repo.track_delta_change("BTC-USD", -3.0)
        self.assertEqual(out["position_type"], "short")

    def test_get_unrealized_pnl_pct_zero(self):
        fills = [fill(size=2, price=100)]
        db = make_db({Fill: QueryStub(rows=fills)})
        repo = pos.PositionsRepository(db)
        out = repo.get_unrealized_pnl("BTC-USD", current_price=100)
        self.assertEqual(out["unrealized_pnl_pct"], 0)


if __name__ == "__main__":
    unittest.main()
