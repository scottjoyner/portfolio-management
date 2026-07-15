import unittest
from unittest.mock import MagicMock

from portfolio.manager import PortfolioError, PortfolioManager


class _Obj:
    pass


class TestPortfolioManager(unittest.TestCase):
    def _db(self, first=None, all=None, add_capture=None):
        db = MagicMock()
        q = MagicMock()
        q.filter.return_value = q
        q.first.return_value = first
        q.all.return_value = all if all is not None else []
        db.query.return_value = q
        if add_capture is not None:
            db.add.side_effect = lambda o: add_capture.append(o)
        return db

    def test_get_portfolio_found(self):
        p = MagicMock()
        db = self._db(first=p)
        mgr = PortfolioManager(db)
        self.assertIs(mgr.get_portfolio("p1"), p)

    def test_get_portfolio_none(self):
        db = self._db(first=None)
        mgr = PortfolioManager(db)
        self.assertIsNone(mgr.get_portfolio("p1"))

    def test_list_portfolios(self):
        db = self._db(all=[MagicMock(), MagicMock()])
        mgr = PortfolioManager(db)
        self.assertEqual(len(mgr.list_portfolios()), 2)

    def test_update_nav_none(self):
        db = self._db(first=None)
        mgr = PortfolioManager(db)
        self.assertIsNone(mgr.update_nav("p1", 100.0))

    def test_update_nav_updates(self):
        p = MagicMock()
        db = self._db(first=p)
        mgr = PortfolioManager(db)
        out = mgr.update_nav("p1", 100.0, realized_pnl=1.0, unrealized_pnl=2.0)
        self.assertIs(out, p)
        self.assertEqual(p.nav, 100.0)
        self.assertEqual(p.realized_pnl, 1.0)
        self.assertEqual(p.unrealized_pnl, 2.0)
        db.commit.assert_called_once()

    def test_update_nav_partial(self):
        p = _Obj()
        p.nav = 0.0
        db = self._db(first=p)
        mgr = PortfolioManager(db)
        mgr.update_nav("p1", 50.0)
        self.assertEqual(p.nav, 50.0)
        self.assertFalse(hasattr(p, "realized_pnl"))
        db.commit.assert_called_once()

    def test_adjust_capital_none(self):
        db = self._db(first=None)
        mgr = PortfolioManager(db)
        self.assertIsNone(mgr.adjust_capital("p1", "b1", 10.0))

    def test_adjust_capital_insufficient(self):
        b = MagicMock()
        b.amount = 5.0
        db = self._db(first=b)
        mgr = PortfolioManager(db)
        with self.assertRaises(PortfolioError):
            mgr.adjust_capital("p1", "b1", -10.0)

    def test_adjust_capital_ok(self):
        b = MagicMock()
        b.amount = 5.0
        db = self._db(first=b)
        mgr = PortfolioManager(db)
        out = mgr.adjust_capital("p1", "b1", 10.0)
        self.assertIs(out, b)
        self.assertAlmostEqual(b.amount, 15.0)
        db.commit.assert_called_once()

    def test_rebalance_sleeves_portfolio_not_found(self):
        db = self._db(first=None)
        mgr = PortfolioManager(db)
        with self.assertRaises(PortfolioError):
            mgr.rebalance_sleeves("p1", {"growth": 1.0})

    def test_rebalance_sleeves_bad_total(self):
        p = MagicMock()
        db = self._db(first=p, all=[])
        mgr = PortfolioManager(db)
        with self.assertRaises(PortfolioError):
            mgr.rebalance_sleeves("p1", {"growth": 0.5})

    def test_rebalance_sleeves_update_and_add(self):
        p = MagicMock()
        existing = MagicMock()
        existing.name = "growth"
        added = []
        db = self._db(first=p, all=[existing], add_capture=added)
        mgr = PortfolioManager(db)
        out = mgr.rebalance_sleeves("p1", {"growth": 0.6, "value": 0.4})
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].name, "growth")
        self.assertEqual(out[0].weight, 0.6)
        self.assertEqual(len(added), 1)
        self.assertEqual(added[0].name, "value")
        self.assertEqual(added[0].weight, 0.4)
        db.commit.assert_called_once()

    def test_get_sleeve_allocation(self):
        s = MagicMock()
        db = self._db(first=s)
        mgr = PortfolioManager(db)
        self.assertIs(mgr.get_sleeve_allocation("p1", "growth"), s)


if __name__ == "__main__":
    unittest.main()
