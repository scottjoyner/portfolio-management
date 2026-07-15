import unittest
from unittest import mock


class _Row:
    def __init__(self, **kw):
        self._d = kw

    def __getattr__(self, name):
        if name in self._d:
            return self._d[name]
        raise AttributeError(name)

    def __getitem__(self, k):
        return self._d.get(k)


class FakeSession:
    def __init__(self, query_rows=None, fetchone=None, fetchall=None,
                 raise_query=False, raise_execute=False):
        self.query_rows = query_rows or []
        self.fetchone = fetchone
        self.fetchall = fetchall
        self.raise_query = raise_query
        self.raise_execute = raise_execute
        self.closed = False

    def query(self, *a, **k):
        if self.raise_query:
            raise RuntimeError("db")
        chain = mock.MagicMock()
        chain.filter.return_value = chain
        chain.order_by.return_value = chain
        chain.offset.return_value = chain
        chain.limit.return_value = chain
        chain.all.return_value = self.query_rows
        return chain

    def execute(self, *a, **k):
        if self.raise_execute:
            raise RuntimeError("db")
        res = mock.MagicMock()
        res.fetchone.return_value = self.fetchone
        res.fetchall.return_value = self.fetchall
        return res

    def close(self):
        self.closed = True


from trading_system.api.databases import session as session_mod


class TestSession(unittest.TestCase):
    def setUp(self):
        self.mock_session = mock.MagicMock()
        session_mod.db_manager._session = self.mock_session

    def tearDown(self):
        if hasattr(session_mod.db_manager, "_session"):
            del session_mod.db_manager._session

    # ---- happy paths (try branch) ----
    def test_get_accounts_ok(self):
        self.mock_session.execute.return_value.fetchall.return_value = [("a",)]
        self.assertEqual(session_mod.get_accounts(), [("a",)])

    def test_get_trades_ok(self):
        self.mock_session.execute.return_value.fetchall.return_value = [("t",)]
        self.assertEqual(session_mod.get_trades(10, 0), [("t",)])

    def test_get_positions_ok(self):
        self.mock_session.execute.return_value.fetchall.return_value = [("p",)]
        self.assertEqual(session_mod.get_positions(), [("p",)])

    def test_get_strategies_ok(self):
        self.mock_session.execute.return_value.fetchall.return_value = [("s",)]
        self.assertEqual(session_mod.get_strategies(), [("s",)])

    def test_get_performance_ok(self):
        r = _Row(total_pnl=5.0)
        self.mock_session.execute.return_value.fetchone.return_value = r
        self.assertEqual(session_mod.get_performance(), {"total_realized_pnl_usd": 5.0})

    def test_get_performance_ok_none(self):
        self.mock_session.execute.return_value.fetchone.return_value = None
        self.assertEqual(session_mod.get_performance(), {"total_realized_pnl_usd": 0.0})

    def test_get_price_estimates_ok(self):
        r = _Row(
            current_market_price=100.0,
            dcf_intrinsic_value=120.0,
            technical_score=0.5,
            consensus_vs_current_pct=1.5,
            confidence_score=0.9,
        )
        self.mock_session.execute.return_value.fetchone.return_value = r
        res = session_mod.get_price_estimates("BTC")
        self.assertEqual(res["current_price"], 100.0)
        self.assertEqual(res["price_estimates"]["dcf_intrinsic_value"], 120.0)

    def test_get_price_estimates_ok_none_fields(self):
        r = _Row(
            current_market_price=100.0,
            dcf_intrinsic_value=None,
            technical_score=None,
            consensus_vs_current_pct=None,
            confidence_score=0.0,
        )
        self.mock_session.execute.return_value.fetchone.return_value = r
        res = session_mod.get_price_estimates("BTC")
        self.assertIsNone(res["price_estimates"]["dcf_intrinsic_value"])

    def test_get_price_estimates_none_result(self):
        self.mock_session.execute.return_value.fetchone.return_value = None
        res = session_mod.get_price_estimates("BTC")
        self.assertIsNone(res["current_price"])

    def test_get_approvals_ok(self):
        rows = [_Row(status="PENDING"), _Row(status="approved"), _Row(status="other")]
        self.mock_session.execute.return_value.fetchall.return_value = rows
        res = session_mod.get_approvals()
        self.assertEqual(res["pending_count"], 1)
        self.assertEqual(res["completed_count"], 2)

    def test_get_research_hypotheses_ok(self):
        r1 = _Row(
            id=1,
            product_id="BTC",
            hypothesis_text="t",
            confidence_score=0.7,
            expiration_datetime="e",
            timestamp="t",
        )
        r2 = _Row(id=2)
        r3 = ("tup",)
        r4 = _Row(
            id=3,
            product_id=None,
            hypothesis_text=None,
            confidence_score=0.0,
            expiration_datetime=None,
            timestamp=None,
        )
        fake = FakeSession(query_rows=[r1, r2, r3, r4])
        with mock.patch("sqlalchemy.orm.Session", return_value=fake):
            res = session_mod.get_research_hypotheses()
        self.assertEqual(len(res["hypotheses"]), 4)

    # ---- except paths ----
    def test_get_accounts_except(self):
        self.mock_session.execute.side_effect = RuntimeError("db")
        self.assertEqual(session_mod.get_accounts(), [])

    def test_get_trades_except(self):
        self.mock_session.execute.side_effect = RuntimeError("db")
        self.assertEqual(session_mod.get_trades(), [])

    def test_get_positions_except(self):
        self.mock_session.execute.side_effect = RuntimeError("db")
        self.assertEqual(session_mod.get_positions(), [])

    def test_get_strategies_except(self):
        self.mock_session.execute.side_effect = RuntimeError("db")
        self.assertEqual(session_mod.get_strategies(), [])

    def test_get_performance_except(self):
        self.mock_session.execute.side_effect = RuntimeError("db")
        self.assertEqual(session_mod.get_performance(), {"total_realized_pnl_usd": 0.0})

    def test_get_price_estimates_except(self):
        self.mock_session.execute.side_effect = RuntimeError("db")
        self.assertEqual(session_mod.get_price_estimates("BTC"), {"current_price": None, "price_estimates": {}})

    def test_get_approvals_except(self):
        self.mock_session.execute.side_effect = RuntimeError("db")
        self.assertEqual(session_mod.get_approvals(), {"pending_count": 0, "completed_count": 0})

    def test_get_research_hypotheses_except(self):
        fake = FakeSession(raise_execute=True)
        with mock.patch("sqlalchemy.orm.Session", return_value=fake):
            res = session_mod.get_research_hypotheses()
        self.assertEqual(res, {"hypotheses": [], "market_regimes": {}})


if __name__ == "__main__":
    unittest.main()
