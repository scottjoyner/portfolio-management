import datetime
import unittest
from unittest import mock

import trading_system.api.routes_prod as routes_prod


DT = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)


class _Row:
    def __init__(self, **kw):
        self._d = kw

    def __getattr__(self, name):
        if name in self._d:
            return self._d[name]
        raise AttributeError(name)


def make_db(rows, first=None, raise_query=False):
    db = mock.MagicMock()
    chain = mock.MagicMock()
    chain.filter.return_value = chain
    chain.order_by.return_value = chain
    chain.limit.return_value = chain
    chain.offset.return_value = chain
    chain.all.return_value = rows
    chain.first.return_value = first
    if raise_query:
        db.query.side_effect = RuntimeError("db")
    else:
        db.query.return_value = chain
    return db


class TestRoutesProd(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import sys
        import types

        # Inject a fake storage.postgres.models module so the lazily-imported
        # model classes inside routes_prod resolve regardless of which `storage`
        # package pytest happens to put on the path.
        fake_storage = types.ModuleType("storage")
        fake_postgres = types.ModuleType("storage.postgres")
        fake_models = types.ModuleType("storage.postgres.models")
        for name in ["Account", "TradeOrder", "CapitalBucket", "Approval",
                     "ResearchNote", "StrategyConfig"]:
            cls = type(name, (), {})
            cls.created_at = mock.MagicMock()
            cls.status = mock.MagicMock()
            cls.date = mock.MagicMock()
            cls.current_balance = mock.MagicMock()
            cls.updated_at = mock.MagicMock()
            setattr(fake_models, name, cls)
        fake_postgres.models = fake_models
        fake_storage.postgres = fake_postgres
        self._orig = {}
        for k in ["storage", "storage.postgres", "storage.postgres.models"]:
            self._orig[k] = sys.modules.get(k)
            sys.modules[k] = {"storage": fake_storage, "storage.postgres": fake_postgres,
                              "storage.postgres.models": fake_models}[k]
        self._orig_dm = routes_prod.DATABASE_MODE

    def tearDown(self):
        import sys
        for k, val in self._orig.items():
            if val is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = val
        routes_prod.DATABASE_MODE = self._orig_dm

    # ---- health_check / get_metrics ----
    async def test_health_check_no_db(self):
        res = await routes_prod.health_check()
        self.assertEqual(res["status"], "healthy")

    async def test_health_check_with_db(self):
        res = await routes_prod.health_check(mock.MagicMock())
        self.assertTrue(res["components"]["database"])

    async def test_get_metrics_no_db(self):
        res = await routes_prod.get_metrics()
        self.assertIn("postgresql", res["metrics"])

    async def test_get_metrics_with_db(self):
        res = await routes_prod.get_metrics(mock.MagicMock())
        self.assertEqual(res["metrics"]["postgresql"]["total_tables"], 19)

    # ---- sync / price estimations (no branches) ----
    async def test_sync_account_transactions(self):
        res = await routes_prod.sync_account_transactions("acc1")
        self.assertEqual(res["account_id"], "acc1")

    async def test_get_price_estimations(self):
        res = await routes_prod.get_price_estimations("BTC")
        self.assertEqual(res["instrument"], "BTC")

    # ---- list_accounts ----
    async def test_list_accounts_mock_branch(self):
        routes_prod.DATABASE_MODE = True
        res = await routes_prod.list_accounts(mock.MagicMock())
        self.assertEqual(res["total_accounts"], 0)

    async def test_list_accounts_fallback_success(self):
        routes_prod.DATABASE_MODE = False
        a1 = _Row(id=1, name=None, provider="P", currency=None,
                  current_balance=100, fiat_balance=50, created_at=DT,
                  status="active", institution_name="Inst")
        a2 = _Row(id=2, name="Acct", provider="Q", currency="EUR",
                  current_balance=0, fiat_balance=0, created_at=DT,
                  status="inactive", institution_name=None)
        db = make_db([a1, a2])
        res = await routes_prod.list_accounts(db)
        self.assertEqual(res["total_accounts"], 1)
        self.assertEqual(res["accounts"][0]["currency"], "USD")

    async def test_list_accounts_fallback_except(self):
        routes_prod.DATABASE_MODE = True
        res = await routes_prod.list_accounts(None)
        self.assertEqual(res["status"], "no_accounts_found")

    # ---- list_trades ----
    async def test_list_trades_success(self):
        routes_prod.DATABASE_MODE = True
        t1 = _Row(order_id=1, product_id="BTC", side="buy", remaining_size=10,
                  price=100, created_at=DT, status="closed", fee=1.0,
                  approval_id=5, exchange="cb")
        t2 = _Row(order_id=2, product_id="ETH", side="sell", remaining_size=0,
                  price=None, created_at=DT, status="closed")
        db = make_db([t1, t2])
        res = await routes_prod.list_trades(db, 50, 0)
        self.assertEqual(res["total_trades"], 2)
        self.assertFalse(res["has_more"])

    async def test_list_trades_has_more(self):
        routes_prod.DATABASE_MODE = True
        rows = [_Row(order_id=i, product_id="BTC", side="buy",
                     remaining_size=1, price=1, created_at=DT, status="closed")
                for i in range(60)]
        db = make_db(rows)
        res = await routes_prod.list_trades(db, 50, 0)
        self.assertTrue(res["has_more"])

    async def test_list_trades_except(self):
        routes_prod.DATABASE_MODE = True
        res = await routes_prod.list_trades(None, 50, 0)
        self.assertEqual(res["total_trades"], 0)

    async def test_list_trades_db_mode_false(self):
        routes_prod.DATABASE_MODE = False
        res = await routes_prod.list_trades(mock.MagicMock(), 50, 0)
        self.assertEqual(res["total_trades"], 0)

    # ---- list_positions ----
    async def test_list_positions_success(self):
        routes_prod.DATABASE_MODE = True
        db = mock.MagicMock()
        repo_cls = mock.MagicMock()
        inst = mock.MagicMock()
        inst.list_positions.return_value = [
            {"market_value": 10.0, "unrealized_pnl": 2.0},
            {"market_value": 5.0},
        ]
        repo_cls.return_value = inst
        orig = routes_prod.PositionsRepository
        routes_prod.PositionsRepository = repo_cls
        try:
            res = await routes_prod.list_positions(db, "p1")
        finally:
            routes_prod.PositionsRepository = orig
        self.assertEqual(res["total_positions"], 2)
        self.assertEqual(res["total_exposure_usd"], 15.0)

    async def test_list_positions_empty(self):
        routes_prod.DATABASE_MODE = True
        db = mock.MagicMock()
        repo_cls = mock.MagicMock()
        inst = mock.MagicMock()
        inst.list_positions.return_value = []
        repo_cls.return_value = inst
        orig = routes_prod.PositionsRepository
        routes_prod.PositionsRepository = repo_cls
        try:
            res = await routes_prod.list_positions(db, "p1")
        finally:
            routes_prod.PositionsRepository = orig
        self.assertEqual(res["total_positions"], 0)

    async def test_list_positions_except(self):
        routes_prod.DATABASE_MODE = True
        db = mock.MagicMock()
        repo_cls = mock.MagicMock()
        inst = mock.MagicMock()
        inst.list_positions.side_effect = RuntimeError("boom")
        repo_cls.return_value = inst
        orig = routes_prod.PositionsRepository
        routes_prod.PositionsRepository = repo_cls
        try:
            res = await routes_prod.list_positions(db, "p1")
        finally:
            routes_prod.PositionsRepository = orig
        self.assertEqual(res["positions"], [])

    async def test_list_positions_db_mode_false(self):
        routes_prod.DATABASE_MODE = False
        res = await routes_prod.list_positions(mock.MagicMock(), "p1")
        self.assertEqual(res["positions"], [])

    # ---- list_strategies ----
    async def test_list_strategies_success(self):
        routes_prod.DATABASE_MODE = True
        s1 = _Row(config_key="k", name="N", description="d", category="c",
                  status="active", last_backtest=DT)
        s2 = _Row(config_key=None, name=None, description=None, category=None,
                  status="active")
        db = make_db([s1, s2])
        res = await routes_prod.list_strategies(db)
        self.assertEqual(res["total_strategies"], 2)

    async def test_list_strategies_except(self):
        routes_prod.DATABASE_MODE = True
        res = await routes_prod.list_strategies(None)
        self.assertEqual(res["strategies"], [])

    async def test_list_strategies_db_mode_false(self):
        routes_prod.DATABASE_MODE = False
        res = await routes_prod.list_strategies(mock.MagicMock())
        self.assertEqual(res["strategies"], [])

    # ---- get_performance ----
    async def test_get_performance_success(self):
        routes_prod.DATABASE_MODE = True
        b1 = _Row(date=DT, current_balance=100.0, status="active")
        db = make_db([b1])
        res = await routes_prod.get_performance(db)
        self.assertGreater(res["portfolio_performance"]["current_nav_usd"], 0)

    async def test_get_performance_except(self):
        routes_prod.DATABASE_MODE = True
        res = await routes_prod.get_performance(None)
        self.assertEqual(res["portfolio_performance"], {})

    async def test_get_performance_db_mode_false(self):
        routes_prod.DATABASE_MODE = False
        res = await routes_prod.get_performance(mock.MagicMock())
        self.assertEqual(res["portfolio_performance"], {})

    # ---- get_approvals ----
    async def test_get_approvals_success(self):
        routes_prod.DATABASE_MODE = True
        ap1 = _Row(approval_id=1, approval_type="order", summary="s",
                   capital_affected=10, status="pending", approved_by="bob",
                   created_at=DT, liquidity_impact=5.0)
        ap2 = _Row(approval_id=2, approval_type=None, summary=None,
                   capital_affected=0, status="approved", created_at=DT)
        db = make_db([ap1, ap2])
        res = await routes_prod.get_approvals(db)
        self.assertEqual(res["pending_count"], 1)

    async def test_get_approvals_else_no_db(self):
        routes_prod.DATABASE_MODE = True
        res = await routes_prod.get_approvals(None)
        self.assertEqual(res["pending_count"], 0)

    async def test_get_approvals_db_mode_false(self):
        routes_prod.DATABASE_MODE = False
        res = await routes_prod.get_approvals(mock.MagicMock())
        self.assertEqual(res["pending_count"], 0)

    async def test_get_approvals_except(self):
        routes_prod.DATABASE_MODE = True
        db = make_db([], raise_query=True)
        res = await routes_prod.get_approvals(db)
        self.assertEqual(res["approvals"], [])

    # ---- get_research_hypotheses ----
    async def test_get_research_hypotheses_success(self):
        routes_prod.DATABASE_MODE = True
        n1 = _Row(id=1, title="T", content="C", status="active", created_at=DT)
        n2 = _Row(id=2)
        db = make_db([n1, n2])
        res = await routes_prod.get_research_hypotheses(db)
        self.assertEqual(len(res["hypotheses"]), 2)

    async def test_get_research_hypotheses_else_no_db(self):
        routes_prod.DATABASE_MODE = True
        res = await routes_prod.get_research_hypotheses(None)
        self.assertEqual(res["hypotheses"], [])

    async def test_get_research_hypotheses_db_mode_false(self):
        routes_prod.DATABASE_MODE = False
        res = await routes_prod.get_research_hypotheses(mock.MagicMock())
        self.assertEqual(res["hypotheses"], [])

    async def test_get_research_hypotheses_except(self):
        routes_prod.DATABASE_MODE = True
        db = make_db([], raise_query=True)
        res = await routes_prod.get_research_hypotheses(db)
        self.assertEqual(res["hypotheses"], [])

    # ---- endpoint_wrapper ----
    async def test_endpoint_wrapper_dict(self):
        async def f(db=None):
            return {"x": 1}
        res = await routes_prod.endpoint_wrapper(f, None)
        self.assertIn("timestamp", res)

    async def test_endpoint_wrapper_non_dict(self):
        async def f(db=None):
            return [1, 2, 3]
        res = await routes_prod.endpoint_wrapper(f, None)
        self.assertEqual(res, [1, 2, 3])


if __name__ == "__main__":
    unittest.main()
