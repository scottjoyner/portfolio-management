import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from _env import install_stubs  # noqa: E402

install_stubs()

from research.approval import ApprovalService  # noqa: E402


class _Q:
    def __init__(self, first=None, all=None):
        self._first = first
        self._all = all if all is not None else []

    def filter(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self

    def first(self):
        return self._first

    def all(self):
        return self._all


class TestApprovalService(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(name="db")
        self.store = {}
        self.db.query.side_effect = lambda model: _Q(
            first=self.store.get(model.__name__ + "_first"),
            all=self.store.get(model.__name__ + "_all", []),
        )
        self.reg_mock = MagicMock(name="registry")
        with patch("research.approval.HypothesisRegistry", return_value=self.reg_mock):
            self.svc = ApprovalService(self.db)

    # ---- create_strategy_approval --------------------------------------
    def test_create_with_hypothesis_and_certs(self):
        cfg = MagicMock(name="cfg", config_hash="ch", hypothesis_id="h1")
        hyp = MagicMock(name="hyp", hypothesis_id="h1", config_hash="ch",
                        philosophy="value", target_instruments="ti",
                        signal_rules="sr", exit_rules="xr", expected_edge="ee")
        cert = MagicMock(name="cert", sharpe=1.0, max_drawdown=0.1, total_return=0.2,
                         live_transfer_confidence=0.8, fragility_score=0.2, status="certified")
        self.store["StrategyConfig_first"] = cfg
        self.store["StrategyHypothesis_first"] = hyp
        self.reg_mock.get_certifications.return_value = [cert]
        ap = self.svc.create_strategy_approval("s1", hypothesis_id="h1")
        self.assertTrue(ap.approval_id.startswith("sa-s1-"))
        self.assertEqual(ap.status, "pending")
        self.assertIsNotNone(ap.expires_at)
        self.assertEqual(ap.hypothesis_id, "h1")
        self.assertIn("sharpe", ap.backtest_evidence_json)

    def test_create_elif_cfg_hypothesis(self):
        cfg = MagicMock(name="cfg", config_hash="ch", hypothesis_id="hX")
        hyp = MagicMock(name="hyp", hypothesis_id="hX")
        self.store["StrategyConfig_first"] = cfg
        self.store["StrategyHypothesis_first"] = hyp
        self.reg_mock.get_certifications.return_value = []
        ap = self.svc.create_strategy_approval("s1")
        self.assertEqual(ap.hypothesis_id, "hX")

    def test_create_no_cfg_all_none_certs(self):
        cert = MagicMock(name="cert", sharpe=None, max_drawdown=None, total_return=None,
                         live_transfer_confidence=None, fragility_score=None, status="x")
        self.store["StrategyConfig_first"] = None
        self.reg_mock.get_certifications.return_value = [cert]
        ap = self.svc.create_strategy_approval("s1")
        self.assertIsNone(ap.hypothesis_id)
        self.assertIsNotNone(ap.backtest_evidence_json)

    # ---- approve_strategy ----------------------------------------------
    def test_approve_not_found(self):
        self.store["StrategyApproval_first"] = None
        with self.assertRaises(ValueError):
            self.svc.approve_strategy("aid", "admin")

    def test_approve_not_pending(self):
        ap = MagicMock(name="ap", status="approved")
        self.store["StrategyApproval_first"] = ap
        with self.assertRaises(ValueError):
            self.svc.approve_strategy("aid", "admin")

    def test_approve_success(self):
        ap = MagicMock(name="ap", status="pending", strategy_id="s1")
        self.store["StrategyApproval_first"] = ap
        cfg = MagicMock(name="cfg")
        self.store["StrategyConfig_first"] = cfg
        out = self.svc.approve_strategy("aid", "admin", notes="ok")
        self.assertEqual(out.status, "approved")
        self.assertEqual(cfg.status, "approved")

    def test_approve_no_cfg(self):
        ap = MagicMock(name="ap", status="pending", strategy_id="s1")
        self.store["StrategyApproval_first"] = ap
        self.store["StrategyConfig_first"] = None
        out = self.svc.approve_strategy("aid", "admin")
        self.assertEqual(out.status, "approved")

    # ---- reject_strategy -----------------------------------------------
    def test_reject_not_found(self):
        self.store["StrategyApproval_first"] = None
        with self.assertRaises(ValueError):
            self.svc.reject_strategy("aid", "admin", "reason")

    def test_reject_success(self):
        ap = MagicMock(name="ap", status="pending")
        self.store["StrategyApproval_first"] = ap
        out = self.svc.reject_strategy("aid", "admin", "bad")
        self.assertEqual(out.status, "rejected")

    # ---- expire_pending ------------------------------------------------
    def test_expire_pending(self):
        now = datetime.now(timezone.utc)
        a1 = MagicMock(name="a1", expires_at=None)
        a2 = MagicMock(name="a2", expires_at=(now - timedelta(days=1)).replace(tzinfo=None))
        a3 = MagicMock(name="a3", expires_at=(now + timedelta(days=1)).replace(tzinfo=None))
        a4 = MagicMock(name="a4", expires_at=now - timedelta(days=1))
        a5 = MagicMock(name="a5", expires_at=now + timedelta(days=1))
        self.store["StrategyApproval_all"] = [a1, a2, a3, a4, a5]
        n = self.svc.expire_pending()
        self.assertEqual(n, 2)
        self.assertEqual(a2.status, "expired")
        self.assertEqual(a4.status, "expired")

    # ---- check_strategy_approved ---------------------------------------
    def test_check_strategy_no_active(self):
        self.store["StrategyApproval_first"] = None
        ok, ref = self.svc.check_strategy_approved("s1")
        self.assertFalse(ok)

    def test_check_strategy_active_no_expiry(self):
        ap = MagicMock(name="ap", approval_id="aid", expires_at=None)
        self.store["StrategyApproval_first"] = ap
        ok, ref = self.svc.check_strategy_approved("s1")
        self.assertTrue(ok)
        self.assertEqual(ref, "aid")

    def test_check_strategy_expired_naive(self):
        now = datetime.now(timezone.utc)
        ap = MagicMock(name="ap", approval_id="aid",
                       expires_at=(now - timedelta(days=1)).replace(tzinfo=None))
        self.store["StrategyApproval_first"] = ap
        ok, ref = self.svc.check_strategy_approved("s1")
        self.assertFalse(ok)
        self.assertEqual(ref, "strategy approval expired")

    def test_check_strategy_expired_aware(self):
        now = datetime.now(timezone.utc)
        ap = MagicMock(name="ap", approval_id="aid", expires_at=now - timedelta(days=1))
        self.store["StrategyApproval_first"] = ap
        ok, ref = self.svc.check_strategy_approved("s1")
        self.assertFalse(ok)

    def test_check_strategy_future_aware(self):
        now = datetime.now(timezone.utc)
        ap = MagicMock(name="ap", approval_id="aid", expires_at=now + timedelta(days=1))
        self.store["StrategyApproval_first"] = ap
        ok, ref = self.svc.check_strategy_approved("s1")
        self.assertTrue(ok)

    # ---- trade approvals ------------------------------------------------
    def test_create_trade_approval_no_bounds(self):
        ta = self.svc.create_trade_approval(
            strategy_id="s1", strategy_approval_id="sa", account="a", venue="v",
            product_id="BTC-USD", side="buy", order_type="limit",
        )
        self.assertTrue(ta.approval_id.startswith("ta-"))
        self.assertIsNone(ta.order_bounds_json)

    def test_create_trade_approval_with_bounds(self):
        ta = self.svc.create_trade_approval(
            strategy_id="s1", strategy_approval_id="sa", account="a", venue="v",
            product_id="BTC-USD", side="buy", order_type="limit",
            order_bounds={"low": 1, "high": 2},
        )
        self.assertIn("low", ta.order_bounds_json)

    def test_approve_trade_not_found(self):
        self.store["TradeApproval_first"] = None
        with self.assertRaises(ValueError):
            self.svc.approve_trade("tid", "admin")

    def test_approve_trade_success(self):
        ta = MagicMock(name="ta", status="pending")
        self.store["TradeApproval_first"] = ta
        out = self.svc.approve_trade("tid", "admin")
        self.assertEqual(out.status, "approved")

    def test_reject_trade_not_found(self):
        self.store["TradeApproval_first"] = None
        with self.assertRaises(ValueError):
            self.svc.reject_trade("tid", "admin", "reason")

    def test_reject_trade_success(self):
        ta = MagicMock(name="ta", status="pending")
        self.store["TradeApproval_first"] = ta
        out = self.svc.reject_trade("tid", "admin", "bad")
        self.assertEqual(out.status, "rejected")

    def test_check_trade_not_found(self):
        self.store["TradeApproval_first"] = None
        ok, ref = self.svc.check_trade_approved("s1", "BTC-USD")
        self.assertFalse(ok)

    def test_check_trade_found(self):
        ta = MagicMock(name="ta", approval_id="tid")
        self.store["TradeApproval_first"] = ta
        ok, ref = self.svc.check_trade_approved("s1", "BTC-USD")
        self.assertTrue(ok)
        self.assertEqual(ref, "tid")

    # ---- list methods ---------------------------------------------------
    def test_list_strategy_approvals(self):
        lst = [MagicMock()]
        self.store["StrategyApproval_all"] = lst
        self.assertEqual(self.svc.list_strategy_approvals(), lst)
        self.assertEqual(self.svc.list_strategy_approvals(strategy_id="s1", status="x"), lst)

    def test_list_trade_approvals(self):
        lst = [MagicMock()]
        self.store["TradeApproval_all"] = lst
        self.assertEqual(self.svc.list_trade_approvals(), lst)
        self.assertEqual(self.svc.list_trade_approvals(strategy_id="s1", status="x"), lst)

    def test_get_strategy_approval(self):
        ap = MagicMock()
        self.store["StrategyApproval_first"] = ap
        self.assertIs(self.svc.get_strategy_approval("aid"), ap)


if __name__ == "__main__":
    unittest.main()
