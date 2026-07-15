import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from _env import install_stubs  # noqa: E402

install_stubs()

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import research.routes as rr  # noqa: E402
from research.incubation import IncubationReport  # noqa: E402


def _hyp(**kw):
    base = dict(
        hypothesis_id="h1", strategy_id="s1", philosophy="value",
        timeframe="1h", expected_edge="0.1", active=True,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        target_instruments="ti", holding_period="1d", signal_rules="sr",
        exit_rules="xr", risk_constraints="rc", config_hash="ch", version="1.0",
    )
    base.update(kw)
    return MagicMock(**base)


def _approval(**kw):
    base = dict(
        approval_id="a1", strategy_id="s1", hypothesis_id="h1", status="pending",
        required_approver="admin", approved_by=None, status_reason=None,
        expires_at=datetime(2024, 2, 1, tzinfo=timezone.utc),
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        strategy_approval_id="sa", product_id="BTC-USD", side="buy",
        order_type="limit", expected_slippage_bps=2.0, fill_risk_score=0.1,
    )
    base.update(kw)
    return MagicMock(**base)


def _cert(**kw):
    base = dict(
        id=1, hypothesis_id="h1", status="certified", sharpe=1.5,
        max_drawdown=0.1, live_transfer_confidence=0.8, fragility_score=0.2,
        rejection_reason=None,
        certified_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    base.update(kw)
    return MagicMock(**base)


class TestResearchRoutes(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(name="db")

    # ---------------- hypothesis registration -------------------------
    def test_register_hypothesis(self):
        hyp = _hyp()
        with patch.object(rr, "HypothesisRegistry") as R:
            R.return_value.register_hypothesis.return_value = hyp
            out = rr.register_hypothesis(
                rr.RegisterHypothesisRequest(
                    strategy_id="s1", philosophy="value", target_instruments=["ETH"],
                    timeframe="1h", holding_period="1d", signal_rules="r",
                    exit_rules="x", risk_constraints="rc", expected_edge="0.1"),
                db=self.db,
            )
        self.assertEqual(out["hypothesis_id"], "h1")
        self.assertEqual(out["status"], "registered")

    def test_list_hypotheses(self):
        hyp = _hyp()
        with patch.object(rr, "HypothesisRegistry") as R:
            R.return_value.list_hypotheses.return_value = [hyp]
            out = rr.list_hypotheses(active_only=True, db=self.db)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["hypothesis_id"], "h1")

    def test_list_hypotheses_inactive(self):
        with patch.object(rr, "HypothesisRegistry") as R:
            R.return_value.list_hypotheses.return_value = []
            out = rr.list_hypotheses(active_only=False, db=self.db)
        self.assertEqual(out, [])

    def test_get_strategy_hypotheses(self):
        hyp = _hyp()
        with patch.object(rr, "HypothesisRegistry") as R:
            R.return_value.get_strategy_hypotheses.return_value = [hyp]
            out = rr.get_strategy_hypotheses("s1", db=self.db)
        self.assertEqual(out[0]["config_hash"], "ch")

    # ---------------- certification -----------------------------------
    def test_certify_strategy(self):
        res = MagicMock(status="certified", sharpe=1.0, max_drawdown=0.1,
                        total_return=0.2, live_transfer_confidence=0.8,
                        fragility_score=0.2, check_details={}, rejection_reason=None)
        with patch.object(rr, "BacktestCertificationService") as C:
            C.return_value.certify.return_value = res
            out = rr.certify_strategy(rr.CertifyRequest(strategy_id="s1"), db=self.db)
        self.assertEqual(out["status"], "certified")

    def test_get_certifications(self):
        c1 = _cert(sharpe=1.5)
        c2 = _cert(sharpe=0.0, certified_at=None)
        with patch.object(rr, "HypothesisRegistry") as R:
            R.return_value.get_certifications.return_value = [c1, c2]
            out = rr.get_certifications("s1", db=self.db)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["sharpe"], 1.5)
        self.assertIsNone(out[1]["sharpe"])

    def test_verify_backtest_eligible(self):
        with patch.object(rr, "HypothesisRegistry") as R:
            R.return_value.verify_backtest_eligible.return_value = (True, "eligible")
            out = rr.verify_backtest_eligible("s1", db=self.db)
        self.assertTrue(out["eligible"])

    def test_verify_live_eligible(self):
        with patch.object(rr, "HypothesisRegistry") as R:
            R.return_value.verify_live_eligible.return_value = (False, "disabled")
            out = rr.verify_live_eligible("s1", db=self.db)
        self.assertFalse(out["eligible"])

    # ---------------- incubation --------------------------------------
    def _report(self):
        return IncubationReport(
            strategy_id="s1", task_id="t1", total_orders=2, total_fills=1,
            total_fill_value=Decimal("100"), avg_slippage_bps=5.0, fill_rate=0.5,
            realized_pnl=Decimal("0"), backtest_sharpe=None,
            backtest_max_drawdown=None, backtest_total_return=None,
            slippage_drift_bps=2.0, latency_drift_ms=0.0, fill_quality_ratio=1.0,
            started_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )

    def test_track_incubation(self):
        run = MagicMock(task_id="t1", strategy_id="s1", mode="paper",
                        status="running", started_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
                        queued_at=None)
        self.db.query.return_value.filter.return_value.order_by.return_value.first.return_value = run
        inst = MagicMock()
        inst.track_run.return_value = self._report()
        with patch.object(rr, "IncubationService", return_value=inst):
            out = rr.track_incubation(
                rr.TrackRunRequest(strategy_id="s1", total_orders=2, total_fills=1),
                db=self.db,
            )
        self.assertEqual(out["task_id"], "t1")
        self.assertEqual(out["total_orders"], 2)

    def test_track_incubation_creates_run(self):
        self.db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None
        inst = MagicMock()
        inst.track_run.return_value = self._report()
        with patch.object(rr, "IncubationService", return_value=inst):
            out = rr.track_incubation(
                rr.TrackRunRequest(strategy_id="s1", total_orders=2, total_fills=1),
                db=self.db,
            )
        self.assertEqual(out["task_id"], "t1")
        self.db.add.assert_called()

    def test_approve_incubation_ok(self):
        inst = MagicMock()
        inst.approve_for_live.return_value = (True, "approved")
        with patch.object(rr, "IncubationService", return_value=inst):
            out = rr.approve_incubation("t1", db=self.db)
        self.assertEqual(out["status"], "approved_for_live")

    def test_approve_incubation_fail(self):
        inst = MagicMock()
        inst.approve_for_live.return_value = (False, "run not found")
        with patch.object(rr, "IncubationService", return_value=inst):
            with self.assertRaises(Exception):  # HTTPException 400
                rr.approve_incubation("t1", db=self.db)

    def test_generate_shadow_payload(self):
        inst = MagicMock()
        inst.shadow_payload.return_value = {"shadow_id": "x"}
        with patch.object(rr, "IncubationService", return_value=inst):
            out = rr.generate_shadow_payload(
                rr.ShadowRequest(strategy_id="s1", product_id="BTC-USD", side="buy", size=10),
                db=self.db,
            )
        self.assertEqual(out["shadow_id"], "x")

    # ---------------- strategy approvals ------------------------------
    def test_create_strategy_approval(self):
        ap = _approval(expires_at=datetime(2024, 2, 1, tzinfo=timezone.utc))
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.create_strategy_approval.return_value = ap
            out = rr.create_strategy_approval(
                rr.CreateStrategyApprovalRequest(strategy_id="s1"), db=self.db)
        self.assertEqual(out["approval_id"], "a1")

    def test_list_strategy_approvals(self):
        ap = _approval()
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.list_strategy_approvals.return_value = [ap]
            out = rr.list_strategy_approvals(db=self.db)
        self.assertEqual(out[0]["approval_id"], "a1")

    def test_approve_strategy(self):
        ap = _approval(status="approved", approved_by="admin")
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.approve_strategy.return_value = ap
            out = rr.approve_strategy(
                "a1", rr.ApproveRejectRequest(actor="admin", reason="ok"), db=self.db)
        self.assertEqual(out["status"], "approved")

    def test_reject_strategy_with_reason(self):
        ap = _approval(status="rejected", status_reason="bad")
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.reject_strategy.return_value = ap
            out = rr.reject_strategy(
                "a1", rr.ApproveRejectRequest(actor="admin", reason="bad"), db=self.db)
        self.assertEqual(out["status_reason"], "bad")

    def test_reject_strategy_default_reason(self):
        ap = _approval(status="rejected", status_reason="Rejected")
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.reject_strategy.return_value = ap
            out = rr.reject_strategy(
                "a1", rr.ApproveRejectRequest(actor="admin"), db=self.db)
        self.assertEqual(out["status_reason"], "Rejected")

    def test_expire_pending_approvals(self):
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.expire_pending.return_value = 3
            out = rr.expire_pending_approvals(db=self.db)
        self.assertEqual(out["expired_count"], 3)

    # ---------------- trade approvals ---------------------------------
    def test_create_trade_approval(self):
        ta = _approval(approval_id="ta1")
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.create_trade_approval.return_value = ta
            out = rr.create_trade_approval(
                rr.CreateTradeApprovalRequest(strategy_id="s1", strategy_approval_id="sa",
                                             product_id="BTC-USD", side="buy"),
                db=self.db)
        self.assertEqual(out["approval_id"], "ta1")

    def test_list_trade_approvals(self):
        t1 = _approval(expected_slippage_bps=2.0, fill_risk_score=0.1)
        t2 = _approval(expected_slippage_bps=0.0, fill_risk_score=0.0)
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.list_trade_approvals.return_value = [t1, t2]
            out = rr.list_trade_approvals(db=self.db)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["expected_slippage_bps"], 2.0)
        self.assertIsNone(out[1]["expected_slippage_bps"])

    def test_approve_trade(self):
        ta = _approval(status="approved")
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.approve_trade.return_value = ta
            out = rr.approve_trade("ta1", rr.ApproveRejectRequest(actor="admin"), db=self.db)
        self.assertEqual(out["status"], "approved")

    def test_reject_trade(self):
        ta = _approval(status="rejected", status_reason="no")
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.reject_trade.return_value = ta
            out = rr.reject_trade("ta1", rr.ApproveRejectRequest(actor="admin", reason="no"),
                                  db=self.db)
        self.assertEqual(out["status_reason"], "no")

    def test_check_strategy_approved(self):
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.check_strategy_approved.return_value = (True, "a1")
            out = rr.check_strategy_approved("s1", db=self.db)
        self.assertTrue(out["approved"])

    def test_check_trade_approved(self):
        with patch.object(rr, "ApprovalService") as A:
            A.return_value.check_trade_approved.return_value = (False, "none")
            out = rr.check_trade_approved("s1", "BTC-USD", db=self.db)
        self.assertFalse(out["approved"])


class TestResearchRoutesClient(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(rr.router)
        from storage.postgres.session import get_db as real_get_db

        self.app.dependency_overrides[real_get_db] = lambda: MagicMock()
        self.client = TestClient(self.app)

    def test_client_register_success(self):
        hyp = _hyp()
        with patch.object(rr, "HypothesisRegistry") as R:
            R.return_value.register_hypothesis.return_value = hyp
            resp = self.client.post(
                "/research/hypotheses",
                json={
                    "strategy_id": "s1", "philosophy": "value",
                    "target_instruments": ["ETH"], "timeframe": "1h",
                    "holding_period": "1d", "signal_rules": "r",
                    "exit_rules": "x", "risk_constraints": "rc", "expected_edge": "0.1",
                },
            )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["hypothesis_id"], "h1")

    def test_client_approve_incubation_error(self):
        inst = MagicMock()
        inst.approve_for_live.return_value = (False, "run not found")
        with patch.object(rr, "IncubationService", return_value=inst):
            resp = self.client.post("/research/incubate/approve-live/t1")
        self.assertEqual(resp.status_code, 400)


if __name__ == "__main__":
    unittest.main()
