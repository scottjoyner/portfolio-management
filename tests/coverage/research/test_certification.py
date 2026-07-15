import os
import sys
import unittest
from unittest.mock import MagicMock, patch

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from _env import install_stubs  # noqa: E402

install_stubs()

from research.certification import (  # noqa: E402
    BacktestCertificationService,
    CertificationResult,
)


PASS = {
    "_run_sharpe_check": {"value": 1.0, "passed": True, "detail": "ok"},
    "_run_drawdown_check": {"value": 0.1, "passed": True, "detail": "ok"},
    "_run_return_check": {"value": 0.1, "passed": True, "detail": "ok"},
    "_run_walk_forward_check": {"value": 1.0, "decay": 0.1, "passed": True, "detail": "ok"},
    "_run_out_of_sample_check": {"value": 1.0, "passed": True, "detail": "ok"},
    "_run_multi_regime_check": {"value": 3, "passed": True, "detail": "ok"},
    "_run_sensitivity_check": {"value": 0.9, "passed": True, "detail": "ok"},
    "_run_tail_risk_check": {"value": 0.1, "passed": True, "detail": "ok"},
}

FAIL = {
    "_run_sharpe_check": {"value": 0.1, "passed": False, "detail": "ok"},
    "_run_drawdown_check": {"value": 0.4, "passed": False, "detail": "ok"},
    "_run_return_check": {"value": -0.1, "passed": False, "detail": "ok"},
    "_run_walk_forward_check": {"value": 0.1, "decay": 0.4, "passed": False, "detail": "ok"},
    "_run_out_of_sample_check": {"value": 0.1, "passed": False, "detail": "ok"},
    "_run_multi_regime_check": {"value": 1, "passed": False, "detail": "ok"},
    "_run_sensitivity_check": {"value": 0.4, "passed": False, "detail": "ok"},
    "_run_tail_risk_check": {"value": 0.25, "passed": False, "detail": "ok"},
}


def _make_svc(db, hypotheses, eligible=True, realism_fragility=0.3):
        reg = MagicMock(name="registry")
        reg.verify_backtest_eligible.return_value = (
            (True, "eligible") if eligible else (False, "no hypothesis")
        )
        reg.get_strategy_hypotheses.return_value = hypotheses

        def _assess(**kwargs):
            r = MagicMock(name="assessment")
            r.live_transfer_confidence = 0.8
            r.fragility_score = realism_fragility
            r.expected_live_return = 0.1
            return r

        with patch("research.certification.HypothesisRegistry", return_value=reg):
            svc = BacktestCertificationService(db)
        patch(
            "research.certification.BacktestRealismScorer.assess_strategy",
            side_effect=_assess,
        ).start()
        return svc, reg


class TestCertification(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(name="db")

    def _patch_checks(self, svc, fail_name=None):
        for name, val in PASS.items():
            setattr(svc, name, lambda config=None, _d=val: _d)
        if fail_name:
            setattr(svc, fail_name, lambda config=None, _d=FAIL[fail_name]: _d)

    def test_certified(self):
        hyp = [MagicMock(hypothesis_id="h1")]
        svc, reg = _make_svc(self.db, hyp)
        self._patch_checks(svc)
        res = svc.certify("s1", config={"holding_period": "intraday"})
        self.assertEqual(res.status, "certified")
        self.assertEqual(res.win_rate, 0.62)
        self.assertEqual(res.profit_factor, 1.45)
        reg.record_certification.assert_called_once()

    def test_certified_no_hypothesis(self):
        svc, reg = _make_svc(self.db, [])
        self._patch_checks(svc)
        res = svc.certify("s1")
        self.assertEqual(res.status, "certified")
        reg.record_certification.assert_not_called()

    def test_not_eligible(self):
        svc, reg = _make_svc(self.db, [], eligible=False)
        self._patch_checks(svc)
        res = svc.certify("s1")
        self.assertEqual(res.status, "rejected")
        self.assertEqual(res.check_details["gate"], "hypothesis")
        self.assertEqual(res.rejection_reason, "no hypothesis")

    def test_each_failure_branch(self):
        for name in FAIL:
            with self.subTest(check=name):
                hyp = [MagicMock(hypothesis_id="h1")]
                svc, reg = _make_svc(self.db, hyp)
                self._patch_checks(svc, fail_name=name)
                res = svc.certify("s1")
                self.assertEqual(res.status, "rejected")
                self.assertTrue(res.rejection_reason)

    def test_fragility_failure(self):
        hyp = [MagicMock(hypothesis_id="h1")]
        svc, reg = _make_svc(self.db, hyp, realism_fragility=0.9)
        self._patch_checks(svc)
        res = svc.certify("s1")
        self.assertEqual(res.status, "rejected")
        self.assertIn("Fragility", res.rejection_reason)

    def test_check_methods_execute(self):
        svc, _ = _make_svc(self.db, [])
        for name in PASS:
            out = getattr(svc, name)(None)
            self.assertIn("value", out)
            self.assertIn("passed", out)
        # ensure random-based checks run without error
        for name in PASS:
            self.assertIsInstance(getattr(svc, name)({"a": 1})["value"], (int, float))


if __name__ == "__main__":
    unittest.main()
