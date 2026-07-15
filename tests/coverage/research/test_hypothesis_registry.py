import os
import sys
import unittest
from unittest.mock import MagicMock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from _env import install_stubs  # noqa: E402

install_stubs()

from research import hypothesis_registry as hr  # noqa: E402
from research.hypothesis_registry import (  # noqa: E402
    HypothesisRegistry,
    compute_config_hash,
)


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


class TestHypothesisRegistry(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(name="db")
        self.store = {}
        self.db.query.side_effect = lambda model: _Q(
            first=self.store.get(model.__name__ + "_first"),
            all=self.store.get(model.__name__ + "_all", []),
        )
        self.reg = HypothesisRegistry(self.db)

    def test_compute_config_hash(self):
        h = compute_config_hash({"a": 1, "b": 2})
        self.assertEqual(len(h), 16)
        # deterministic
        self.assertEqual(h, compute_config_hash({"a": 1, "b": 2}))
        self.assertNotEqual(h, compute_config_hash({"a": 1, "b": 3}))

    def test_register_new_config(self):
        self.store["StrategyConfig_first"] = None
        hyp = self.reg.register_hypothesis(
            strategy_id="s1", philosophy="value", target_instruments=["ETH"],
            timeframe="1h", holding_period="1d", signal_rules="r", exit_rules="x",
            risk_constraints="rc", expected_edge="0.1", author="me",
            config={"k": "v"},
        )
        self.assertTrue(hyp.hypothesis_id.startswith("hyp-s1-"))
        self.db.add.assert_called()
        self.db.commit.assert_called()

    def test_register_existing_config(self):
        cfg = MagicMock(name="cfg")
        self.store["StrategyConfig_first"] = cfg
        hyp = self.reg.register_hypothesis(
            strategy_id="s1", philosophy="value", target_instruments=["ETH"],
            timeframe="1h", holding_period="1d", signal_rules="r", exit_rules="x",
            risk_constraints="rc", expected_edge="0.1",
        )
        self.assertEqual(cfg.hypothesis_id, hyp.hypothesis_id)
        self.assertEqual(cfg.config_hash, hyp.config_hash)

    def test_get_hypothesis(self):
        h = MagicMock()
        self.store["StrategyHypothesis_first"] = h
        self.assertIs(self.reg.get_hypothesis("hid"), h)

    def test_get_strategy_hypotheses(self):
        lst = [MagicMock()]
        self.store["StrategyHypothesis_all"] = lst
        self.assertEqual(self.reg.get_strategy_hypotheses("s1"), lst)

    def test_list_hypotheses_active_only(self):
        lst = [MagicMock()]
        self.store["StrategyHypothesis_all"] = lst
        self.assertEqual(self.reg.list_hypotheses(active_only=True), lst)
        self.assertEqual(self.reg.list_hypotheses(active_only=False), lst)

    def test_verify_backtest_eligible_no_hyp(self):
        self.store["StrategyHypothesis_first"] = None
        ok, reason = self.reg.verify_backtest_eligible("s1")
        self.assertFalse(ok)
        self.assertIn("no active", reason)

    def test_verify_backtest_eligible_ok(self):
        hyp = MagicMock()
        self.store["StrategyHypothesis_first"] = hyp
        self.store["StrategyConfig_first"] = None
        ok, reason = self.reg.verify_backtest_eligible("s1")
        self.assertTrue(ok)
        self.assertEqual(reason, "eligible")

    def test_verify_backtest_eligible_mismatch(self):
        hyp = MagicMock()
        cfg = MagicMock()
        cfg.hypothesis_id = "other"
        self.store["StrategyHypothesis_first"] = hyp
        self.store["StrategyConfig_first"] = cfg
        ok, reason = self.reg.verify_backtest_eligible("s1")
        self.assertTrue(ok)
        self.assertEqual(cfg.hypothesis_id, hyp.hypothesis_id)

    def test_record_certification_non_certified(self):
        self.store["StrategyConfig_first"] = None
        cert = self.reg.record_certification(
            hypothesis_id="h1", strategy_id="s1", status="rejected",
            sharpe=0.5, max_drawdown=0.2,
        )
        self.assertEqual(cert.status, "rejected")
        self.db.add.assert_called()
        self.db.commit.assert_called()

    def test_record_certification_certified(self):
        cfg = MagicMock()
        self.store["StrategyConfig_first"] = cfg
        cert = self.reg.record_certification(
            hypothesis_id="h1", strategy_id="s1", status="certified",
            sharpe=1.5, max_drawdown=0.1,
        )
        self.assertEqual(cert.status, "certified")
        self.assertEqual(cfg.certification_status, "certified")

    def test_record_certification_certified_no_cfg(self):
        self.store["StrategyConfig_first"] = None
        cert = self.reg.record_certification(
            hypothesis_id="h1", strategy_id="s1", status="certified",
            sharpe=1.5, max_drawdown=0.1,
        )
        self.assertEqual(cert.status, "certified")

    def test_get_certifications(self):
        lst = [MagicMock()]
        self.store["StrategyCertification_all"] = lst
        self.assertEqual(self.reg.get_certifications("s1"), lst)

    def test_verify_live_eligible_no_cfg(self):
        self.store["StrategyConfig_first"] = None
        ok, reason = self.reg.verify_live_eligible("s1")
        self.assertFalse(ok)
        self.assertIn("not found", reason)

    def test_verify_live_eligible_not_certified(self):
        cfg = MagicMock()
        cfg.certification_status = "pending"
        cfg.enabled = True
        self.store["StrategyConfig_first"] = cfg
        ok, reason = self.reg.verify_live_eligible("s1")
        self.assertFalse(ok)
        self.assertIn("certified", reason)

    def test_verify_live_eligible_disabled(self):
        cfg = MagicMock()
        cfg.certification_status = "certified"
        cfg.enabled = False
        self.store["StrategyConfig_first"] = cfg
        ok, reason = self.reg.verify_live_eligible("s1")
        self.assertFalse(ok)
        self.assertIn("disabled", reason)

    def test_verify_live_eligible_ok(self):
        cfg = MagicMock()
        cfg.certification_status = "certified"
        cfg.enabled = True
        self.store["StrategyConfig_first"] = cfg
        ok, reason = self.reg.verify_live_eligible("s1")
        self.assertTrue(ok)
        self.assertEqual(reason, "eligible")


if __name__ == "__main__":
    unittest.main()
