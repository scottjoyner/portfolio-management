import sys
import types
import unittest
from unittest import mock

import trading_system.approval.models as am


class FakePEC:
    pass


class FakeEngine:
    def __init__(self, config):
        self.config = config


class TestApprovalModels(unittest.TestCase):
    def test_approval_status_enum(self):
        self.assertEqual(am.ApprovalStatus.PENDING.value, "pending")
        self.assertIn(am.ApprovalStatus.APPROVED, list(am.ApprovalStatus))

    def test_create_evaluation_engine_no_db(self):
        fake_mod = types.ModuleType("evaluation.pricing_models")
        fake_mod.PriceEstimationEngine = FakeEngine
        sys.modules["evaluation.pricing_models"] = fake_mod
        am.EvaluationConfiguration = FakePEC
        try:
            res = am.create_evaluation_engine()
        finally:
            del sys.modules["evaluation.pricing_models"]
            del am.EvaluationConfiguration
        self.assertIsInstance(res, FakeEngine)
        self.assertEqual(res.config, {})

    def test_create_evaluation_engine_with_config(self):
        fake_mod = types.ModuleType("evaluation.pricing_models")
        fake_mod.PriceEstimationEngine = FakeEngine
        sys.modules["evaluation.pricing_models"] = fake_mod
        am.EvaluationConfiguration = FakePEC
        cfg_row = types.SimpleNamespace(
            price_source="db", use_ml_models=True,
            volatility_threshold_high=0.7, volatility_threshold_extreme=0.9,
        )
        db = mock.MagicMock()
        db.query.return_value.first.return_value = cfg_row
        try:
            res = am.create_evaluation_engine(db)
        finally:
            del sys.modules["evaluation.pricing_models"]
            del am.EvaluationConfiguration
        self.assertEqual(res.config["price_source"], "db")

    def test_create_evaluation_engine_no_config_row(self):
        fake_mod = types.ModuleType("evaluation.pricing_models")
        fake_mod.PriceEstimationEngine = FakeEngine
        sys.modules["evaluation.pricing_models"] = fake_mod
        am.EvaluationConfiguration = FakePEC
        db = mock.MagicMock()
        db.query.return_value.first.return_value = None
        try:
            res = am.create_evaluation_engine(db)
        finally:
            del sys.modules["evaluation.pricing_models"]
            del am.EvaluationConfiguration
        self.assertEqual(res.config, {})


if __name__ == "__main__":
    unittest.main()
