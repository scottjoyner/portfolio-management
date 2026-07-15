import asyncio
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from _env import install_stubs  # noqa: E402

install_stubs()

from evaluation.pricing_models import (  # noqa: E402
    PriceEstimationEngine,
    PriceTargetModel,
    PositionQualityMetrics,
)


class TestPricingModels(unittest.TestCase):
    def test_enum_and_dataclass(self):
        self.assertEqual(PriceTargetModel.FUNDAMENTAL_BASED.value, "fundamental")
        m = PositionQualityMetrics(
            risk_score=0.2, alpha_score=1.0, beta_exposure=0.5,
            correlation_to_index=0.7, volatility_regime="low")
        self.assertEqual(m.volatility_regime, "low")

    def test_config(self):
        eng = PriceEstimationEngine(config={"price_source": "technical", "use_ml_models": True})
        self.assertEqual(eng.price_source, "technical")
        self.assertTrue(eng.use_ml_models)
        eng2 = PriceEstimationEngine()
        self.assertEqual(eng2.price_source, "fundamental")

    def test_estimate_price_all_models(self):
        eng = PriceEstimationEngine()
        for model in PriceTargetModel:
            out = asyncio.run(eng.estimate_price("ETH", model, {"current_price": "5000"}))
            self.assertIn("buy_level", out)
            self.assertIn("sell_level", out)
            self.assertEqual(out["model_used"], model.value)
            self.assertEqual(out["buy_level"], 4750.0)

    def test_estimate_price_numeric(self):
        eng = PriceEstimationEngine()
        out = asyncio.run(eng.estimate_price("ETH", PriceTargetModel.CONSENSUS_AVERAGE, {"current_price": 5000}))
        self.assertEqual(out["hold_level"], 5000.0)
        self.assertEqual(out["confidence_score"], 0.6)

    def test_position_quality_with_entry(self):
        eng = PriceEstimationEngine()
        m = asyncio.run(eng.calculate_position_quality({
            "entry_price": "4500", "current_price": "5000",
            "beta_exposure": 1.2, "correlation_to_index": 0.85,
            "volatility_regime": "high"}))
        self.assertEqual(m.beta_exposure, 1.2)
        self.assertEqual(m.volatility_regime, "high")
        self.assertAlmostEqual(m.alpha_score, (500.0 / 4500 * 100) * 10, places=2)

    def test_position_quality_zero_entry(self):
        eng = PriceEstimationEngine()
        m = asyncio.run(eng.calculate_position_quality({
            "entry_price": "0", "current_price": "5000",
            "beta_exposure": 1.0}))
        self.assertEqual(m.alpha_score, 0.0)
        self.assertEqual(m.volatility_regime, "moderate")

    def test_position_quality_default_regime(self):
        eng = PriceEstimationEngine()
        m = asyncio.run(eng.calculate_position_quality({
            "entry_price": "100", "current_price": "110"}))
        self.assertEqual(m.correlation_to_index, 0.7)
        self.assertEqual(m.beta_exposure, 1.0)


if __name__ == "__main__":
    unittest.main()
