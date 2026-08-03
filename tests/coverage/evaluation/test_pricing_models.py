import unittest

from _env import install_stubs

install_stubs()

from evaluation.pricing_models import (
    PriceEstimationEngine,
    PriceTargetModel,
    PositionQualityMetrics,
)


class TestPricingModels(unittest.TestCase):
    def test_enum_and_dataclass(self):
        self.assertEqual(PriceTargetModel.FUNDAMENTAL_BASED.value, "fundamental")
        metrics = PositionQualityMetrics(
            risk_score=0.2,
            alpha_score=1.0,
            beta_exposure=0.5,
            correlation_to_index=0.7,
            volatility_regime="low",
        )
        self.assertEqual(metrics.volatility_regime, "low")

    def test_config(self):
        engine = PriceEstimationEngine(config={"price_source": "technical", "use_ml_models": True})
        self.assertEqual(engine.price_source, "technical")
        self.assertTrue(engine.use_ml_models)
        self.assertEqual(PriceEstimationEngine().price_source, "fundamental")

    def test_estimate_price_all_models(self):
        engine = PriceEstimationEngine()
        for model in PriceTargetModel:
            result = engine.estimate_price("ETH", model, {"current_price": "5000"})
            self.assertIn("buy_level", result)
            self.assertIn("sell_level", result)
            self.assertEqual(result["model_used"], model.value)
            self.assertEqual(result["buy_level"], 4750.0)

    def test_estimate_price_numeric(self):
        result = PriceEstimationEngine().estimate_price(
            "ETH",
            PriceTargetModel.CONSENSUS_AVERAGE,
            {"current_price": 5000},
        )
        self.assertEqual(result["hold_level"], 5000.0)
        self.assertEqual(result["confidence_score"], 0.65)

    def test_position_quality_with_entry(self):
        metrics = PriceEstimationEngine().calculate_position_quality({
            "entry_price": "4500",
            "current_price": "5000",
            "beta_exposure": 1.2,
            "correlation_to_index": 0.85,
            "volatility_regime": "high",
        })
        self.assertEqual(metrics.beta_exposure, 1.0)
        self.assertEqual(metrics.volatility_regime, "high")
        self.assertAlmostEqual(metrics.alpha_score, (500.0 / 4500 * 100) * 100, places=2)

    def test_position_quality_zero_entry(self):
        metrics = PriceEstimationEngine().calculate_position_quality({
            "entry_price": "0",
            "current_price": "5000",
            "beta_exposure": 1.0,
        })
        self.assertEqual(metrics.alpha_score, 0.0)
        self.assertEqual(metrics.volatility_regime, "moderate")

    def test_position_quality_default_regime(self):
        metrics = PriceEstimationEngine().calculate_position_quality({
            "entry_price": "100",
            "current_price": "110",
        })
        self.assertEqual(metrics.correlation_to_index, 0.7)
        self.assertEqual(metrics.beta_exposure, 1.0)


if __name__ == "__main__":
    unittest.main()
