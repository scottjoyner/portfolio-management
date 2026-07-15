from __future__ import annotations

from unittest import IsolatedAsyncioTestCase

from research.hypothesis_generator import (
    Hypothesis,
    HypothesisGenerator,
    MarketRegime,
)


class TestHypothesisGenerator(IsolatedAsyncioTestCase):
    def test_dataclasses(self):
        r = MarketRegime(state="bull", volatility_percentile=0.3, correlation_matrix={"a": 1.0})
        self.assertEqual(r.state, "bull")
        h = Hypothesis(name="n", description="d", strategy_type="momentum",
                       target_instruments=["BTC"], expected_correlation=0.5, confidence_score=0.8)
        self.assertEqual(h.name, "n")

    def test_init_default(self):
        g = HypothesisGenerator()
        self.assertEqual(g.min_confidence, 0.6)

    def test_init_config(self):
        g = HypothesisGenerator(config={"min_hypothesis_confidence": 0.9})
        self.assertEqual(g.min_confidence, 0.9)

    async def test_analyze_signal_correlations(self):
        g = HypothesisGenerator()
        res = await g.analyze_signal_correlations(["ETH", "BTC"], {"prices": {}})
        self.assertIn("eth_btc_correlation", res)
        self.assertEqual(res["cross_asset_beta"], 1.2)

    async def test_detect_market_regime_bull(self):
        g = HypothesisGenerator()
        r = await g.detect_market_regime({"average_volatility_20d": 20})
        self.assertEqual(r.state, "bull")
        self.assertAlmostEqual(r.volatility_percentile, 20 / 60)

    async def test_detect_market_regime_bear(self):
        g = HypothesisGenerator()
        r = await g.detect_market_regime({"average_volatility_20d": 50})
        self.assertEqual(r.state, "bear")

    async def test_detect_market_regime_sideways(self):
        g = HypothesisGenerator()
        r = await g.detect_market_regime({"average_volatility_20d": 40})
        self.assertEqual(r.state, "sideways")

    async def test_detect_market_regime_str_vol(self):
        g = HypothesisGenerator()
        r = await g.detect_market_regime({"average_volatility_20d": "25"})
        self.assertEqual(r.state, "bull")

    async def test_detect_market_regime_default(self):
        g = HypothesisGenerator()
        r = await g.detect_market_regime({})
        self.assertAlmostEqual(r.volatility_percentile, 30 / 60)

    async def test_generate_bull_low_vol(self):
        g = HypothesisGenerator()
        hyps = await g.generate_hypotheses_from_regime(
            MarketRegime(state="bull", volatility_percentile=0.3), ["ETH", "BTC"])
        self.assertEqual(len(hyps), 1)
        self.assertEqual(hyps[0].strategy_type, "mean-reversion")

    async def test_generate_bull_high_vol_empty(self):
        g = HypothesisGenerator()
        hyps = await g.generate_hypotheses_from_regime(
            MarketRegime(state="bull", volatility_percentile=0.9), ["ETH"])
        self.assertEqual(hyps, [])

    async def test_generate_bear(self):
        g = HypothesisGenerator()
        hyps = await g.generate_hypotheses_from_regime(
            MarketRegime(state="bear", volatility_percentile=0.8))
        self.assertEqual(len(hyps), 1)
        self.assertEqual(hyps[0].strategy_type, "carry")

    async def test_generate_sideways(self):
        g = HypothesisGenerator()
        hyps = await g.generate_hypotheses_from_regime(
            MarketRegime(state="sideways", volatility_percentile=0.5))
        self.assertEqual(len(hyps), 1)
        self.assertEqual(hyps[0].strategy_type, "mean-reversion")


if __name__ == "__main__":
    import unittest

    unittest.main()
