import asyncio
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from _env import install_stubs  # noqa: E402

install_stubs()

from research.hypothesis_generator import (  # noqa: E402
    HypothesisGenerator,
    MarketRegime,
    Hypothesis,
)


class TestHypothesisGenerator(unittest.TestCase):
    def setUp(self):
        self.gen = HypothesisGenerator()
        self.gen_cfg = HypothesisGenerator(config={"min_hypothesis_confidence": 0.9})

    def test_init_uses_config(self):
        self.assertEqual(self.gen.min_confidence, 0.6)
        self.assertEqual(self.gen_cfg.min_confidence, 0.9)

    def test_analyze_signal_correlations(self):
        out = asyncio.run(
            self.gen.analyze_signal_correlations(
                ["ETH", "BTC"], {"prices": {"ETH": [1], "BTC": [2]}}
            )
        )
        self.assertIn("eth_btc_correlation", out)
        self.assertIn("cross_asset_beta", out)

    def test_detect_market_regime_bull(self):
        reg = asyncio.run(self.gen.detect_market_regime({"average_volatility_20d": 20}))
        self.assertEqual(reg.state, "bull")
        self.assertAlmostEqual(reg.volatility_percentile, 20 / 60)

    def test_detect_market_regime_bear(self):
        reg = asyncio.run(self.gen.detect_market_regime({"average_volatility_20d": 50}))
        self.assertEqual(reg.state, "bear")
        self.assertAlmostEqual(reg.volatility_percentile, 50 / 60)

    def test_detect_market_regime_sideways(self):
        reg = asyncio.run(self.gen.detect_market_regime({"average_volatility_20d": 35}))
        self.assertEqual(reg.state, "sideways")
        self.assertAlmostEqual(reg.volatility_percentile, 35 / 60)

    def test_detect_market_regime_default(self):
        reg = asyncio.run(self.gen.detect_market_regime({}))
        self.assertEqual(reg.state, "sideways")

    def test_generate_bull(self):
        reg = MarketRegime(state="bull", volatility_percentile=0.35)
        hyps = asyncio.run(self.gen.generate_hypotheses_from_regime(reg))
        self.assertEqual(len(hyps), 1)
        self.assertEqual(hyps[0].strategy_type, "mean-reversion")

    def test_generate_bear(self):
        reg = MarketRegime(state="bear", volatility_percentile=0.5)
        hyps = asyncio.run(self.gen.generate_hypotheses_from_regime(reg))
        self.assertEqual(len(hyps), 1)
        self.assertEqual(hyps[0].strategy_type, "carry")

    def test_generate_sideways(self):
        reg = MarketRegime(state="sideways", volatility_percentile=0.5)
        hyps = asyncio.run(self.gen.generate_hypotheses_from_regime(reg))
        self.assertEqual(len(hyps), 1)
        self.assertEqual(hyps[0].strategy_type, "mean-reversion")

    def test_generate_none(self):
        reg = MarketRegime(state="unknown", volatility_percentile=0.9)
        hyps = asyncio.run(self.gen.generate_hypotheses_from_regime(reg))
        self.assertEqual(hyps, [])

    def test_generate_with_instruments(self):
        reg = MarketRegime(state="bull", volatility_percentile=0.1)
        hyps = asyncio.run(
            self.gen.generate_hypotheses_from_regime(reg, ["SOL", "AVAX"])
        )
        self.assertEqual(len(hyps), 1)


if __name__ == "__main__":
    unittest.main()
