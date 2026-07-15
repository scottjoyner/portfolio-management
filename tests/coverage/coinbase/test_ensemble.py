import unittest
from coinbase.src.ensemble import (
    StrategyPosterior, BayesianSignalBlender, StrategyConfidenceAggregator,
)
from coinbase.src.protocols import Direction, Opportunity


def make_opp(direction, score=1.0, name="s"):
    return Opportunity(product_id="BTC-USD", direction=direction, instrument_type=None,
                       entry_price=1, stop_price=0.9, target_price=1.1, risk_reward=2,
                       confidence=0.5, reason="r", strategy_name=name, score=score)


class TestStrategyPosterior(unittest.TestCase):
    def test_win_rate(self):
        p = StrategyPosterior(name="s", alpha=3, beta=1)
        self.assertAlmostEqual(p.win_rate, 0.75)

    def test_uncertainty(self):
        p = StrategyPosterior(name="s", alpha=1, beta=1)
        self.assertEqual(p.uncertainty, 1.0)
        p2 = StrategyPosterior(name="s", alpha=10, beta=10)
        self.assertLess(p2.uncertainty, 1.0)

    def test_credible_lower(self):
        p = StrategyPosterior(name="s", alpha=10, beta=2)
        self.assertGreater(p.credible_lower, 0)

    def test_weight(self):
        p = StrategyPosterior(name="s", alpha=10, beta=2)
        self.assertAlmostEqual(p.weight, p.credible_lower)

    def test_update(self):
        p = StrategyPosterior(name="s")
        p.update(True)
        self.assertEqual(p.alpha, 2.0)
        self.assertEqual(p.wins, 1)
        p.update(False)
        self.assertEqual(p.beta, 2.0)

    def test_merge(self):
        p = StrategyPosterior(name="s", alpha=3, beta=1, trades=5, wins=4)
        other = StrategyPosterior(name="o", alpha=2, beta=2, trades=2, wins=1)
        p.merge(other)
        self.assertEqual(p.alpha, 3 + (2 - 1))
        self.assertEqual(p.trades, 7)


class TestBayesianSignalBlender(unittest.TestCase):
    def test_get_or_create(self):
        b = BayesianSignalBlender()
        p = b.get_or_create("s")
        self.assertIs(p, b.get_or_create("s"))
        self.assertEqual(p.alpha, 1.0)

    def test_record_and_weight_default(self):
        b = BayesianSignalBlender()
        b.record("s", True)
        w = b.weight("s")
        self.assertGreaterEqual(w, 0.0)
        self.assertLessEqual(w, 1.0)

    def test_weight_unknown(self):
        b = BayesianSignalBlender()
        self.assertEqual(b.weight("unknown"), 1.0)

    def test_weight_regime_blend(self):
        b = BayesianSignalBlender()
        for _ in range(3):
            b.record("s", True, regime="trend")
        w = b.weight("s", "trend")
        self.assertGreaterEqual(w, 0.0)
        self.assertLessEqual(w, 1.0)

    def test_blend_signals(self):
        b = BayesianSignalBlender()
        opps = [make_opp(Direction.LONG, score=1.0, name="s"),
                make_opp(Direction.LONG, score=2.0, name="s")]
        blended = b.blend_signals(opps)
        self.assertEqual(len(blended), 2)
        self.assertIn("bayesian_weight", blended[0].meta)

    def test_top_strategies(self):
        b = BayesianSignalBlender()
        for _ in range(4):
            b.record("s", True)
        top = b.top_strategies(n=5)
        self.assertTrue(any(t["name"] == "s" for t in top))

    def test_strategy_universe(self):
        b = BayesianSignalBlender()
        for _ in range(4):
            b.record("s", True)
        universe = b.strategy_universe()
        self.assertIn("s", universe)

    def test_to_dict(self):
        b = BayesianSignalBlender()
        b.record("s", True)
        d = b.to_dict()
        self.assertIn("s", d)


class TestStrategyConfidenceAggregator(unittest.TestCase):
    def test_aggregate_empty(self):
        agg = StrategyConfidenceAggregator()
        self.assertEqual(agg.aggregate([]), [])

    def test_aggregate_single_direction(self):
        agg = StrategyConfidenceAggregator()
        opps = [make_opp(Direction.LONG, score=1.0, name="s"),
                make_opp(Direction.LONG, score=2.0, name="s2")]
        res = agg.aggregate(opps)
        self.assertEqual(len(res), 1)
        self.assertIn("ensemble_score", res[0].meta)

    def test_aggregate_zero_weight_skipped(self):
        agg = StrategyConfidenceAggregator()
        opps = [make_opp(Direction.LONG, score=0.0, name="s"),
                make_opp(Direction.SHORT, score=2.0, name="s2")]
        res = agg.aggregate(opps)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].direction, Direction.SHORT)

    def test_aggregate_both_directions(self):
        agg = StrategyConfidenceAggregator()
        opps = [make_opp(Direction.LONG, score=1.0, name="s"),
                make_opp(Direction.SHORT, score=1.0, name="s2")]
        res = agg.aggregate(opps)
        self.assertEqual(len(res), 2)


if __name__ == "__main__":
    unittest.main()
