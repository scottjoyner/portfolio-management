import unittest
from coinbase.src.confluence import (
    MultiTimeframeConfluence, TimeframeSignal, OrderBookImbalanceStrategy,
)
from coinbase.src.protocols import Direction, Opportunity


def make_sig(tf, direction, conf):
    return TimeframeSignal(tf, direction, conf, "r")


class TestTimeframeSignal(unittest.TestCase):
    def test_props(self):
        b = make_sig("1h", Direction.LONG, 0.8)
        self.assertTrue(b.is_bullish)
        self.assertFalse(b.is_bearish)
        self.assertAlmostEqual(b.strength, 0.8)
        s = make_sig("1h", Direction.SHORT, 0.6)
        self.assertTrue(s.is_bearish)
        self.assertAlmostEqual(s.strength, 0.6)
        n = make_sig("1h", None, 0.6)
        self.assertAlmostEqual(n.strength, 0.0)


class TestMultiTimeframeConfluence(unittest.TestCase):
    def test_evaluate_empty(self):
        c = MultiTimeframeConfluence()
        res = c.evaluate("BTC-USD", {})
        self.assertIsNone(res.overall_direction)
        self.assertEqual(res.agreement_pct, 0.0)

    def test_evaluate_total_zero(self):
        c = MultiTimeframeConfluence()
        res = c.evaluate("BTC-USD", {"1h": [make_sig("1h", Direction.LONG, 0.0)]})
        self.assertIsNone(res.overall_direction)

    def test_evaluate_bullish(self):
        c = MultiTimeframeConfluence()
        sigs = {"1h": [make_sig("1h", Direction.LONG, 0.8)],
                "4h": [make_sig("4h", Direction.LONG, 0.6)]}
        res = c.evaluate("BTC-USD", sigs)
        self.assertEqual(res.overall_direction, Direction.LONG)
        self.assertGreater(res.agreement_pct, 0)

    def test_evaluate_bearish(self):
        c = MultiTimeframeConfluence()
        sigs = {"1h": [make_sig("1h", Direction.SHORT, 0.8)]}
        res = c.evaluate("BTC-USD", sigs)
        self.assertEqual(res.overall_direction, Direction.SHORT)

    def test_evaluate_divergence(self):
        c = MultiTimeframeConfluence()
        sigs = {"1h": [make_sig("1h", Direction.LONG, 0.8)],
                "4h": [make_sig("4h", Direction.SHORT, 0.8)]}
        res = c.evaluate("BTC-USD", sigs)
        self.assertTrue(res.divergence_detected)

    def test_evaluate_dominant_tf(self):
        c = MultiTimeframeConfluence()
        sigs = {"1h": [make_sig("1h", Direction.LONG, 0.8)],
                "4h": [make_sig("4h", Direction.LONG, 0.8)]}
        res = c.evaluate("BTC-USD", sigs)
        self.assertIn(res.dominant_timeframe, ("1h", "4h"))

    def test_boost_conflict(self):
        c = MultiTimeframeConfluence()
        opp = Opportunity(product_id="BTC-USD", direction=Direction.SHORT, instrument_type=None,
                         entry_price=1, stop_price=0.9, target_price=1.1, risk_reward=2,
                         confidence=0.8, reason="r", strategy_name="s")
        res = c.evaluate("BTC-USD", {"1h": [make_sig("1h", Direction.LONG, 0.8)]})
        out = c.boost_opportunity(opp, res)
        self.assertLess(out.confidence, 0.8)
        self.assertEqual(out.meta["confluence"], "conflict")

    def test_boost_agree(self):
        c = MultiTimeframeConfluence()
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                         entry_price=1, stop_price=0.9, target_price=1.1, risk_reward=2,
                         confidence=0.5, reason="r", strategy_name="s")
        res = c.evaluate("BTC-USD", {"1h": [make_sig("1h", Direction.LONG, 0.8)]})
        out = c.boost_opportunity(opp, res)
        self.assertGreater(out.confidence, 0.5)
        self.assertIn("confluence_agreement", out.meta)

    def test_boost_divergence(self):
        c = MultiTimeframeConfluence()
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                         entry_price=1, stop_price=0.9, target_price=1.1, risk_reward=2,
                         confidence=0.5, reason="r", strategy_name="s")
        res = c.evaluate("BTC-USD", {"1h": [make_sig("1h", Direction.LONG, 0.8)],
                                     "4h": [make_sig("4h", Direction.SHORT, 0.8)]})
        before = out_conf = 0.5
        out = c.boost_opportunity(opp, res)
        self.assertLess(out.confidence, 0.5 * 1.35)

    def test_build_signals(self):
        c = MultiTimeframeConfluence()
        opps = [Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                           entry_price=1, stop_price=0.9, target_price=1.1, risk_reward=2,
                           confidence=0.5, reason="r", strategy_name="s")]
        sigs = c.build_signals(opps, timeframe="1h")
        self.assertEqual(len(sigs), 1)
        self.assertEqual(sigs[0].timeframe, "1h")


class TestOrderBookImbalanceStrategy(unittest.TestCase):
    def setUp(self):
        self.strat = OrderBookImbalanceStrategy()
        self.assertEqual(self.strat.name(), "orderbook_imbalance")

    def test_compute_imbalance_empty(self):
        self.assertEqual(self.strat.compute_imbalance([], []), 0.0)

    def test_compute_imbalance(self):
        bids = [(100.0, 5.0)]
        asks = [(101.0, 3.0)]
        imb = self.strat.compute_imbalance(bids, asks)
        self.assertAlmostEqual(imb, (5.0 - 3.0) / 8.0)

    def test_evaluate_below_threshold(self):
        bids = [(100.0, 1.0)]
        asks = [(101.0, 1.0)]
        self.assertIsNone(self.strat.evaluate(bids, asks, 100.0, atr=2.0))

    def test_evaluate_long(self):
        bids = [(100.0, 10.0)]
        asks = [(101.0, 1.0)]
        res = self.strat.evaluate(bids, asks, 100.0, atr=2.0)
        self.assertEqual(res.direction, Direction.LONG)

    def test_evaluate_short(self):
        bids = [(100.0, 1.0)]
        asks = [(101.0, 10.0)]
        res = self.strat.evaluate(bids, asks, 100.0, atr=2.0)
        self.assertEqual(res.direction, Direction.SHORT)


if __name__ == "__main__":
    unittest.main()
