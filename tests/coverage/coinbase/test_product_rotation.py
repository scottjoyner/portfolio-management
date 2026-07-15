import unittest
from coinbase.src.product_rotation import (
    ProductRotator, MomentumRotationStrategy, ProductScore,
)
from coinbase.src.protocols import Direction, Bar, Opportunity


def make_bar(close, volume=1000.0):
    return Bar(timestamp=1.0, open=close, high=close + 1, low=close - 1,
               close=close, volume=volume)


class TestProductRotator(unittest.TestCase):
    def test_record_bar_cap(self):
        r = ProductRotator()
        for i in range(250):
            r.record_bar("BTC-USD", 100.0 + i, 1000.0)
        self.assertEqual(len(r._price_histories["BTC-USD"]), 200)

    def test_score_all_min_len(self):
        r = ProductRotator()
        for i in range(5):
            r.record_bar("BTC-USD", 100.0 + i, 1000.0 + i)
        self.assertEqual(r.score_all(), [])

    def test_score_all(self):
        r = ProductRotator()
        for i in range(30):
            r.record_bar("BTC-USD", 100.0 + i, 1000.0)
        scores = r.score_all()
        self.assertEqual(len(scores), 1)
        self.assertEqual(scores[0].product_id, "BTC-USD")
        self.assertEqual(scores[0].rank, 1)

    def test_rebalance_cooldown_and_force(self):
        r = ProductRotator(rebalance_cooldown_bars=24)
        for i in range(30):
            r.record_bar("BTC-USD", 100.0 + i, 1000.0)
        active1 = r.rebalance()
        self.assertIn("BTC-USD", active1)
        # cooldown not elapsed, active non-empty -> cached
        active2 = r.rebalance()
        self.assertEqual(active1, active2)
        # force rebalance
        r._bars_since_rebalance = 24
        active3 = r.rebalance()
        self.assertIn("BTC-USD", active3)

    def test_rebalance_empty_active_forces_score(self):
        r = ProductRotator(rebalance_cooldown_bars=24)
        for i in range(30):
            r.record_bar("BTC-USD", 100.0 + i, 1000.0)
        r._active_products = []
        active = r.rebalance()
        self.assertIn("BTC-USD", active)

    def test_ranked_products_property(self):
        r = ProductRotator()
        for i in range(30):
            r.record_bar("BTC-USD", 100.0 + i, 1000.0)
        rp = r.ranked_products
        self.assertIn("BTC-USD", rp)

    def test_top_opportunity_filter(self):
        r = ProductRotator()
        for i in range(30):
            r.record_bar("BTC-USD", 100.0 + i, 1000.0)
        r.rebalance()
        opps = [Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                            entry_price=1, stop_price=0.9, target_price=1.1, risk_reward=2,
                            confidence=0.5, reason="r", strategy_name="s"),
                Opportunity(product_id="ETH-USD", direction=Direction.LONG, instrument_type=None,
                            entry_price=1, stop_price=0.9, target_price=1.1, risk_reward=2,
                            confidence=0.5, reason="r", strategy_name="s")]
        filt = r.top_opportunity_filter(opps)
        self.assertEqual(len(filt), 1)

    def test_atr_helper(self):
        self.assertEqual(MomentumRotationStrategy._estimate_atr([1, 2], [2, 3], [1, 2]), 0.0)


class TestMomentumRotationStrategy(unittest.TestCase):
    def setUp(self):
        self.strat = MomentumRotationStrategy()
        self.strat.set_product_id("BTC-USD")
        self.assertEqual(self.strat.name(), "momentum_rotation")

    def test_no_pid(self):
        s = MomentumRotationStrategy()
        self.assertIsNone(s.on_bar(make_bar(100), []))

    def test_too_few_bars_for_atr(self):
        bars = [make_bar(100.0 + i) for i in range(10)]
        self.assertIsNone(self.strat.on_bar(bars[-1], bars[:-1]))

    def test_score_not_found(self):
        bars = [make_bar(100.0 + i) for i in range(40)]
        self.assertIsNone(self.strat.on_bar(bars[-1], bars[:-1]))

    def test_long_signal(self):
        for i in range(40):
            self.strat.rotator.record_bar("BTC-USD", 100.0 + i, 1000.0)
        bars = [make_bar(100.0 + i) for i in range(40)]
        res = self.strat.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(res)
        self.assertEqual(res.direction, Direction.LONG)

    def test_short_signal(self):
        strat = MomentumRotationStrategy()
        strat.set_product_id("ETH-USD")
        for i in range(40):
            strat.rotator.record_bar("ETH-USD", 200.0 - 5 * i, 1000.0)
        bars = [make_bar(200.0 - 5 * i) for i in range(40)]
        res = strat.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(res)
        self.assertEqual(res.direction, Direction.SHORT)

    def test_neutral_no_signal(self):
        strat = MomentumRotationStrategy()
        strat.set_product_id("SOL-USD")
        for i in range(40):
            strat.rotator.record_bar("SOL-USD", 100.0, 1000.0)
        bars = [make_bar(100.0) for i in range(40)]
        self.assertIsNone(strat.on_bar(bars[-1], bars[:-1]))

    def test_rank_exceeds_topn(self):
        strat = MomentumRotationStrategy(ProductRotator(top_n=1))
        strat.set_product_id("SOL-USD")
        # give SOL low composite by ranking another product higher
        for i in range(40):
            strat.rotator.record_bar("BTC-USD", 100.0 + i, 1000.0)
            strat.rotator.record_bar("SOL-USD", 100.0, 1000.0)
        strat.rotator.rebalance()
        bars = [make_bar(100.0) for i in range(40)]
        self.assertIsNone(strat.on_bar(bars[-1], bars[:-1]))

    def test_vol_confirm_long(self):
        for i in range(40):
            self.strat.rotator.record_bar("BTC-USD", 100.0 + i,
                                          1000.0 if i < 39 else 5000.0)
        bars = [make_bar(100.0 + i, volume=1000.0 if i < 39 else 5000.0)
                for i in range(40)]
        res = self.strat.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(res)
        self.assertEqual(res.direction, Direction.LONG)
        # last volume (5000) >> prior avg (1000) -> vol_confirm boosts confidence
        # (base would be 0.3; confirmed path lifts it to 0.4)
        self.assertGreater(res.confidence, 0.3)

    def test_on_bar_zero_atr(self):
        # high==low==close => ATR 0 => on_bar returns None
        bars = [Bar(timestamp=float(i), open=100.0, high=100.0, low=100.0,
                    close=100.0, volume=1000.0) for i in range(40)]
        for b in bars[:-1]:
            self.strat.rotator.record_bar("BTC-USD", b.close, b.volume)
        self.assertIsNone(self.strat.on_bar(bars[-1], bars[:-1]))

    def test_atr_helper(self):
        self.assertEqual(MomentumRotationStrategy._estimate_atr([1, 2], [2, 3], [1, 2]), 0.0)


if __name__ == "__main__":
    unittest.main()
