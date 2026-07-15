"""Tests for coinbase/src/liquidity_sizer.py"""
import unittest
from unittest import mock

from coinbase.src import liquidity_sizer as ls
from coinbase.src.protocols import Direction, Opportunity


def make_opp(product="BTC-USD", base=10.0, entry=100.0, rr=2.0, conf=0.5):
    return Opportunity(product_id=product, direction=Direction.LONG,
                       instrument_type=None, entry_price=entry, stop_price=entry * 0.9,
                       target_price=entry * 1.1, risk_reward=rr, confidence=conf,
                       reason="r", strategy_name="s", base_size=base,
                       quote_size=base * entry)


class TestLiquidityProfile(unittest.TestCase):
    def test_mid_empty(self):
        p = ls.LiquidityProfile()
        self.assertEqual(p.mid_price, 0.0)

    def test_mid(self):
        p = ls.LiquidityProfile(bid_depth=[(99.0, 1)], ask_depth=[(101.0, 1)])
        self.assertEqual(p.mid_price, 100.0)


class TestMarketImpactModel(unittest.TestCase):
    def test_zero_volume(self):
        m = ls.MarketImpactModel()
        self.assertEqual(m.estimate_impact_bps(100.0, 0.0, 100.0), 10.0)

    def test_zero_price(self):
        m = ls.MarketImpactModel()
        self.assertEqual(m.estimate_impact_bps(100.0, 100.0, 0.0), 10.0)

    def test_normal(self):
        m = ls.MarketImpactModel()
        bps = m.estimate_impact_bps(1000.0, 100_000_000, 100.0)
        self.assertGreaterEqual(bps, 0)
        self.assertLessEqual(bps, 50.0)

    def test_optimal_zero_volume(self):
        m = ls.MarketImpactModel()
        self.assertEqual(m.optimal_participation(1000.0, 0.0, 100.0), 0.01)

    def test_optimal_normal(self):
        m = ls.MarketImpactModel()
        p = m.optimal_participation(1000.0, 100_000_000, 100.0)
        self.assertGreaterEqual(p, 0.001)
        self.assertLessEqual(p, 0.3)


class TestOrderBookDepthEstimator(unittest.TestCase):
    def setUp(self):
        self.e = ls.OrderBookDepthEstimator()

    def test_estimate_depth(self):
        p = self.e.estimate_depth("BTC-USD", 100.0, 200_000_000, 1.0)
        self.assertEqual(p.product_id, "BTC-USD")
        self.assertGreater(len(p.bid_depth), 0)

    def test_cache(self):
        p1 = self.e.estimate_depth("BTC-USD", 100.0, 200_000_000, 1.0)
        p2 = self.e.estimate_depth("BTC-USD", 100.0, 200_000_000, 1.0)
        self.assertIs(p1, p2)

    def test_set_price_provider(self):
        self.e.set_price_provider(lambda pid: (1.0, 2.0, 3.0))
        self.assertIsNotNone(self.e._price_provider)

    def test_tier_branches(self):
        # daily_notional = volume_24h * price(100); distinct product ids avoid cache hits
        for pid, v, expect in [("A", 200_000, "deep"), ("B", 5_000, "moderate"),
                               ("C", 500, "thin"), ("D", 50, "illiquid")]:
            p = self.e.estimate_depth(pid, 100.0, v, 1.0)
            self.assertEqual(p.liquidity_score, ls.LIQUIDITY_TIERS[expect])

    def test_cache_clear(self):
        e = ls.OrderBookDepthEstimator()
        for i in range(120):
            e.estimate_depth(f"P{i}", 100.0, 200_000_000, 1.0)
        # cache should have been cleared at >100
        self.assertLessEqual(len(e._depth_cache), 100)

    def test_max_liquid_size_buy(self):
        s = self.e.max_liquid_size("BTC-USD", "buy", 100.0, 200_000_000, 1.0)
        self.assertGreater(s, 0)

    def test_max_liquid_size_sell(self):
        s = self.e.max_liquid_size("BTC-USD", "sell", 100.0, 200_000_000, 1.0)
        self.assertGreater(s, 0)

    def test_max_liquid_size_impact_scale(self):
        # large notional triggers impact scaling branch
        s = self.e.max_liquid_size("BTC-USD", "buy", 100.0, 1_000.0, 0.1)
        self.assertGreaterEqual(s, 0)


class TestLiquidityAwareSizer(unittest.TestCase):
    def setUp(self):
        self.s = ls.LiquidityAwareSizer()

    def test_init_defaults(self):
        self.assertIsInstance(self.s.depth_estimator, ls.OrderBookDepthEstimator)
        self.assertIsInstance(self.s.impact_model, ls.MarketImpactModel)

    def test_set_volume(self):
        self.s.set_volume_24h("BTC-USD", 5_000_000)
        self.assertEqual(self.s._volume_24h_cache["BTC-USD"], 5_000_000)

    def test_max_size_default_vol(self):
        s = self.s.max_size_for_liquidity("BTC-USD", "buy", 100.0, 1.0)
        self.assertGreaterEqual(s, 0)

    def test_size_with_liquidity(self):
        opp = make_opp()
        self.s.set_volume_24h("BTC-USD", 200_000_000)
        out = self.s.size_with_liquidity(opp, 1.0)
        self.assertIn("liquidity_score", out.meta)
        self.assertIn("estimated_impact_bps", out.meta)

    def test_size_with_liquidity_impact_estimate(self):
        opp = make_opp()
        self.s.set_volume_24h("BTC-USD", 10.0)  # tiny volume -> higher impact
        out = self.s.size_with_liquidity(opp, 50.0)
        self.assertIn("estimated_impact_bps", out.meta)
        self.assertGreaterEqual(out.meta["estimated_impact_bps"], 0.0)

    def test_size_with_liquidity_zero_max(self):
        opp = make_opp()
        self.s.set_volume_24h("BTC-USD", 1.0)
        out = self.s.size_with_liquidity(opp, 100.0)
        # base_size should never exceed the requested size
        self.assertLessEqual(out.base_size, opp.base_size)

    def test_size_batch(self):
        opps = [make_opp(), make_opp(product="ETH-USD")]
        self.s.set_volume_24h("BTC-USD", 200_000_000)
        self.s.set_volume_24h("ETH-USD", 200_000_000)
        out = self.s.size_batch(opps, 1.0)
        self.assertEqual(len(out), 2)

    def test_liquidation_size(self):
        self.s.set_volume_24h("BTC-USD", 200_000_000)
        s = self.s.liquidation_size("BTC-USD", 100.0, 1.0)
        self.assertGreaterEqual(s, 0)


if __name__ == "__main__":
    unittest.main()
