"""Tests for coinbase/src/fill_model.py"""
import unittest
from unittest import mock

from coinbase.src import fill_model as fm


class FakeDirection:
    def __init__(self, v):
        self.value = v


class TestFillModel(unittest.TestCase):
    def setUp(self):
        self.m = fm.FillModel(seed=42)

    def test_estimate_buy(self):
        est = self.m.estimate("BTC-USD", "BUY", 1.0, 100.0, 100_000_000)
        self.assertGreater(est.entry_price, 100.0)
        self.assertLess(est.exit_price, 100.0)

    def test_estimate_sell(self):
        est = self.m.estimate("BTC-USD", "SELL", 1.0, 100.0, 100_000_000)
        self.assertLess(est.entry_price, 100.0)
        self.assertGreater(est.exit_price, 100.0)

    def test_estimate_empty_side(self):
        est = self.m.estimate("X", "", 1.0, 100.0, 100_000_000)
        # empty side falls through to SELL branch
        self.assertLess(est.entry_price, 100.0)

    def test_volume_tiers_fills(self):
        for v in (200_000_000, 20_000_000, 5_000_000, 500_000, 50_000, 0):
            est = self.m.estimate("X", "BUY", 1.0, 100.0, v)
            self.assertGreater(est.fill_seconds, 0)

    def test_partial_fill_tiers(self):
        micro = self.m.estimate("X", "BUY", 1.0, 100.0, 10_000)
        small = self.m.estimate("X", "BUY", 1.0, 100.0, 500_000)
        large = self.m.estimate("X", "BUY", 1.0, 100.0, 5_000_000)
        self.assertGreaterEqual(micro.partial_fill_pct, 0.7)
        self.assertLessEqual(micro.partial_fill_pct, 1.0)
        self.assertGreaterEqual(small.partial_fill_pct, 0.85)
        self.assertGreaterEqual(large.partial_fill_pct, 0.95)
        self.assertLessEqual(large.partial_fill_pct, 1.0)

    def test_size_impact_zero_volume(self):
        est = self.m.estimate("X", "BUY", 1.0, 100.0, 0.0)
        self.assertGreaterEqual(est.entry_slippage_bps, 0)

    def test_size_impact_large_notional(self):
        est = self.m.estimate("X", "BUY", 1_000_000.0, 100.0, 10_000_000)
        self.assertGreater(est.entry_slippage_bps, 0)

    def test_is_maker(self):
        self.m._rng.random = lambda: 0.1
        self.assertTrue(self.m.is_maker(0.5))
        self.m._rng.random = lambda: 0.9
        self.assertFalse(self.m.is_maker(0.5))

    def test_fill_buy_direction(self):
        r = self.m.fill(FakeDirection("BUY"), 100.0, 1.0, 99.0, 101.0, 100_000_000, "BTC-USD")
        self.assertGreater(r.price, 0)
        self.assertEqual(r.size, 1.0)
        self.assertGreater(r.fees, 0)

    def test_fill_sell_direction(self):
        r = self.m.fill(FakeDirection("SHORT"), 100.0, 1.0, 99.0, 101.0, 100_000_000, "BTC-USD")
        self.assertLess(r.price, 100.0)

    def test_volume_tier_fallback(self):
        self.assertEqual(self.m._volume_tier(-1), (4.0, 8.0))

    def test_volume_tier_match(self):
        self.assertEqual(self.m._volume_tier(60_000_000), (0.3, 0.5))


class TestAdaptiveFillModel(unittest.TestCase):
    def setUp(self):
        self.m = fm.AdaptiveFillModel(seed=7)

    def test_observe_empty_product(self):
        self.m.observe_fill("", 5.0)  # returns early
        self.assertEqual(self.m._observed, {})

    def test_observe_first(self):
        self.m.observe_fill("BTC-USD", 5.0)
        self.assertEqual(self.m._observed["BTC-USD"], 5.0)

    def test_observe_ema(self):
        self.m.observe_fill("BTC-USD", 5.0)
        self.m.observe_fill("BTC-USD", 15.0)
        self.assertAlmostEqual(self.m._observed["BTC-USD"], 0.7 * 5.0 + 0.3 * 15.0)

    def test_estimate_blend(self):
        self.m.observe_fill("BTC-USD", 2.0)
        est = self.m.estimate("BTC-USD", "BUY", 1.0, 100.0, 100_000_000)
        self.assertGreater(est.entry_slippage_bps, 0)

    def test_estimate_no_observation(self):
        est = self.m.estimate("ETH-USD", "SELL", 1.0, 100.0, 100_000_000)
        self.assertGreater(est.entry_slippage_bps, 0)

    def test_estimate_sell_blend(self):
        self.m.observe_fill("BTC-USD", 3.0)
        est = self.m.estimate("BTC-USD", "SELL", 1.0, 100.0, 100_000_000)
        self.assertGreater(est.entry_slippage_bps, 0)


if __name__ == "__main__":
    unittest.main()
