"""Offline unit tests for coinbase.src.fill_model (fill simulation + adaptive model)."""

from __future__ import annotations

import unittest

from coinbase.src.fill_model import FillModel, AdaptiveFillModel, FillEstimate, SLIPPAGE_TABLE


class TestFillModel(unittest.TestCase):
    def setUp(self):
        self.fm = FillModel(seed=1)

    def test_volume_tier_boundaries(self):
        # (threshold, min, max) tuples; pick representative volumes.
        self.assertEqual(self.fm._volume_tier(60_000_000), SLIPPAGE_TABLE[0][1:])
        self.assertEqual(self.fm._volume_tier(20_000_000), SLIPPAGE_TABLE[1][1:])
        self.assertEqual(self.fm._volume_tier(5_000_000), SLIPPAGE_TABLE[2][1:])
        self.assertEqual(self.fm._volume_tier(500_000), SLIPPAGE_TABLE[3][1:])
        self.assertEqual(self.fm._volume_tier(1_000), SLIPPAGE_TABLE[4][1:])

    def test_estimate_buy_above_price(self):
        est = self.fm.estimate("BTC-USD", "BUY", 0.001, 60000.0, 60_000_000)
        self.assertIsInstance(est, FillEstimate)
        self.assertGreater(est.entry_price, 60000.0)          # buys at ask
        self.assertGreaterEqual(est.entry_slippage_bps, 0.0)
        self.assertLessEqual(est.fill_seconds, 10.0)
        self.assertLessEqual(est.partial_fill_pct, 1.0)

    def test_estimate_sell_below_price(self):
        est = self.fm.estimate("BTC-USD", "SELL", 0.001, 60000.0, 60_000_000)
        self.assertLess(est.entry_price, 60000.0)             # sells at bid

    def test_estimate_micro_cap_wider(self):
        est = self.fm.estimate("ZZZ-USD", "BUY", 0.001, 1.0, 1_000)
        # micro tier base slippage (4.0) is wider than mega-cap (0.3); entry is
        # half-spread scaled by 0.8-1.2, so compare against a mega-cap estimate.
        mega = self.fm.estimate("BTC-USD", "BUY", 0.001, 60000.0, 60_000_000)
        self.assertGreater(est.entry_slippage_bps, mega.entry_slippage_bps)
        self.assertGreater(est.fill_seconds, 2.0)
        self.assertLess(est.partial_fill_pct, 1.0)

    def test_estimate_zero_volume(self):
        # volume_24h == 0 -> size_impact stays 1.0 (no participation term)
        est = self.fm.estimate("ZZZ-USD", "BUY", 0.001, 1.0, 0)
        self.assertGreater(est.entry_price, 1.0)

    def test_estimate_volume_tiers_fill_delay(self):
        for vol in (20_000_000, 5_000_000, 500_000):
            est = self.fm.estimate("X-USD", "BUY", 0.001, 10.0, vol)
            self.assertLessEqual(est.fill_seconds, 10.0)
            self.assertLessEqual(est.partial_fill_pct, 1.0)

    def test_estimate_small_cap_partial(self):
        # volume between 100k and 1M -> partial_fill in [0.85, 1.0)
        est = self.fm.estimate("X-USD", "BUY", 0.001, 10.0, 500_000)
        self.assertLess(est.partial_fill_pct, 1.0)
        self.assertGreaterEqual(est.partial_fill_pct, 0.85)

    def test_fill_string_direction(self):
        res = self.fm.fill("BUY", 100.0, 0.5, 99.0, 101.0, 60_000_000, "BTC-USD")
        self.assertEqual(res.size, 0.5)
        self.assertGreater(res.price, 100.0)
        self.assertGreater(res.fees, 0.0)

    def test_fill_long_direction_object(self):
        class Dir:
            value = "LONG"
        res = self.fm.fill(Dir(), 100.0, 0.5, 99.0, 101.0, 60_000_000, "BTC-USD")
        self.assertGreater(res.price, 100.0)

    def test_fill_sell_direction(self):
        res = self.fm.fill("SELL", 100.0, 0.5, 99.0, 101.0, 60_000_000, "BTC-USD")
        self.assertLess(res.price, 100.0)

    def test_is_maker_bounds(self):
        self.assertTrue(self.fm.is_maker(1.0))   # always maker
        self.assertFalse(self.fm.is_maker(0.0))  # never maker
        self.assertIsInstance(self.fm.is_maker(0.5), bool)


class TestAdaptiveFillModel(unittest.TestCase):
    def test_no_observation_matches_base(self):
        base = FillModel(seed=2)
        ada = AdaptiveFillModel(seed=2)
        e1 = base.estimate("BTC-USD", "BUY", 0.001, 60000.0, 60_000_000)
        e2 = ada.estimate("BTC-USD", "BUY", 0.001, 60000.0, 60_000_000)
        self.assertAlmostEqual(e1.entry_slippage_bps, e2.entry_slippage_bps)

    def test_observation_blends_slippage(self):
        ada = AdaptiveFillModel(seed=2)
        ada.observe_fill("BTC-USD", 5.0)   # realized slippage 5 bps
        est = ada.estimate("BTC-USD", "BUY", 0.001, 60000.0, 60_000_000)
        # Blended = (model_slippage + 5.0) / 2, strictly greater than base model.
        base = FillModel(seed=2).estimate("BTC-USD", "BUY", 0.001, 60000.0, 60_000_000)
        self.assertGreater(est.entry_slippage_bps, base.entry_slippage_bps)
        self.assertAlmostEqual(est.entry_slippage_bps, (base.entry_slippage_bps + 5.0) / 2.0,
                                places=4)

    def test_observe_empty_product_is_noop(self):
        ada = AdaptiveFillModel(seed=2)
        ada.observe_fill("", 5.0)
        est = ada.estimate("BTC-USD", "BUY", 0.001, 60000.0, 60_000_000)
        base = FillModel(seed=2).estimate("BTC-USD", "BUY", 0.001, 60000.0, 60_000_000)
        self.assertAlmostEqual(est.entry_slippage_bps, base.entry_slippage_bps)


if __name__ == "__main__":
    unittest.main(verbosity=2)
