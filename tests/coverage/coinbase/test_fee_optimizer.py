"""Tests for coinbase/src/fee_optimizer.py"""
import time
import unittest
from unittest import mock

from coinbase.src import fee_optimizer as fo


class FakeRustFeeTracker:
    def __init__(self, initial_volume_30d=0.0):
        self._vol = initial_volume_30d
        self._trades = []

    def rolling_30d_volume(self):
        return self._vol + sum(v for _, v in self._trades)

    def current_tier_min_volume(self):
        return self._current().min_volume

    def current_tier_maker_rate(self):
        return self._current().maker_rate

    def current_tier_taker_rate(self):
        return self._current().taker_rate

    def _current(self):
        t = fo.COINBASE_FEE_TIERS[0]
        for tier in reversed(fo.COINBASE_FEE_TIERS):
            if self.rolling_30d_volume() >= tier.min_volume:
                return tier
        return t

    def next_tier_min_volume(self):
        cur = self._current()
        for tier in fo.COINBASE_FEE_TIERS:
            if tier.min_volume > cur.min_volume:
                return tier.min_volume
        return None

    def volume_to_next_tier(self):
        nv = self.next_tier_min_volume()
        if nv is None:
            return 0.0
        return max(0.0, nv - self.rolling_30d_volume())

    def record_trade(self, volume_usd, timestamp=None):
        self._trades.append((timestamp or time.time(), volume_usd))

    def fee_cost(self, trade_volume, is_maker):
        t = self._current()
        rate = t.maker_rate if is_maker else t.taker_rate
        return trade_volume * rate

    def maker_rate(self):
        return self._current().maker_rate

    def taker_rate(self):
        return self._current().taker_rate

    def savings_to_next_tier(self, projected_monthly_volume):
        cur = self._current()
        nxt = None
        for tier in fo.COINBASE_FEE_TIERS:
            if tier.min_volume > cur.min_volume:
                nxt = tier
                break
        if not nxt:
            return 0.0
        return ((cur.maker_rate - nxt.maker_rate) * projected_monthly_volume * 0.5
                + (cur.taker_rate - nxt.taker_rate) * projected_monthly_volume * 0.5)

    def to_state(self):
        return self._vol, list(self._trades)

    @classmethod
    def from_state(cls, initial_vol, trades):
        obj = cls(initial_vol)
        obj._trades = list(trades)
        return obj


def set_mode(rust_enabled):
    if rust_enabled:
        fo._HAS_RUST_FEE = True
        fo._RustFeeTracker = FakeRustFeeTracker
    else:
        fo._HAS_RUST_FEE = False
        fo._RustFeeTracker = None


def make_tracker(rust_enabled, vol=0.0):
    set_mode(rust_enabled)
    return fo.FeeTracker(initial_volume_30d=vol)


class TestFeeTier(unittest.TestCase):
    def test_tiers(self):
        self.assertEqual(fo.COINBASE_FEE_TIERS[0].maker_rate, 0.0060)
        self.assertEqual(fo.COINBASE_FEE_TIERS[-1].min_volume, 20_000_000)


def _exercise(t, rust_enabled):
    set_mode(rust_enabled)
    _ = t.rolling_30d_volume
    _ = t.get_current_tier()
    _ = t.get_next_tier()
    _ = t.volume_to_next_tier()
    t.record_trade(1000.0)
    _ = t.rolling_30d_volume
    _ = t.fee_cost(1000.0, True)
    _ = t.fee_cost(1000.0, False)
    _ = t.maker_rate()
    _ = t.taker_rate()
    _ = t.savings_to_next_tier(10000.0)
    st = t.to_state()
    _ = st
    _ = fo.FeeTracker.from_state(st)


class TestFeeTracker(unittest.TestCase):
    def test_both_modes(self):
        for rust_enabled in (True, False):
            t = make_tracker(rust_enabled, vol=0.0)
            _exercise(t, rust_enabled)

    def test_high_volume_top_tier(self):
        for rust_enabled in (True, False):
            t = make_tracker(rust_enabled, vol=50_000_000)
            self.assertEqual(t.get_current_tier().min_volume, 20_000_000)
            self.assertIsNone(t.get_next_tier())
            self.assertEqual(t.volume_to_next_tier(), 0.0)
            self.assertEqual(t.savings_to_next_tier(10000.0), 0.0)
            t.record_trade(1000.0)
            self.assertGreater(t.rolling_30d_volume, 50_000_000)

    def test_from_state_none(self):
        for rust_enabled in (True, False):
            t = fo.FeeTracker.from_state(None)
            self.assertEqual(t.rolling_30d_volume, 0.0)

    def test_tier_display(self):
        for rust_enabled in (True, False):
            t = make_tracker(rust_enabled, vol=0.0)
            disp = t.tier_display()
            self.assertIn("maker", disp)
            # advance to top tier: no next
            t2 = make_tracker(rust_enabled, vol=50_000_000)
            disp2 = t2.tier_display()
            self.assertNotIn("to next", disp2)

    def test_prune_path(self):
        set_mode(False)
        t = fo.FeeTracker(initial_volume_30d=0.0)
        old = time.time() - 40 * 86400
        t._trades_30d = [(old, 100.0), (time.time(), 50.0)]
        t._prune()
        self.assertEqual(len(t._trades_30d), 1)

    def test_record_trade_default_ts(self):
        set_mode(False)
        t = fo.FeeTracker()
        t.record_trade(500.0)  # timestamp None
        self.assertEqual(len(t._trades_30d), 1)


class TestFeeAwareSizer(unittest.TestCase):
    def test_init_default(self):
        s = fo.FeeAwareSizer()
        self.assertIsInstance(s.fee_tracker, fo.FeeTracker)

    def test_effective_expected_return(self):
        s = fo.FeeAwareSizer(make_tracker(False, 50_000_000))
        r = s.effective_expected_return(0.01, 100.0, is_maker=False)
        self.assertAlmostEqual(r, 0.01 - s.fee_tracker.taker_rate())
        r2 = s.effective_expected_return(0.01, 100.0, is_maker=True)
        self.assertAlmostEqual(r2, 0.01 - s.fee_tracker.maker_rate())

    def test_volume_boost_no_need(self):
        s = fo.FeeAwareSizer(make_tracker(False, 50_000_000))
        self.assertEqual(s.volume_boost(100.0), 1.0)

    def test_volume_boost_partial(self):
        s = fo.FeeAwareSizer(make_tracker(False, 0.0))
        b = s.volume_boost(10.0)
        self.assertGreaterEqual(b, 1.0)
        self.assertLessEqual(b, 1.4)

    def test_size_with_fee_boost(self):
        s = fo.FeeAwareSizer(make_tracker(False, 0.0))
        from coinbase.src.protocols import Direction, Opportunity
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG,
                          instrument_type=None, entry_price=100, stop_price=90,
                          target_price=110, risk_reward=2, confidence=0.5,
                          reason="r", strategy_name="s", base_size=1.0)
        out = s.size_with_fee_boost(opp)
        self.assertGreater(out.base_size, 1.0)
        self.assertIn("fee_volume_boost", out.meta)

    def test_should_generate_volume(self):
        s = fo.FeeAwareSizer(make_tracker(False, 0.0))
        gen, sav = s.should_generate_volume(min_savings=50.0)
        self.assertIsInstance(gen, bool)
        # at top tier => no need
        s2 = fo.FeeAwareSizer(make_tracker(False, 50_000_000))
        gen2, sav2 = s2.should_generate_volume()
        self.assertFalse(gen2)
        self.assertEqual(sav2, 0.0)


class TestVolumeGenerator(unittest.TestCase):
    def test_init(self):
        vg = fo.VolumeGenerator(make_tracker(False, 0.0))
        self.assertEqual(vg.max_volume_per_day, 10000.0)

    def test_no_need_top_tier(self):
        vg = fo.VolumeGenerator(make_tracker(False, 50_000_000))
        self.assertIsNone(vg.generate_volume_opportunities("BTC-USD", 100, 1.0))

    def test_daily_cap(self):
        vg = fo.VolumeGenerator(make_tracker(False, 0.0), max_volume_per_day=500.0)
        vg._daily_volume = 500.0
        self.assertIsNone(vg.generate_volume_opportunities("BTC-USD", 100, 1.0))

    def test_no_next_tier(self):
        vg = fo.VolumeGenerator(make_tracker(False, 50_000_000))
        self.assertIsNone(vg.generate_volume_opportunities("BTC-USD", 100, 1.0))

    def test_spread_savings_le_zero(self):
        # tier == next_tier maker rate -> spread_savings <= 0 path
        vg = fo.VolumeGenerator(make_tracker(False, 0.0))
        # patch next tier equal to current
        vg.fee_tracker.get_next_tier = lambda: vg.fee_tracker.get_current_tier()
        self.assertIsNone(vg.generate_volume_opportunities("BTC-USD", 100, 1.0))

    def test_generates(self):
        vg = fo.VolumeGenerator(make_tracker(False, 0.0))
        opp = vg.generate_volume_opportunities("BTC-USD", 100.0, 1.0)
        self.assertIsNotNone(opp)
        self.assertGreater(opp.quote_size, 0)
        self.assertEqual(opp.strategy_name, "volume_generator")

    def test_daily_check_reset(self):
        vg = fo.VolumeGenerator(make_tracker(False, 0.0))
        vg._daily_volume = 5.0
        vg._daily_reset_ts = time.time() - 90000
        vg._daily_check()
        self.assertEqual(vg._daily_volume, 0.0)

    def test_record_generated(self):
        vg = fo.VolumeGenerator(make_tracker(False, 0.0))
        before = vg.fee_tracker.rolling_30d_volume
        vg.record_generated(100.0)
        self.assertEqual(vg._daily_volume, 100.0)
        self.assertGreater(vg.fee_tracker.rolling_30d_volume, before)


if __name__ == "__main__":
    unittest.main()
