"""Tests for coinbase/src/regime.py"""
import unittest
from unittest import mock

from coinbase.src import regime as rg


def set_rust(enabled):
    rg._HAS_RUST_REGIME = enabled


class TestRegimeEnum(unittest.TestCase):
    def test_values(self):
        self.assertEqual(rg.Regime.STRONG_UPTREND.value, "strong_uptrend")
        self.assertEqual(rg.Regime.UNKNOWN.value, "unknown")


class TestRegimeFeatures(unittest.TestCase):
    def test_is_trending(self):
        f = rg.RegimeFeatures(adx=30, trend_strength=0.05)
        self.assertTrue(f.is_trending)
        f2 = rg.RegimeFeatures(adx=20, trend_strength=0.05)
        self.assertFalse(f2.is_trending)

    def test_is_volatile(self):
        self.assertTrue(rg.RegimeFeatures(volatility=0.05).is_volatile)
        self.assertFalse(rg.RegimeFeatures(volatility=0.01).is_volatile)

    def test_is_ranging(self):
        self.assertTrue(rg.RegimeFeatures(adx=10, volatility=0.01).is_ranging)
        self.assertFalse(rg.RegimeFeatures(adx=30, volatility=0.05).is_ranging)


class TestClassify(unittest.TestCase):
    def setUp(self):
        self.d = rg.RegimeDetector()

    def _f(self, **kw):
        return rg.RegimeFeatures(**kw)

    def test_strong_uptrend(self):
        self.assertEqual(self.d._classify(self._f(volatility=0.05, adx=30, trend_strength=0.05)),
                         rg.Regime.STRONG_UPTREND)

    def test_strong_downtrend(self):
        self.assertEqual(self.d._classify(self._f(volatility=0.05, adx=30, trend_strength=-0.05)),
                         rg.Regime.STRONG_DOWNTREND)

    def test_weak_uptrend(self):
        self.assertEqual(self.d._classify(self._f(adx=30, trend_strength=0.05)),
                         rg.Regime.WEAK_UPTREND)

    def test_weak_downtrend(self):
        self.assertEqual(self.d._classify(self._f(adx=30, trend_strength=-0.05)),
                         rg.Regime.WEAK_DOWNTREND)

    def test_high_vol_hurst_up(self):
        self.assertEqual(self.d._classify(self._f(volatility=0.05, hurst_exponent=0.7, trend_strength=0.02)),
                         rg.Regime.STRONG_UPTREND)

    def test_high_vol_hurst_down(self):
        self.assertEqual(self.d._classify(self._f(volatility=0.05, hurst_exponent=0.7, trend_strength=-0.02)),
                         rg.Regime.STRONG_DOWNTREND)

    def test_high_volatility(self):
        self.assertEqual(self.d._classify(self._f(volatility=0.05)), rg.Regime.HIGH_VOLATILITY)

    def test_ranging(self):
        self.assertEqual(self.d._classify(self._f(adx=10, volatility=0.01)), rg.Regime.RANGING)

    def test_low_volatility(self):
        self.assertEqual(self.d._classify(self._f(adx=25, volatility=0.005)), rg.Regime.LOW_VOLATILITY)

    def test_unknown(self):
        self.assertEqual(self.d._classify(self._f(adx=25, volatility=0.02)), rg.Regime.UNKNOWN)


class TestFeatureHelpers(unittest.TestCase):
    def setUp(self):
        self.d = rg.RegimeDetector()

    def test_volatility_short(self):
        self.assertEqual(self.d._compute_volatility([1.0]), 0.0)

    def test_volatility_zero_var(self):
        self.assertEqual(self.d._compute_volatility([1.0, 1.0, 1.0]), 0.0)

    def test_volatility_normal(self):
        v = self.d._compute_volatility([1.0, 1.01, 0.99, 1.02])
        self.assertGreater(v, 0)

    def test_trend_strength_short(self):
        self.assertEqual(self.d._compute_trend_strength([1, 2]), 0.0)

    def test_trend_strength_normal(self):
        prices = list(range(50, 100))
        self.assertGreater(self.d._compute_trend_strength(prices), 0)

    def test_price_position_short(self):
        self.assertEqual(self.d._compute_price_position([1.0]), 0.5)

    def test_price_position_flat(self):
        self.assertEqual(self.d._compute_price_position([2.0, 2.0, 2.0]), 0.5)

    def test_price_position_normal(self):
        pp = self.d._compute_price_position([1.0, 2.0, 3.0, 4.0])
        self.assertAlmostEqual(pp, 1.0)

    def test_adx_short(self):
        self.assertEqual(self.d._compute_adx([1.0], [1.0], [1.0]), 25.0)

    def test_adx_no_hl(self):
        self.assertEqual(self.d._compute_adx([1.0, 2.0], None, None), 25.0)

    def test_adx_normal(self):
        closes = [10, 11, 12, 13, 14, 13, 12, 13, 14, 15, 16, 15, 14, 15, 16, 17]
        highs = [c + 0.5 for c in closes]
        lows = [c - 0.5 for c in closes]
        adx = self.d._compute_adx(closes, highs, lows)
        self.assertGreaterEqual(adx, 0.0)
        self.assertLessEqual(adx, 100.0)

    def test_adx_zero_tr(self):
        closes = [10.0] * 5
        highs = [10.0] * 5
        lows = [10.0] * 5
        self.assertEqual(self.d._compute_adx(closes, highs, lows), 25.0)

    def test_skewness_short(self):
        self.assertEqual(self.d._compute_skewness([1.0, 2.0]), 0.0)

    def test_skewness_zero_var(self):
        self.assertEqual(self.d._compute_skewness([1.0, 1.0, 1.0]), 0.0)

    def test_skewness_normal(self):
        s = self.d._compute_skewness([1.0, 1.0, 1.0, 1.0, 10.0])
        self.assertGreater(s, 0)

    def test_kurtosis_short(self):
        self.assertEqual(self.d._compute_kurtosis([1.0, 2.0, 3.0]), 3.0)

    def test_kurtosis_zero_var(self):
        self.assertEqual(self.d._compute_kurtosis([1.0, 1.0, 1.0, 1.0]), 3.0)

    def test_kurtosis_normal(self):
        k = self.d._compute_kurtosis([1.0, 2.0, 1.0, 2.0, 1.0, 2.0])
        self.assertGreater(k, 0)

    def test_hurst_short(self):
        self.assertEqual(self.d._hurst_exponent([1, 2, 3]), 0.5)

    def test_hurst_normal(self):
        prices = [100 + i + (i % 3) for i in range(120)]
        h = self.d._hurst_exponent(prices)
        self.assertGreaterEqual(h, 0.0)
        self.assertLessEqual(h, 1.0)

    def test_serial_corr_short(self):
        self.assertEqual(self.d._serial_correlation([1.0, 2.0]), 0.0)

    def test_serial_corr_zero_den(self):
        self.assertEqual(self.d._serial_correlation([1.0, 1.0, 1.0, 1.0]), 0.0)

    def test_serial_corr_normal(self):
        c = self.d._serial_correlation([1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertGreaterEqual(c, -1.0)


class TestDetect(unittest.TestCase):
    def _series(self, n, base=100.0):
        return [base + i for i in range(n)]

    def test_detect_rust(self):
        set_rust(True)
        d = rg.RegimeDetector()
        reg, feats = d.detect(self._series(50))
        self.assertIsInstance(reg, rg.Regime)
        self.assertIsInstance(feats, rg.RegimeFeatures)

    def test_detect_no_rust_short(self):
        set_rust(False)
        d = rg.RegimeDetector()
        reg, feats = d.detect(self._series(10))
        self.assertIsInstance(reg, rg.Regime)
        set_rust(True)

    def test_detect_no_rust_long_with_hl(self):
        set_rust(False)
        d = rg.RegimeDetector()
        closes = self._series(50)
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        reg, feats = d.detect(closes, highs, lows, [1000] * 50)
        self.assertIsInstance(reg, rg.Regime)
        self.assertGreaterEqual(feats.volume_trend, -1e9)
        set_rust(True)

    def test_detect_rust_short_uses_features(self):
        set_rust(True)
        d = rg.RegimeDetector()
        # < 30 closes -> pure python branch even with rust available
        reg, feats = d.detect(self._series(20))
        self.assertIsInstance(reg, rg.Regime)

    def test_recommended_rust(self):
        set_rust(True)
        d = rg.RegimeDetector()
        self.assertIsInstance(d.recommended_strategies(rg.Regime.RANGING), list)

    def test_recommended_no_rust(self):
        set_rust(False)
        d = rg.RegimeDetector()
        s = d.recommended_strategies(rg.Regime.RANGING)
        self.assertIn("rsi_revert", s)
        # unknown falls back to UNKNOWN map
        self.assertIsInstance(d.recommended_strategies(rg.Regime.UNKNOWN), list)
        set_rust(True)


class FakeBlender:
    def blend_signals(self, signals, regime):
        return signals


class TestAdaptiveStrategySelector(unittest.TestCase):
    def setUp(self):
        set_rust(True)
        self.sel = rg.AdaptiveStrategySelector()

    def test_set_regime_enum(self):
        self.assertEqual(self.sel.set_regime(rg.Regime.RANGING), rg.Regime.RANGING)

    def test_set_regime_str(self):
        self.assertEqual(self.sel.set_regime("strong_uptrend"), rg.Regime.STRONG_UPTREND)
        self.assertEqual(self.sel.set_regime("not_a_regime"), rg.Regime.UNKNOWN)

    def test_update_swap(self):
        self.sel.set_regime(rg.Regime.RANGING)
        self.sel.update([100 + i for i in range(40)])
        self.sel.update([200 - i for i in range(40)])  # likely different regime
        self.assertGreaterEqual(self.sel._strategy_swap_count, 0)

    def test_update_history_cap(self):
        for i in range(120):
            self.sel.update([100 + i + j for j in range(40)])
        self.assertEqual(len(self.sel._regime_history), 100)

    def test_active_strategies(self):
        self.sel.set_regime(rg.Regime.RANGING)
        self.assertIn("rsi_revert", self.sel.active_strategies())

    def test_select(self):
        self.sel.set_regime(rg.Regime.RANGING)
        out = self.sel.select("rsi_revert")
        self.assertTrue(out["enabled"])

    def test_filter_no_blender(self):
        self.sel.set_regime(rg.Regime.RANGING)

        class O:
            def __init__(s, n):
                s.strategy_name = n
        out = self.sel.filter_opportunities([O("rsi_revert"), O("ema_cross")])
        self.assertEqual(len(out), 1)

    def test_filter_with_blender(self):
        self.sel.set_regime(rg.Regime.RANGING)
        self.sel.blender = FakeBlender()

        class O:
            def __init__(s, n):
                s.strategy_name = n
        out = self.sel.filter_opportunities([O("rsi_revert")])
        self.assertEqual(len(out), 1)

    def test_regime_stability_short(self):
        self.assertEqual(self.sel.regime_stability(), 1.0)

    def test_regime_stability_full(self):
        for i in range(12):
            self.sel.update([100 + (i % 2) + j for j in range(40)])
        self.assertGreaterEqual(self.sel.regime_stability(), 0.0)

    def test_regime_summary(self):
        self.sel.set_regime(rg.Regime.RANGING)
        d = self.sel.regime_summary()
        self.assertEqual(d["regime"], "ranging")
        self.assertIn("strategies", d)


if __name__ == "__main__":
    unittest.main()
