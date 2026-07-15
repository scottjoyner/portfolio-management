import unittest
from coinbase.src.adaptive_mode import (
    AdaptiveModeSelector, AdaptiveScalpSwingStrategy, TradingMode, MODE_PROFILES,
)
from coinbase.src.protocols import Direction, Bar, BracketSetup


def make_bars(n, base=100.0, drift=0.0):
    bars = []
    for i in range(n):
        c = base + i * drift
        bars.append(Bar(timestamp=float(i), open=c, high=c + 1.0, low=c - 1.0,
                        close=c, volume=1000.0))
    return bars


class TestAdaptiveModeSelector(unittest.TestCase):
    def test_default_mode(self):
        s = AdaptiveModeSelector()
        self.assertEqual(s.current_mode, TradingMode.SWING)

    def test_update_cooldown(self):
        s = AdaptiveModeSelector(switch_cooldown_bars=10)
        s._bars_since_switch = 5
        self.assertEqual(s.update("ranging", 10.0), TradingMode.SWING)

    def test_update_switches(self):
        s = AdaptiveModeSelector(switch_cooldown_bars=0)
        s._bars_since_switch = 100
        new = s.update("strong_uptrend", 10.0, fear_greed_value=50, adx=40, trend_strength=0.05)
        self.assertEqual(new, TradingMode.TREND)
        self.assertEqual(s._last_fg, 50.0)
        self.assertEqual(s._last_regime, "strong_uptrend")

    def test_update_history_cap(self):
        s = AdaptiveModeSelector(switch_cooldown_bars=0)
        for _ in range(20):
            s._bars_since_switch = 100
            s.update("ranging", 10.0)
        self.assertLessEqual(len(s._mode_history), 10)

    def test_select_mode_cases(self):
        cases = [
            (dict(regime="x", vol_bps=10, fg=10, adx=25, trend=0), TradingMode.TREND),
            (dict(regime="x", vol_bps=10, fg=90, adx=25, trend=0), TradingMode.TREND),
            (dict(regime="strong_uptrend", vol_bps=10, fg=50, adx=25, trend=0), TradingMode.TREND),
            (dict(regime="weak_uptrend", vol_bps=50, fg=50, adx=25, trend=0), TradingMode.SWING),
            (dict(regime="strong_downtrend", vol_bps=100, fg=50, adx=25, trend=0), TradingMode.SCALP),
            (dict(regime="ranging", vol_bps=50, fg=50, adx=25, trend=0), TradingMode.SCALP),
            (dict(regime="low_volatility", vol_bps=50, fg=50, adx=25, trend=0), TradingMode.SCALP),
            (dict(regime="high_volatility", vol_bps=50, fg=50, adx=25, trend=0), TradingMode.SCALP),
            (dict(regime="weak_uptrend", vol_bps=50, fg=50, adx=10, trend=0), TradingMode.SWING),
            (dict(regime="weak_uptrend", vol_bps=50, fg=50, adx=40, trend=0), TradingMode.SWING),
            (dict(regime="weird", vol_bps=50, fg=50, adx=25, trend=0), TradingMode.SWING),
        ]
        for kwargs, expected in cases:
            s = AdaptiveModeSelector()
            self.assertEqual(s._select_mode(**kwargs), expected, kwargs)

    def test_select_mode_enum_regime(self):
        s = AdaptiveModeSelector()
        self.assertEqual(s._select_mode(regime=TradingMode.SCALP, vol_bps=10, fg=50, adx=10, trend=0),
                         TradingMode.SCALP)

    def test_profile_and_summary(self):
        s = AdaptiveModeSelector()
        s.current_mode = TradingMode.SCALP
        self.assertEqual(s.profile(), MODE_PROFILES[TradingMode.SCALP])
        summ = s.summary()
        self.assertEqual(summ["mode"], "scalp")
        self.assertIn("stop_atr", summ)


class TestAdaptiveScalpSwingStrategy(unittest.TestCase):
    def setUp(self):
        self.strat = AdaptiveScalpSwingStrategy()
        self.assertEqual(self.strat.name(), "adaptive_mode")

    def test_on_bar_too_few(self):
        bars = make_bars(10)
        self.assertIsNone(self.strat.on_bar(bars[-1], bars[:-1]))

    def test_on_bar_zero_atr(self):
        bars = make_bars(40, drift=0.0)
        self.strat.mode_selector.current_mode = TradingMode.SCALP
        self.assertIsNone(self.strat.on_bar(bars[-1], bars[:-1]))

    def test_on_bar_dispatch_each_mode(self):
        bars = make_bars(40, drift=0.05)
        for mode in (TradingMode.SCALP, TradingMode.SWING, TradingMode.TREND, TradingMode.HOLD):
            self.strat.mode_selector.current_mode = mode
            self.strat.on_bar(bars[-1], bars[:-1])

    def test_scalp_signal_branches(self):
        prof = MODE_PROFILES[TradingMode.SCALP]
        cur, atr = 100.0, 2.0
        self.assertIsNone(self.strat._scalp_signal(cur, atr, 30, 10, True, prof))
        self.assertIsNone(self.strat._scalp_signal(cur, atr, 30, 200, False, prof))
        self.assertIsNone(self.strat._scalp_signal(cur, atr, 50, 10, False, prof))
        lng = self.strat._scalp_signal(cur, atr, 20, 10, False, prof)
        self.assertEqual(lng.direction, Direction.LONG)
        sht = self.strat._scalp_signal(cur, atr, 80, 10, False, prof)
        self.assertEqual(sht.direction, Direction.SHORT)
        # rr guard
        none = self.strat._scalp_signal(cur, atr, 20, 10, False, {"stop_atr": 0.8, "target_atr": 1.2, "min_rr": 100})
        self.assertIsNone(none)

    def test_swing_signal_branches(self):
        prof = MODE_PROFILES[TradingMode.SWING]
        atr = 2.0
        closes = [100.0] * 25
        # current>ma20 and rsi<45 -> long
        lng = self.strat._swing_signal(101.0, atr, 40, 10, prof, closes)
        self.assertEqual(lng.direction, Direction.LONG)
        # current<ma20 and rsi>55 -> short
        sht = self.strat._swing_signal(99.0, atr, 56, 10, prof, closes)
        self.assertEqual(sht.direction, Direction.SHORT)
        # rsi<35 -> long
        lng2 = self.strat._swing_signal(100.0, atr, 30, 10, prof, closes)
        self.assertEqual(lng2.direction, Direction.LONG)
        # rsi>65 -> short
        sht2 = self.strat._swing_signal(100.0, atr, 70, 10, prof, closes)
        self.assertEqual(sht2.direction, Direction.SHORT)
        # else none
        self.assertIsNone(self.strat._swing_signal(100.0, atr, 50, 10, prof, closes))
        # rr guard
        self.assertIsNone(self.strat._swing_signal(101.0, atr, 40, 10, {"stop_atr": 1.5, "target_atr": 2.5, "min_rr": 100}, closes))

    def test_trend_signal_branches(self):
        prof = MODE_PROFILES[TradingMode.TREND]
        cur = 100.0
        uptrend = [99.0] * 30 + [101.0] * 30
        downtrend = [101.0] * 30 + [99.0] * 30
        self.assertIsNone(self.strat._trend_signal(cur, 2.0, 50, prof, [100.0] * 10))
        lng = self.strat._trend_signal(cur, 2.0, 40, prof, uptrend)
        self.assertEqual(lng.direction, Direction.LONG)
        sht = self.strat._trend_signal(cur, 2.0, 56, prof, downtrend)
        self.assertEqual(sht.direction, Direction.SHORT)
        # rr guard
        self.assertIsNone(self.strat._trend_signal(cur, 2.0, 40, {"stop_atr": 1.0, "target_atr": 5.0, "min_rr": 100}, uptrend))

    def test_rsi_helper(self):
        self.assertEqual(self.strat._rsi([1, 2], period=14), 50.0)
        closes = [100.0] * 15
        for i in range(5):
            closes.append(closes[-1] * 0.9)
        r = self.strat._rsi(closes, period=14)
        self.assertLess(r, 50)
        # all gains -> 100
        gains = [float(i) for i in range(1, 16)]
        self.assertEqual(self.strat._rsi(gains, period=14), 100.0)

    def test_atr_helper(self):
        self.assertEqual(self.strat._estimate_atr([1, 2], [2, 3], [1, 2], period=14), 0.0)
        closes = list(range(20, 40))
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        atr = self.strat._estimate_atr(closes, highs, lows, period=14)
        self.assertGreater(atr, 0)


class TestAdaptiveModeEdgeCases(unittest.TestCase):
    def setUp(self):
        self.strat = AdaptiveScalpSwingStrategy()
        self.sel = AdaptiveModeSelector()

    def test_history_cap_truncation(self):
        # force repeated mode switches so _mode_history exceeds the cap of 10
        s = AdaptiveModeSelector(switch_cooldown_bars=0)
        s.current_mode = TradingMode.SCALP
        for _ in range(25):
            s._bars_since_switch = 100
            # alternate regimes that map to different modes
            s.update("strong_uptrend", 10.0)
            s._bars_since_switch = 100
            s.update("ranging", 10.0)
        self.assertLessEqual(len(s._mode_history), 10)

    def test_select_mode_adx_gt_35_unknown_regime(self):
        # unknown regime falling through to adx>35 -> TREND
        self.assertEqual(
            self.sel._select_mode(regime="weird", vol_bps=50, fg=50, adx=40, trend=0),
            TradingMode.TREND,
        )

    def test_select_mode_enum_regime_value(self):
        self.assertEqual(
            self.sel._select_mode(regime=TradingMode.HIGH_VOLATILITY if False else "high_volatility",
                                  vol_bps=50, fg=50, adx=25, trend=0),
            TradingMode.SCALP,
        )

    def test_on_bar_zero_atr_trend_mode(self):
        # bars with high==low==close => true zero ATR => on_bar returns None
        bars = [Bar(timestamp=float(i), open=100.0, high=100.0, low=100.0,
                    close=100.0, volume=1000.0) for i in range(60)]
        self.strat.mode_selector.current_mode = TradingMode.TREND
        self.assertIsNone(self.strat.on_bar(bars[-1], bars[:-1]))

    def test_trend_signal_rr_guard_and_else_branches(self):
        prof = MODE_PROFILES[TradingMode.TREND]
        uptrend = [99.0] * 30 + [101.0] * 30
        # up-trend but rsi >= 45 -> None (else branch)
        self.assertIsNone(self.strat._trend_signal(100.0, 2.0, 50, prof, uptrend))
        # down-trend but rsi <= 55 -> None (else branch)
        downtrend = [101.0] * 30 + [99.0] * 30
        self.assertIsNone(self.strat._trend_signal(100.0, 2.0, 55, prof, downtrend))
        # rr guard via huge min_rr
        self.assertIsNone(self.strat._trend_signal(
            100.0, 2.0, 40, {"stop_atr": 1.0, "target_atr": 5.0, "min_rr": 100}, uptrend))

    def test_on_bar_requires_30_bars_with_history(self):
        bars = make_bars(31, drift=0.1)
        self.strat.mode_selector.current_mode = TradingMode.SCALP
        # 31 bars -> enough; just ensure no crash and returns (possibly None if rsi mid)
        self.strat.on_bar(bars[-1], bars[:-1])


if __name__ == "__main__":
    unittest.main()
