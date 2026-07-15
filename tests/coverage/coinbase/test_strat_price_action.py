import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from _strat_helpers import bars_from

from coinbase.src.protocols import Direction
from coinbase.src.strat_price_action import (
    PriceActionSRStrategy,
    SwingPoint,
    SupportResistanceLevel,
)


def make_strat(**kw):
    return PriceActionSRStrategy(**kw)


class TestPriceActionOnBar(unittest.TestCase):
    def _bars(self, n=35):
        closes = [100 + i * 0.1 for i in range(n)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        vols = [1000.0] * n
        return bars_from(closes, highs=highs, lows=lows, vols=vols)

    def test_insufficient_history(self):
        s = make_strat()
        bars = self._bars(20)
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_no_setup(self):
        s = make_strat()
        s._check_break_retest = lambda *a, **k: None
        s._check_bounce = lambda *a, **k: None
        s._check_trendline_break = lambda *a, **k: None
        bars = self._bars()
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_break_retest_return(self):
        s = make_strat()
        s._check_break_retest = lambda *a, **k: _setup(Direction.LONG)
        s._check_bounce = lambda *a, **k: None
        s._check_trendline_break = lambda *a, **k: None
        bars = self._bars()
        self.assertIsNotNone(s.on_bar(bars[-1], bars[:-1]))

    def test_bounce_return(self):
        s = make_strat()
        s._check_break_retest = lambda *a, **k: None
        s._check_bounce = lambda *a, **k: _setup(Direction.LONG)
        s._check_trendline_break = lambda *a, **k: None
        bars = self._bars()
        self.assertIsNotNone(s.on_bar(bars[-1], bars[:-1]))

    def test_trendline_return(self):
        s = make_strat()
        s._check_break_retest = lambda *a, **k: None
        s._check_bounce = lambda *a, **k: None
        s._check_trendline_break = lambda *a, **k: _setup(Direction.SHORT)
        bars = self._bars()
        self.assertIsNotNone(s.on_bar(bars[-1], bars[:-1]))


def _setup(direction):
    from coinbase.src.protocols import Bar, BracketSetup
    return BracketSetup(direction=direction, entry_price=100, stop_price=98,
                        target_price=104, risk_reward=2.0, confidence=0.5,
                        reason="x", strategy_name="price_action_sr", atr=1.0)


class TestBreakRetest(unittest.TestCase):
    def _levels(self, price, kind):
        return [SupportResistanceLevel(price=price, kind=kind, touches=2, strength=1.0)]

    def test_no_levels(self):
        s = make_strat()
        bars = bars_from([98, 99, 100, 99, 101])
        self.assertIsNone(s._check_break_retest(bars, [], self._levels(100, "resistance"), 1.0))

    def test_resistance_up(self):
        s = make_strat()
        bars = bars_from([98.0, 99.0, 100.0, 99.0, 101.0])
        setup = s._check_break_retest(bars, [], self._levels(100, "resistance"), 1.0)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_resistance_down(self):
        s = make_strat()
        bars = bars_from([102.0, 101.0, 100.0, 101.0, 99.0])
        setup = s._check_break_retest(bars, [], self._levels(100, "resistance"), 1.0)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_support_down(self):
        s = make_strat()
        bars = bars_from([102.0, 101.0, 100.0, 101.0, 99.0])
        setup = s._check_break_retest(bars, [], self._levels(100, "support"), 1.0)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_support_up(self):
        s = make_strat()
        bars = bars_from([98.0, 99.0, 100.0, 99.0, 101.0])
        setup = s._check_break_retest(bars, [], self._levels(100, "support"), 1.0)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_no_break(self):
        s = make_strat()
        bars = bars_from([100.0, 100.0, 100.0, 100.0, 100.0])
        self.assertIsNone(s._check_break_retest(bars, [], self._levels(100, "resistance"), 1.0))

    def test_dist_too_far(self):
        s = make_strat()
        bars = bars_from([98.0, 99.0, 100.0, 99.0, 101.0])
        self.assertIsNone(s._check_break_retest(bars, [], self._levels(200.0, "resistance"), 0.1))

    def test_low_rr(self):
        s = make_strat()
        bars = bars_from([98.0, 99.0, 100.0, 99.0, 101.0])
        self.assertIsNone(s._check_break_retest(bars, [], self._levels(100, "resistance"), 0.1))


class TestBounce(unittest.TestCase):
    def _hist(self, n=15):
        return bars_from([100.0] * n)

    def test_short_closes(self):
        s = make_strat()
        hist = self._hist(12)
        self.assertIsNone(s._check_bounce(bars_from([100.0])[0], hist, (100.0, 1.0), None, 1.0))

    def test_long(self):
        s = make_strat()
        s._rsi = lambda closes, p: 30.0
        hist = self._hist(15)
        setup = s._check_bounce(bars_from([100.0])[0], hist, (100.0, 1.0), None, 1.0)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_short(self):
        s = make_strat()
        s._rsi = lambda closes, p: 70.0
        hist = self._hist(15)
        setup = s._check_bounce(bars_from([100.0])[0], hist, None, (110.0, 1.0), 1.0)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_neutral(self):
        s = make_strat()
        s._rsi = lambda closes, p: 50.0
        hist = self._hist(15)
        self.assertIsNone(s._check_bounce(bars_from([100.0])[0], hist, (100.0, 1.0), None, 1.0))


class TestTrendlineBreak(unittest.TestCase):
    def _bars(self, n=25):
        return bars_from([100.0] * n)

    def test_few_swings(self):
        s = make_strat()
        swings = [SwingPoint(0, 100, "high"), SwingPoint(1, 90, "low")]
        self.assertIsNone(s._check_trendline_break(self._bars(), swings, 1.0))

    def test_few_bars(self):
        s = make_strat()
        swings = [SwingPoint(0, 100, "high"), SwingPoint(1, 90, "low"),
                  SwingPoint(2, 100, "high"), SwingPoint(3, 90, "low")]
        self.assertIsNone(s._check_trendline_break(bars_from([100.0] * 15), swings, 1.0))

    def test_long(self):
        s = make_strat()
        highs = [SwingPoint(23, 102, "high"), SwingPoint(24, 100, "high")]
        lows = [SwingPoint(23, 99, "low"), SwingPoint(24, 99, "low")]
        swings = highs + lows
        bars = self._bars()
        bars[-1] = bars_from([101.0])[0]
        setup = s._check_trendline_break(bars, swings, 1.0)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_short(self):
        s = make_strat()
        lows = [SwingPoint(23, 98, "low"), SwingPoint(24, 100, "low")]
        highs = [SwingPoint(23, 101, "high"), SwingPoint(24, 101, "high")]
        swings = highs + lows
        bars = self._bars()
        bars[-1] = bars_from([99.0])[0]
        setup = s._check_trendline_break(bars, swings, 1.0)
        self.assertEqual(setup.direction, Direction.SHORT)


class TestHelpers(unittest.TestCase):
    def test_detect_swing_points(self):
        highs = [1, 2, 3, 2, 1]
        lows = [1, 1, 1, 1, 1]
        swings = PriceActionSRStrategy()._detect_swing_points(highs, lows, lookback=1)
        self.assertTrue(any(s.kind == "high" for s in swings))

    def test_detect_swing_low(self):
        highs = [1, 1, 1, 1, 1]
        lows = [3, 2, 1, 2, 3]
        swings = PriceActionSRStrategy()._detect_swing_points(highs, lows, lookback=1)
        self.assertTrue(any(s.kind == "low" for s in swings))

    def test_build_sr_levels(self):
        swings = [SwingPoint(1, 100.0, "low"), SwingPoint(2, 100.0, "low")]
        closes = [100.0] * 20
        levels = PriceActionSRStrategy()._build_sr_levels(swings, closes)
        self.assertEqual(len(levels), 1)
        self.assertEqual(levels[0].kind, "support")

    def test_build_sr_no_touch(self):
        swings = [SwingPoint(1, 100.0, "low")]
        closes = [100.0] * 20
        levels = PriceActionSRStrategy()._build_sr_levels(swings, closes)
        self.assertEqual(len(levels), 0)

    def test_nearest_level(self):
        levels = [SupportResistanceLevel(price=100.0, kind="support", touches=2, strength=1.0)]
        res = PriceActionSRStrategy._nearest_level(101.0, levels, "support", 1.0)
        self.assertIsNotNone(res)

    def test_nearest_level_none(self):
        levels = [SupportResistanceLevel(price=100.0, kind="support", touches=2, strength=1.0)]
        res = PriceActionSRStrategy._nearest_level(150.0, levels, "support", 1.0)
        self.assertIsNone(res)

    def test_nearest_level_wrong_kind(self):
        levels = [SupportResistanceLevel(price=100.0, kind="resistance", touches=2, strength=1.0)]
        res = PriceActionSRStrategy._nearest_level(101.0, levels, "support", 1.0)
        self.assertIsNone(res)

    def test_rsi_short(self):
        self.assertEqual(PriceActionSRStrategy._rsi([1, 2]), 50.0)

    def test_rsi_gains(self):
        closes = [100 + i for i in range(15)]
        self.assertEqual(PriceActionSRStrategy._rsi(closes, 14), 100.0)

    def test_rsi_normal(self):
        closes = [100, 102, 101, 103, 100, 98, 101, 104, 99, 97,
                  100, 102, 103, 101, 100]
        r = PriceActionSRStrategy._rsi(closes, 14)
        self.assertTrue(0.0 < r < 100.0)

    def test_atr_short(self):
        self.assertEqual(PriceActionSRStrategy._estimate_atr([1, 2], [1, 2], [1, 2]), 0.0)

    def test_atr_normal(self):
        closes = [100 + i for i in range(20)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        self.assertGreater(PriceActionSRStrategy._estimate_atr(closes, highs, lows), 0.0)


if __name__ == "__main__":
    unittest.main()
