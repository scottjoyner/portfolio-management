import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from _strat_helpers import bars_from

from coinbase.src.protocols import Bar, Direction
from coinbase.src.strat_candlestick import CandlestickPatternStrategy


def bar(o, h, l, c, v=1000.0):
    return Bar(timestamp=0.0, open=o, high=h, low=l, close=c, volume=v)


class TestCandlestickOnBar(unittest.TestCase):
    def test_insufficient_history(self):
        s = CandlestickPatternStrategy()
        bars = bars_from([100 + i for i in range(4)])
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_atr_zero(self):
        closes = [100.0] * 20
        bars = bars_from(closes, highs=closes, lows=closes)
        s = CandlestickPatternStrategy()
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def _neutral_baseline(self, n=18):
        closes = [100 + i for i in range(n)]
        opens = [c * 0.99 for c in closes]
        highs = [c * 1.01 for c in closes]
        lows = [c * 0.98 for c in closes]
        return bars_from(closes, opens=opens, highs=highs, lows=lows)

    def test_no_pattern(self):
        s = CandlestickPatternStrategy()
        bars = self._neutral_baseline(20)
        s._detect_pattern = lambda bars: None
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_long_pattern(self):
        s = CandlestickPatternStrategy()
        bars = self._neutral_baseline(18)
        last = bar(117.05, 119.0, 117.0, 118.95)  # marubozu bull
        setup = s.on_bar(last, bars)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_short_pattern(self):
        s = CandlestickPatternStrategy()
        bars = self._neutral_baseline(18)
        last = bar(118.95, 119.0, 117.0, 117.05)  # marubozu bear
        setup = s.on_bar(last, bars)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_rr_too_small(self):
        s = CandlestickPatternStrategy()
        bars = self._neutral_baseline(18)
        last = bar(117.05, 119.0, 90.0, 118.95)  # LONG hammer, far low -> stop far
        self.assertIsNone(s.on_bar(last, bars))


class TestSinglePatterns(unittest.TestCase):
    def setUp(self):
        self.s = CandlestickPatternStrategy()

    def test_doiji_long(self):
        b = bar(100, 102, 98, 100.05)
        r = self.s._check_single_patterns(b)
        self.assertEqual(r[0], "doji")
        self.assertEqual(r[1], Direction.LONG)

    def test_doiji_short(self):
        b = bar(100, 102, 98, 99.95)
        r = self.s._check_single_patterns(b)
        self.assertEqual(r[1], Direction.SHORT)

    def test_total_range_zero(self):
        self.assertIsNone(self.s._check_single_patterns(bar(100, 100, 100, 100)))

    def test_shooting_star(self):
        b = bar(100, 110, 100, 100.5)
        r = self.s._check_single_patterns(b)
        self.assertEqual(r[0], "shooting_star")

    def test_inverted_hammer(self):
        b = bar(100, 107.2, 97.2, 101.5)
        r = self.s._check_single_patterns(b)
        self.assertEqual(r[0], "inverted_hammer")

    def test_hammer(self):
        b = bar(100, 102, 93, 101)
        r = self.s._check_single_patterns(b)
        self.assertEqual(r[0], "hammer")

    def test_hanging_man(self):
        b = bar(100, 102, 97.5, 101)
        r = self.s._check_single_patterns(b)
        self.assertEqual(r[0], "hanging_man")

    def test_marubozu_bull(self):
        b = bar(99.05, 101, 99, 100.95)
        r = self.s._check_single_patterns(b)
        self.assertEqual(r[0], "marubozu_bull")

    def test_marubozu_bear(self):
        b = bar(100.95, 101, 99, 99.05)
        r = self.s._check_single_patterns(b)
        self.assertEqual(r[0], "marubozu_bear")

    def test_spinning_top_dead(self):
        # spinning_top branch is geometrically unreachable (upper+lower+body==1)
        b = bar(100, 101.3, 99.1, 101.2)
        self.assertIsNone(self.s._check_single_patterns(b))

    def test_none(self):
        b = bar(100, 102, 98, 101)
        self.assertIsNone(self.s._check_single_patterns(b))


class TestTwoBarPatterns(unittest.TestCase):
    def setUp(self):
        self.s = CandlestickPatternStrategy()

    def test_bullish_engulfing(self):
        r = self.s._check_two_bar_patterns(bar(100, 101, 99, 99),
                                           bar(98, 102, 97, 102))
        self.assertEqual(r[0], "bullish_engulfing")

    def test_piercing_line(self):
        r = self.s._check_two_bar_patterns(bar(100, 101, 99, 99),
                                           bar(99.5, 101, 99, 100.5))
        self.assertEqual(r[0], "piercing_line")

    def test_bearish_engulfing(self):
        r = self.s._check_two_bar_patterns(bar(100, 101, 99, 101),
                                           bar(102, 103, 98, 98))
        self.assertEqual(r[0], "bearish_engulfing")

    def test_dark_cloud_cover(self):
        r = self.s._check_two_bar_patterns(bar(100.5, 101, 99, 101),
                                           bar(100.8, 102, 99.5, 99.5))
        self.assertEqual(r[0], "dark_cloud_cover")

    def test_bullish_harami(self):
        r = self.s._check_two_bar_patterns(bar(100, 111, 99, 110),
                                           bar(110.5, 111, 108, 111.0))
        self.assertEqual(r[0], "bullish_harami")

    def test_bearish_harami(self):
        r = self.s._check_two_bar_patterns(bar(110, 111, 99, 101),
                                           bar(100.6, 100.9, 99.5, 100.4))
        self.assertEqual(r[0], "bearish_harami")

    def test_zero_range(self):
        self.assertIsNone(self.s._check_two_bar_patterns(bar(100, 100, 100, 100),
                                                         bar(100, 100, 100, 100)))

    def test_none(self):
        self.assertIsNone(self.s._check_two_bar_patterns(bar(100, 101, 99, 100.5),
                                                         bar(100.5, 102, 100, 101)))


class TestThreeBarPatterns(unittest.TestCase):
    def setUp(self):
        self.s = CandlestickPatternStrategy()

    def _b(self, o, h, l, c):
        return bar(o, h, l, c)

    def test_morning_star(self):
        b1 = self._b(100, 101, 98, 95)
        b2 = self._b(95, 96, 94, 96)
        b3 = self._b(96, 100, 97, 100)
        self.assertEqual(self.s._check_three_bar_patterns(b1, b2, b3)[0], "morning_star")

    def test_morning_star_b2_not_lowest(self):
        # b1_bear & b3_bull but b2.low not below -> falls through
        b1 = self._b(105, 107, 98, 100)
        b2 = self._b(95, 96, 96, 96)
        b3 = self._b(96, 100, 95, 102)
        self.assertIsNone(self.s._check_three_bar_patterns(b1, b2, b3))

    def test_evening_star(self):
        b1 = self._b(100, 101, 99, 105)
        b2 = self._b(105.5, 106, 104, 105.6)
        b3 = self._b(104, 105, 100, 101)
        self.assertEqual(self.s._check_three_bar_patterns(b1, b2, b3)[0], "evening_star")

    def test_evening_star_b2_not_highest(self):
        b1 = self._b(100, 101, 99, 105)
        b2 = self._b(105, 104, 104, 105)
        b3 = self._b(104, 105, 100, 101)
        self.assertIsNone(self.s._check_three_bar_patterns(b1, b2, b3))

    def test_three_bar_reversal_bull(self):
        b1 = self._b(99, 101, 94, 95)
        b2 = self._b(95, 96, 94.5, 96)
        b3 = self._b(96, 100, 95, 100)
        self.assertEqual(self.s._check_three_bar_patterns(b1, b2, b3)[0], "three_bar_reversal_bull")

    def test_three_bar_reversal_bull_noclose(self):
        b1 = self._b(105, 107, 98, 100)
        b2 = self._b(95, 96, 99, 96)
        b3 = self._b(96, 100, 95, 102)
        self.assertIsNone(self.s._check_three_bar_patterns(b1, b2, b3))

    def test_three_bar_reversal_bear(self):
        b1 = self._b(102, 105, 98, 104)
        b2 = self._b(105, 104, 104, 105)
        b3 = self._b(104, 104, 100, 101)
        self.assertEqual(self.s._check_three_bar_patterns(b1, b2, b3)[0], "three_bar_reversal_bear")

    def test_three_bar_reversal_bear_nohigh(self):
        b1 = self._b(102, 105, 98, 104)
        b2 = self._b(105, 104, 104, 105)
        b3 = self._b(104, 108, 100, 101)
        self.assertIsNone(self.s._check_three_bar_patterns(b1, b2, b3))

    def test_three_white_soldiers(self):
        b1 = self._b(100, 102, 99, 101)
        b2 = self._b(101, 103, 100, 102)
        b3 = self._b(102, 104, 101, 103)
        self.assertEqual(self.s._check_three_bar_patterns(b1, b2, b3)[0], "three_white_soldiers")

    def test_three_white_soldiers_fails_body(self):
        b1 = self._b(100, 102, 99, 101)
        b2 = self._b(101, 105, 100, 104)
        b3 = self._b(104, 106, 103, 105)
        self.assertIsNone(self.s._check_three_bar_patterns(b1, b2, b3))

    def test_three_black_crows(self):
        b1 = self._b(100, 101, 98, 99)
        b2 = self._b(99, 100, 97, 98)
        b3 = self._b(98, 99, 96, 97)
        self.assertEqual(self.s._check_three_bar_patterns(b1, b2, b3)[0], "three_black_crows")

    def test_three_black_crows_fails_body(self):
        b1 = self._b(100, 101, 98, 99)
        b2 = self._b(99, 100, 95, 96)
        b3 = self._b(96, 97, 94, 95)
        self.assertIsNone(self.s._check_three_bar_patterns(b1, b2, b3))

    def test_none(self):
        b1 = self._b(100, 101, 99, 100.5)
        b2 = self._b(100.5, 102, 99.5, 100.0)
        b3 = self._b(100, 101, 98, 100.5)
        self.assertIsNone(self.s._check_three_bar_patterns(b1, b2, b3))


class TestDetectPattern(unittest.TestCase):
    def setUp(self):
        self.s = CandlestickPatternStrategy()

    def test_single_only(self):
        self.assertIsNotNone(self.s._detect_pattern([bar(100, 102, 98, 100.05)]))

    def test_two_bar(self):
        self.assertIsNotNone(self.s._detect_pattern([bar(100, 101, 99, 99),
                                                     bar(98, 102, 97, 102)]))

    def test_three_bar(self):
        b1 = bar(100, 101, 98, 95)
        b2 = bar(95, 96, 94, 96)
        b3 = bar(96, 100, 97, 100)
        self.assertEqual(self.s._detect_pattern([b1, b2, b3])[0], "morning_star")

    def test_no_results(self):
        self.assertIsNone(self.s._detect_pattern([bar(100, 102, 98, 101),
                                                  bar(101, 103, 100, 102)]))

    def test_max_by_confidence(self):
        self.assertEqual(self.s._detect_pattern([bar(100, 101, 99, 99),
                                                 bar(98, 102, 97, 102)])[0], "bullish_engulfing")


class TestAtr(unittest.TestCase):
    def test_short(self):
        self.assertEqual(CandlestickPatternStrategy._estimate_atr([1, 2], [1, 2], [1, 2]), 0.0)

    def test_normal(self):
        closes = [100 + i for i in range(20)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        self.assertGreater(CandlestickPatternStrategy._estimate_atr(closes, highs, lows), 0.0)


if __name__ == "__main__":
    unittest.main()
