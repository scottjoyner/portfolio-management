import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from _strat_helpers import bars_from

from coinbase.src.protocols import Direction, InstrumentType
from coinbase.src.strat_funding import FundingRateCaptureStrategy


class TestFunding(unittest.TestCase):
    def _bars(self, n=31, base=100.0):
        closes = [base + i * 0.2 for i in range(n)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        vols = [1000.0] * n
        return bars_from(closes, highs=highs, lows=lows, vols=vols)

    def test_insufficient_history(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars(20)
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_atr_zero(self):
        s = FundingRateCaptureStrategy()
        closes = [100.0] * 31
        bars = bars_from(closes, highs=closes, lows=closes)
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_funding_none(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars()
        s._estimate_funding_rate = lambda bars: None
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_positive_short(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars()
        s._estimate_funding_rate = lambda bars: 0.0003
        s._detect_vol_regime = lambda c, h, l: "normal"
        s._detect_trend = lambda c: "neutral"
        setup = s.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.SHORT)
        self.assertEqual(setup.instrument_type, InstrumentType.PERP_FUTURES)

    def test_positive_short_headwind(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars()
        s._estimate_funding_rate = lambda bars: 0.0003
        s._detect_vol_regime = lambda c, h, l: "normal"
        s._detect_trend = lambda c: "uptrend"
        setup = s.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(setup)
        self.assertIn("(trend headwind)", setup.reason)

    def test_positive_too_high(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars()
        s._estimate_funding_rate = lambda bars: 0.001
        s._detect_vol_regime = lambda c, h, l: "normal"
        s._detect_trend = lambda c: "neutral"
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_positive_high_regime(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars()
        s._estimate_funding_rate = lambda bars: 0.0003
        s._detect_vol_regime = lambda c, h, l: "high"
        s._detect_trend = lambda c: "neutral"
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_negative_long(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars()
        s._estimate_funding_rate = lambda bars: -0.0003
        s._detect_vol_regime = lambda c, h, l: "normal"
        s._detect_trend = lambda c: "neutral"
        setup = s.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_negative_long_headwind(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars()
        s._estimate_funding_rate = lambda bars: -0.0003
        s._detect_vol_regime = lambda c, h, l: "normal"
        s._detect_trend = lambda c: "downtrend"
        setup = s.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(setup)
        self.assertIn("(trend headwind)", setup.reason)

    def test_negative_too_low(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars()
        s._estimate_funding_rate = lambda bars: -0.001
        s._detect_vol_regime = lambda c, h, l: "normal"
        s._detect_trend = lambda c: "neutral"
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_negative_high_regime(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars()
        s._estimate_funding_rate = lambda bars: -0.0003
        s._detect_vol_regime = lambda c, h, l: "high"
        s._detect_trend = lambda c: "neutral"
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_no_funding(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars()
        s._estimate_funding_rate = lambda bars: 0.00001
        s._detect_vol_regime = lambda c, h, l: "normal"
        s._detect_trend = lambda c: "neutral"
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_funding_history_truncate(self):
        s = FundingRateCaptureStrategy()
        bars = self._bars()
        s._funding_history = [0.0] * (s.lookback + 1)
        s._estimate_funding_rate = lambda bars: 0.0003
        s._detect_vol_regime = lambda c, h, l: "normal"
        s._detect_trend = lambda c: "neutral"
        self.assertIsNotNone(s.on_bar(bars[-1], bars[:-1]))
        # history grows by 1 then truncates once per call -> steady length lookback+1
        self.assertLessEqual(len(s._funding_history), s.lookback + 1)


class TestFundingHelpers(unittest.TestCase):
    def _bars(self, n=6, base=100.0):
        closes = [base + i for i in range(n)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        vols = [1000.0] * n
        return bars_from(closes, highs=highs, lows=lows, vols=vols)

    def test_funding_short(self):
        s = FundingRateCaptureStrategy()
        self.assertIsNone(s._estimate_funding_rate([1, 2]))

    def test_funding_base_vol_zero(self):
        bars = bars_from([100.0] * 5, highs=[100.0] * 5, lows=[100.0] * 5)
        self.assertAlmostEqual(FundingRateCaptureStrategy()._estimate_funding_rate(bars), 0.0001)

    def test_funding_normal(self):
        bars = self._bars(6)
        r = FundingRateCaptureStrategy()._estimate_funding_rate(bars)
        self.assertTrue(-0.001 <= r <= 0.001)

    def test_vol_regime_short(self):
        self.assertEqual(FundingRateCaptureStrategy()._detect_vol_regime([1] * 10, [1] * 10, [1] * 10), "normal")

    def test_vol_regime_high(self):
        s = FundingRateCaptureStrategy()
        s._estimate_atr = lambda c, h, l, period=14: 5.0 if len(c) <= 10 else 1.0
        self.assertEqual(s._detect_vol_regime([1] * 20, [1] * 20, [1] * 20), "high")

    def test_vol_regime_low(self):
        s = FundingRateCaptureStrategy()
        s._estimate_atr = lambda c, h, l, period=14: 0.2 if len(c) <= 10 else 1.0
        self.assertEqual(s._detect_vol_regime([1] * 20, [1] * 20, [1] * 20), "low")

    def test_vol_regime_normal(self):
        s = FundingRateCaptureStrategy()
        s._estimate_atr = lambda c, h, l, period=14: 1.0
        self.assertEqual(s._detect_vol_regime([1] * 20, [1] * 20, [1] * 20), "normal")

    def test_trend_short(self):
        self.assertEqual(FundingRateCaptureStrategy()._detect_trend([1] * 10), "neutral")

    def test_trend_uptrend(self):
        s = FundingRateCaptureStrategy()
        closes = [100 + i for i in range(60)]
        self.assertEqual(s._detect_trend(closes), "uptrend")

    def test_trend_downtrend(self):
        s = FundingRateCaptureStrategy()
        closes = [200 - i for i in range(60)]
        self.assertEqual(s._detect_trend(closes), "downtrend")

    def test_trend_neutral(self):
        s = FundingRateCaptureStrategy()
        closes = [100.0] * 60
        self.assertEqual(s._detect_trend(closes), "neutral")

    def test_atr_short(self):
        self.assertEqual(FundingRateCaptureStrategy._estimate_atr([1, 2], [1, 2], [1, 2]), 0.0)

    def test_atr_normal(self):
        closes = [100 + i for i in range(20)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        self.assertGreater(FundingRateCaptureStrategy._estimate_atr(closes, highs, lows), 0.0)


if __name__ == "__main__":
    unittest.main()
