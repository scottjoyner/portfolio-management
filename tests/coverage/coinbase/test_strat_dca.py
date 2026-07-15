import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from _strat_helpers import bars_from

from coinbase.src.protocols import Direction
from coinbase.src.strat_dca import DCAAccumulationStrategy


class TestDCA(unittest.TestCase):
    def _hist(self, n=51, base=100.0):
        closes = [base + i * 0.1 for i in range(n)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        vols = [1000.0] * n
        return bars_from(closes, highs=highs, lows=lows, vols=vols)

    def test_insufficient_history(self):
        s = DCAAccumulationStrategy()
        bars = self._hist(40)
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_atr_zero(self):
        s = DCAAccumulationStrategy()
        closes = [100.0] * 51
        bars = bars_from(closes, highs=closes, lows=closes)
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_interval_guard(self):
        s = DCAAccumulationStrategy()
        bars = self._hist()
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def _buyable(self, s):
        s._bars_since_last_buy = 200
        s._last_buy_price = None

    def test_vol_boost(self):
        s = DCAAccumulationStrategy()
        bars = self._hist()
        self._buyable(s)
        s._volatility_ratio = lambda closes: 2.0
        s._price_drop_from_peak = lambda closes: 0.0
        s._below_sma = lambda closes, p: False
        setup = s.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(setup)
        self.assertAlmostEqual(setup.metadata["boost"], 2.0)

    def test_price_drop(self):
        s = DCAAccumulationStrategy()
        bars = self._hist()
        self._buyable(s)
        s._volatility_ratio = lambda closes: 1.0
        s._price_drop_from_peak = lambda closes: -0.1
        s._below_sma = lambda closes, p: False
        setup = s.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(setup)
        self.assertGreater(setup.metadata["boost"], 1.0)

    def test_below_sma(self):
        s = DCAAccumulationStrategy()
        bars = self._hist()
        self._buyable(s)
        s._volatility_ratio = lambda closes: 1.0
        s._price_drop_from_peak = lambda closes: 0.0
        s._below_sma = lambda closes, p: True
        setup = s.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(setup)
        self.assertAlmostEqual(setup.metadata["boost"], 1.5)

    def test_vol_low(self):
        s = DCAAccumulationStrategy()
        bars = self._hist()
        # shrink last bar volume
        bars[-1] = bars_from([99.95], highs=[100.95], lows=[98.95],
                             opens=[100.0], vols=[10.0])[0]
        self._buyable(s)
        s._volatility_ratio = lambda closes: 1.0
        s._price_drop_from_peak = lambda closes: 0.0
        s._below_sma = lambda closes, p: False
        setup = s.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(setup)
        self.assertLess(setup.metadata["boost"], 1.5)

    def test_boost_clamped(self):
        s = DCAAccumulationStrategy()
        bars = self._hist()
        self._buyable(s)
        s._volatility_ratio = lambda closes: 10.0
        s._price_drop_from_peak = lambda closes: 0.0
        s._below_sma = lambda closes, p: False
        setup = s.on_bar(bars[-1], bars[:-1])
        self.assertIsNotNone(setup)
        self.assertAlmostEqual(setup.metadata["boost"], 4.0)

    def test_held_unfinished(self):
        s = DCAAccumulationStrategy()
        bars = self._hist()
        s._bars_since_last_buy = 200
        s._total_bars = 500
        s._last_buy_price = 1.0
        s._last_buy_bar = 400  # held 100 < min_hold 200
        s._volatility_ratio = lambda closes: 1.0
        s._price_drop_from_peak = lambda closes: 0.0
        s._below_sma = lambda closes, p: False
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))


class TestDCAHelpers(unittest.TestCase):
    def test_volatility_ratio_short(self):
        self.assertEqual(DCAAccumulationStrategy()._volatility_ratio([1, 2]), 0.0)

    def test_volatility_ratio_normal(self):
        closes = [100 + i for i in range(60)]
        r = DCAAccumulationStrategy()._volatility_ratio(closes)
        self.assertGreater(r, 0.0)

    def test_volatility_ratio_flat(self):
        closes = [100.0] * 60
        self.assertEqual(DCAAccumulationStrategy()._volatility_ratio(closes), 0.0)

    def test_price_drop_short(self):
        self.assertEqual(DCAAccumulationStrategy()._price_drop_from_peak([1, 2]), 0.0)

    def test_price_drop_normal(self):
        closes = [100 + i for i in range(60)]
        r = DCAAccumulationStrategy()._price_drop_from_peak(closes)
        self.assertAlmostEqual(r, 0.0)

    def test_price_drop_drop(self):
        closes = [100.0] * 49 + [80.0]
        r = DCAAccumulationStrategy()._price_drop_from_peak(closes)
        self.assertLess(r, 0.0)

    def test_below_sma_short(self):
        self.assertFalse(DCAAccumulationStrategy()._below_sma([1, 2, 3], 200))

    def test_below_sma_true(self):
        closes = [100.0] * 199 + [90.0]
        self.assertTrue(DCAAccumulationStrategy()._below_sma(closes, 200))

    def test_below_sma_false(self):
        closes = [i for i in range(200)]
        self.assertFalse(DCAAccumulationStrategy()._below_sma(closes, 200))

    def test_atr_short(self):
        self.assertEqual(DCAAccumulationStrategy._estimate_atr([1, 2], [1, 2], [1, 2]), 0.0)

    def test_atr_normal(self):
        closes = [100 + i for i in range(20)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        self.assertGreater(DCAAccumulationStrategy._estimate_atr(closes, highs, lows), 0.0)


if __name__ == "__main__":
    unittest.main()
