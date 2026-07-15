import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from _strat_helpers import bars_from

from coinbase.src.protocols import Direction
from coinbase.src.strat_grid import GridTradingStrategy


def make_strat(**kw):
    kw.setdefault("grid_spread_bps", 600)
    return GridTradingStrategy(**kw)


class TestGrid(unittest.TestCase):
    def _hist(self, n=25, close=98.0):
        closes = [close + i * 0.01 for i in range(n)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        vols = [1000.0] * n
        return bars_from(closes, highs=highs, lows=lows, vols=vols)

    def _bar(self, close, vol=1000.0):
        return bars_from([close], highs=[close + 1], lows=[close - 1],
                         vols=[vol])[0]

    def test_name(self):
        self.assertEqual(make_strat().name(), "grid_trade")

    def test_set_product_id(self):
        s = make_strat()
        s.set_product_id("ETH-USD")
        self.assertEqual(s._current_pid, "ETH-USD")

    def test_insufficient_history(self):
        s = make_strat(grid_levels=1)
        bars = self._hist(15)
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_atr_zero(self):
        s = make_strat(grid_levels=1)
        closes = [100.0] * 25
        bars = bars_from(closes, highs=closes, lows=closes)
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_center_initialized(self):
        s = make_strat(grid_levels=1)
        bars = self._hist()
        s._rsi = lambda closes, p=14: 50
        self.assertIsNone(s.on_bar(self._bar(100.0), bars))
        self.assertIsNotNone(s._grid_center)

    def test_floor_buy(self):
        s = make_strat(grid_levels=1)
        bars = self._hist()
        s._grid_center = 100.0
        s._rsi = lambda closes, p=14: 50
        setup = s.on_bar(self._bar(95.5), bars)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_ceil_sell(self):
        s = make_strat(grid_levels=1)
        bars = self._hist()
        s._grid_center = 100.0
        s._rsi = lambda closes, p=14: 50
        setup = s.on_bar(self._bar(104.5), bars)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_rsi_oversold(self):
        s = make_strat(grid_levels=1)
        bars = self._hist()
        s._grid_center = 100.0
        s._rsi = lambda closes, p=14: 20
        setup = s.on_bar(self._bar(100.0), bars)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_rsi_overbought(self):
        s = make_strat(grid_levels=1)
        bars = self._hist()
        s._grid_center = 100.0
        s._rsi = lambda closes, p=14: 80
        setup = s.on_bar(self._bar(100.0), bars)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_vol_spike(self):
        s = make_strat(grid_levels=1)
        bars = self._hist()
        s._grid_center = 100.0
        s._rsi = lambda closes, p=14: 50
        bar = self._bar(100.0, vol=100000.0)
        self.assertIsNone(s.on_bar(bar, bars))

    def test_dist_too_far(self):
        s = make_strat(grid_levels=1)
        bars = self._hist()
        s._grid_center = 200.0
        s._rsi = lambda closes, p=14: 50
        self.assertIsNone(s.on_bar(self._bar(96.0), bars))

    def test_loop_none(self):
        s = make_strat(grid_levels=1)
        bars = self._hist()
        s._grid_center = 100.0
        s._rsi = lambda closes, p=14: 50
        self.assertIsNone(s.on_bar(self._bar(100.0), bars))

    def test_loop_level_long(self):
        s = make_strat(grid_levels=2)
        bars = self._hist()
        s._grid_center = 100.0
        s._rsi = lambda closes, p=14: 50
        setup = s.on_bar(self._bar(94.0), bars)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_loop_level_short(self):
        s = make_strat(grid_levels=2)
        bars = self._hist()
        s._grid_center = 100.0
        s._rsi = lambda closes, p=14: 50
        setup = s.on_bar(self._bar(106.0), bars)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_reset_center(self):
        s = make_strat(grid_levels=1)
        closes = [100.0] * 25
        highs = [101.0] * 25
        lows = [99.0] * 25
        highs[10] = 160.0
        lows[12] = 40.0
        bars = bars_from(closes, highs=highs, lows=lows)
        s._grid_center = 80.0
        s._rsi = lambda closes, p=14: 50
        self.assertIsNone(s.on_bar(self._bar(100.0), bars))
        self.assertNotEqual(s._grid_center, 80.0)


class TestGridHelpers(unittest.TestCase):
    def test_rsi_short(self):
        self.assertEqual(GridTradingStrategy._rsi([1, 2]), 50.0)

    def test_rsi_gains_only(self):
        closes = [100 + i for i in range(15)]
        self.assertEqual(GridTradingStrategy._rsi(closes, 14), 100.0)

    def test_rsi_normal(self):
        closes = [100, 102, 101, 103, 100, 98, 101, 104, 99, 97,
                  100, 102, 103, 101, 100]
        r = GridTradingStrategy._rsi(closes, 14)
        self.assertTrue(0.0 < r < 100.0)

    def test_atr_short(self):
        self.assertEqual(GridTradingStrategy._estimate_atr([1, 2], [1, 2], [1, 2]), 0.0)

    def test_atr_normal(self):
        closes = [100 + i for i in range(20)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        self.assertGreater(GridTradingStrategy._estimate_atr(closes, highs, lows), 0.0)


if __name__ == "__main__":
    unittest.main()
