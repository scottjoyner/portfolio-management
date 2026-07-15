import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from _strat_helpers import bars_from

from coinbase.src.protocols import Direction
from coinbase.src.strat_momaccel import MomentumAccelerationStrategy


def make_strat(**kw):
    return MomentumAccelerationStrategy(**kw)


class TestMomAccel(unittest.TestCase):
    def _hist(self, n=74):
        closes = [100 + i * 0.1 for i in range(n + 1)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        vols = [1000.0] * len(closes)
        return bars_from(closes, highs=highs, lows=lows, vols=vols)

    def _run(self, mom, accel, vol_conf=False, rsi=50.0, atr=1.0):
        s = make_strat()
        s._compute_momentum = lambda closes, p: mom
        s._compute_acceleration = lambda m, p: accel
        s._volume_confirmation = lambda vols, p: vol_conf
        s._rsi = lambda closes, p: rsi
        s._estimate_atr = lambda *a, **k: atr
        bars = self._hist()
        return s.on_bar(bars[-1], bars[:-1])

    def test_insufficient_history(self):
        s = make_strat()
        bars = self._hist(10)
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_atr_zero(self):
        s = make_strat()
        s._compute_momentum = lambda c, p: [0, 0, 0, 0, 0.06]
        s._compute_acceleration = lambda m, p: [0, 0, 0, 0.001, 0.02]
        s._estimate_atr = lambda *a, **k: 0.0
        bars = self._hist()
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_momentum_too_short(self):
        self.assertIsNone(self._run([0, 0, 0, 0], [0, 0, 0, 0.02]))

    def test_accel_too_short(self):
        self.assertIsNone(self._run([0, 0, 0, 0, 0.06], [0, 0]))

    def test_bullish_long(self):
        setup = self._run([0, 0, 0, 0, 0, 0.06], [0, 0, 0, 0.001, 0.02],
                          vol_conf=False, rsi=50.0)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_bullish_long_vol_confirm(self):
        setup = self._run([0, 0, 0, 0, 0, 0.06], [0, 0, 0, 0.001, 0.02],
                          vol_conf=True, rsi=50.0)
        self.assertAlmostEqual(setup.confidence, 0.75)

    def test_bullish_long_overbought(self):
        setup = self._run([0, 0, 0, 0, 0, 0.06], [0, 0, 0, 0.001, 0.02],
                          vol_conf=False, rsi=75.0)
        self.assertLess(setup.confidence, 0.65)

    def test_bullish_prev_accel_high(self):
        self.assertIsNone(self._run([0, 0, 0, 0, 0, 0.06], [0, 0, 0, 0.01, 0.02]))

    def test_bearish_short(self):
        setup = self._run([0, 0, 0, 0, 0, -0.06], [0, 0, 0, -0.002, -0.02],
                          vol_conf=False, rsi=50.0)
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_bearish_short_oversold(self):
        setup = self._run([0, 0, 0, 0, 0, -0.06], [0, 0, 0, -0.002, -0.02],
                          vol_conf=False, rsi=25.0)
        self.assertLess(setup.confidence, 0.65)

    def test_bearish_prev_accel_low(self):
        self.assertIsNone(self._run([0, 0, 0, 0, 0, -0.06], [0, 0, 0, -0.01, -0.02]))

    def test_bearish_divergence(self):
        setup = self._run([0, 0, 0, 0, 0, 0.06], [0, 0, 0, -0.002, -0.02])
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.SHORT)
        self.assertAlmostEqual(setup.confidence, 0.5)

    def test_bearish_divergence_prev_low(self):
        self.assertIsNone(self._run([0, 0, 0, 0, 0, 0.06], [0, 0, 0, -0.01, -0.02]))

    def test_bullish_divergence(self):
        setup = self._run([0, 0, 0, 0, 0, -0.06], [0, 0, 0, 0.001, 0.02])
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.LONG)
        self.assertAlmostEqual(setup.confidence, 0.5)

    def test_bullish_divergence_prev_high(self):
        self.assertIsNone(self._run([0, 0, 0, 0, 0, -0.06], [0, 0, 0, 0.01, 0.02]))

    def test_no_signal(self):
        self.assertIsNone(self._run([0, 0, 0, 0, 0.005], [0, 0, 0.002]))


class TestMomAccelHelpers(unittest.TestCase):
    def test_compute_momentum_short(self):
        self.assertEqual(MomentumAccelerationStrategy()._compute_momentum([1, 2], 5), [])

    def test_compute_momentum_normal(self):
        closes = [100 + i for i in range(20)]
        m = MomentumAccelerationStrategy()._compute_momentum(closes, 14)
        self.assertEqual(len(m), 6)

    def test_compute_acceleration_short(self):
        self.assertEqual(MomentumAccelerationStrategy()._compute_acceleration([1, 2], 5), [])

    def test_compute_acceleration_normal(self):
        mom = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]
        a = MomentumAccelerationStrategy()._compute_acceleration(mom, 3)
        self.assertEqual(len(a), 3)

    def test_volume_confirmation_short(self):
        self.assertFalse(MomentumAccelerationStrategy()._volume_confirmation([1, 2], 5))

    def test_volume_confirmation_true(self):
        vols = [100.0] * 11 + [200.0]
        self.assertTrue(MomentumAccelerationStrategy()._volume_confirmation(vols, 10))

    def test_volume_confirmation_false(self):
        vols = [100.0] * 12
        self.assertFalse(MomentumAccelerationStrategy()._volume_confirmation(vols, 10))

    def test_rsi_short(self):
        self.assertEqual(MomentumAccelerationStrategy()._rsi([1, 2]), 50.0)

    def test_rsi_gains(self):
        closes = [100 + i for i in range(15)]
        self.assertEqual(MomentumAccelerationStrategy()._rsi(closes, 14), 100.0)

    def test_rsi_normal(self):
        closes = [100, 102, 101, 103, 100, 98, 101, 104, 99, 97,
                  100, 102, 103, 101, 100]
        r = MomentumAccelerationStrategy()._rsi(closes, 14)
        self.assertTrue(0.0 < r < 100.0)

    def test_atr_short(self):
        self.assertEqual(MomentumAccelerationStrategy._estimate_atr([1, 2], [1, 2], [1, 2]), 0.0)

    def test_atr_normal(self):
        closes = [100 + i for i in range(20)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        self.assertGreater(MomentumAccelerationStrategy._estimate_atr(closes, highs, lows), 0.0)


if __name__ == "__main__":
    unittest.main()
