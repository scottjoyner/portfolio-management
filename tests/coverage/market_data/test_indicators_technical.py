import unittest

from trading_system.market_data.indicators.technical import TechnicalIndicatorSet


class TestIndicators(unittest.TestCase):
    def _fill(self, ti, prices, volumes=None):
        volumes = volumes or [1.0] * len(prices)
        for p, v in zip(prices, volumes):
            ti.ingest(p, v)

    def test_ingest_trim(self):
        ti = TechnicalIndicatorSet(max_samples=10)
        for i in range(20):
            ti.ingest(100.0 + i, 1.0)
        self.assertEqual(len(ti._prices), 10)

    def test_sma_short(self):
        ti = TechnicalIndicatorSet()
        self.assertEqual(ti.sma(20), 0.0)

    def test_sma_normal(self):
        ti = TechnicalIndicatorSet()
        self._fill(ti, [100.0, 102.0, 104.0, 106.0])
        self.assertEqual(ti.sma(4), 103.0)

    def test_ema_short(self):
        ti = TechnicalIndicatorSet()
        self._fill(ti, [100.0, 102.0])
        # len < period -> falls back to sma(period) which is also < period -> 0.0
        self.assertEqual(ti.ema(20), 0.0)

    def test_ema_normal(self):
        ti = TechnicalIndicatorSet()
        self._fill(ti, [100.0, 102.0, 104.0, 106.0, 108.0])
        r = ti.ema(3)
        self.assertGreater(r, 0.0)

    def test_rsi_short(self):
        ti = TechnicalIndicatorSet()
        self.assertEqual(ti.rsi(14), 50.0)

    def test_rsi_no_loss(self):
        ti = TechnicalIndicatorSet()
        self._fill(ti, [100.0 + i for i in range(20)])
        self.assertEqual(ti.rsi(14), 100.0)

    def test_rsi_normal(self):
        ti = TechnicalIndicatorSet()
        prices = []
        p = 100.0
        for i in range(20):
            p += 2.0 if i % 2 == 0 else -1.0
            prices.append(p)
        self._fill(ti, prices)
        r = ti.rsi(14)
        self.assertGreaterEqual(r, 0.0)
        self.assertLessEqual(r, 100.0)

    def test_stddev_short(self):
        ti = TechnicalIndicatorSet()
        self.assertEqual(ti._stddev(20), 0.0)

    def test_stddev_normal(self):
        ti = TechnicalIndicatorSet()
        self._fill(ti, [100.0, 102.0, 98.0, 104.0])
        self.assertGreater(ti._stddev(4), 0.0)

    def test_bollinger(self):
        ti = TechnicalIndicatorSet()
        self._fill(ti, [100.0, 102.0, 98.0, 104.0, 101.0])
        b = ti.bollinger_bands(period=5, num_std=2.0)
        self.assertGreater(b["upper"], b["mid"])
        self.assertLess(b["lower"], b["mid"])

    def test_zscore_short(self):
        ti = TechnicalIndicatorSet()
        self.assertEqual(ti.zscore(20), 0.0)

    def test_zscore_normal(self):
        ti = TechnicalIndicatorSet()
        self._fill(ti, [100.0, 102.0, 98.0, 104.0, 101.0])
        z = ti.zscore(5)
        self.assertIsInstance(z, float)

    def test_zscore_zero_std(self):
        ti = TechnicalIndicatorSet()
        self._fill(ti, [100.0, 100.0, 100.0, 100.0, 100.0])
        self.assertEqual(ti.zscore(5), 0.0)

    def test_volume_sma_short(self):
        ti = TechnicalIndicatorSet()
        self.assertEqual(ti.volume_sma(20), 0.0)

    def test_volume_sma_normal(self):
        ti = TechnicalIndicatorSet()
        self._fill(ti, [100.0] * 5, volumes=[2.0, 4.0, 6.0, 8.0, 10.0])
        self.assertEqual(ti.volume_sma(5), 6.0)

    def test_volume_ratio_zero_avg(self):
        ti = TechnicalIndicatorSet()
        self._fill(ti, [100.0] * 3, volumes=[0.0, 0.0, 0.0])
        self.assertEqual(ti.volume_ratio(period=3), 1.0)

    def test_volume_ratio_normal(self):
        ti = TechnicalIndicatorSet()
        self._fill(ti, [100.0] * 5, volumes=[1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertAlmostEqual(ti.volume_ratio(period=5), 5.0 / 3.0)


if __name__ == "__main__":
    unittest.main()
