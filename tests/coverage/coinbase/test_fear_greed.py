import unittest
import unittest.mock
from coinbase.src.fear_greed import (
    FearGreedIndex, FearGreedSignalAdapter, FearGreedSnapshot, CLASSIFICATION_RANGES,
)
from coinbase.src.protocols import Direction, Bar, BracketSetup


def make_bar(close, high=None, low=None, volume=1000.0):
    high = high if high is not None else close + 1
    low = low if low is not None else close - 1
    return Bar(timestamp=1.0, open=close, high=high, low=low, close=close, volume=volume)


def rising_closes(n, start=100.0, step=0.5):
    return [start + i * step for i in range(n)]


def closes_from_returns(std, n=40, mean_ret=0.01):
    closes = [100.0]
    for i in range(n):
        r = mean_ret + (std if i % 2 == 0 else -std)
        closes.append(closes[-1] * (1 + r))
    return closes


class TestClassify(unittest.TestCase):
    def test_classify_ranges(self):
        self.assertEqual(FearGreedIndex._classify(10), "extreme_fear")
        self.assertEqual(FearGreedIndex._classify(30), "fear")
        self.assertEqual(FearGreedIndex._classify(50), "neutral")
        self.assertEqual(FearGreedIndex._classify(65), "greed")
        self.assertEqual(FearGreedIndex._classify(90), "extreme_greed")
        self.assertEqual(FearGreedIndex._classify(99.9), "extreme_greed")
        self.assertEqual(FearGreedIndex._classify(100), "extreme_greed")
        self.assertEqual(FearGreedIndex._classify(200), "extreme_greed")

    def test_ranges_constant(self):
        self.assertEqual(len(CLASSIFICATION_RANGES), 5)


class TestFearGreedIndex(unittest.TestCase):
    def test_compute_short_data(self):
        fg = FearGreedIndex()
        snap = fg.compute({"BTC-USD": [100.0, 101.0]})
        self.assertEqual(snap.value, 50.0)

    def test_compute_no_volumes(self):
        fg = FearGreedIndex()
        closes = rising_closes(40)
        snap = fg.compute({"BTC-USD": closes})
        self.assertGreaterEqual(snap.value, 0)
        self.assertLessEqual(snap.value, 100)

    def test_compute_with_volumes(self):
        fg = FearGreedIndex()
        closes = rising_closes(40)
        vols = [1000.0] * 40
        snap = fg.compute({"BTC-USD": closes}, {"BTC-USD": vols})
        self.assertIn("volume_component", snap.__dict__)

    def test_compute_cache_hit(self):
        fg = FearGreedIndex()
        closes = rising_closes(40)
        s1 = fg.compute({"BTC-USD": closes})
        s2 = fg.compute({"BTC-USD": closes})
        self.assertIs(s1, s2)

    def test_get_value(self):
        fg = FearGreedIndex()
        closes = rising_closes(40)
        self.assertGreaterEqual(fg.get_value({"BTC-USD": closes}), 0)

    def test_momentum_score(self):
        fg = FearGreedIndex()
        self.assertEqual(fg._momentum_score([1.0, 2.0]), 50.0)  # too short
        closes = rising_closes(40)
        self.assertGreater(fg._momentum_score(closes), 50.0)

    def test_volatility_score_thresholds(self):
        fg = FearGreedIndex()
        self.assertEqual(fg._volatility_score([1.0] * 10), 50.0)  # too short
        self.assertEqual(fg._volatility_score(closes_from_returns(0.005)), 70.0)
        self.assertEqual(fg._volatility_score(closes_from_returns(0.015)), 60.0)
        self.assertEqual(fg._volatility_score(closes_from_returns(0.03)), 50.0)
        self.assertEqual(fg._volatility_score(closes_from_returns(0.06)), 35.0)
        self.assertEqual(fg._volatility_score(closes_from_returns(0.1)), 20.0)

    def test_volume_volume_score(self):
        fg = FearGreedIndex()
        self.assertEqual(fg._volume_volume_score([1.0] * 40, None), 50.0)
        self.assertEqual(fg._volume_volume_score([1.0] * 10, [1.0] * 10), 50.0)
        closes = rising_closes(40)
        vols_up = [1000.0] * 30 + [3000.0] * 10
        self.assertEqual(fg._volume_volume_score(closes, vols_up), 75.0)
        closes_down = [200.0 - i for i in range(40)]
        self.assertEqual(fg._volume_volume_score(closes_down, vols_up), 25.0)
        vols_mod = [1000.0] * 40
        self.assertIn(fg._volume_volume_score(closes, vols_mod), (50, 60, 40))

    def test_breadth_score(self):
        fg = FearGreedIndex()
        self.assertEqual(fg._breadth_score({"BTC-USD": [1.0]}), 50.0)
        closes = {"BTC-USD": rising_closes(40), "ETH-USD": rising_closes(40, start=50)}
        self.assertGreater(fg._breadth_score(closes), 50.0)
        down = {"BTC-USD": [200.0 - i for i in range(40)], "ETH-USD": [200.0 - i for i in range(40)]}
        self.assertLess(fg._breadth_score(down), 50.0)
        # a product with fewer than 5 closes is skipped in the breadth loop
        mixed = {"BTC-USD": rising_closes(40), "SHORT": [1.0, 2.0, 3.0]}
        self.assertEqual(fg._breadth_score(mixed), 50.0)

    def test_volume_volume_score_hist_zero(self):
        fg = FearGreedIndex()
        # hist_avg <= 0 branch
        self.assertEqual(fg._volume_volume_score(rising_closes(40), [0.0] * 20), 50.0)

    def test_volume_volume_score_short_closes(self):
        fg = FearGreedIndex()
        # volumes long enough but closes < 5 -> early return 50
        self.assertEqual(fg._volume_volume_score([1.0, 2.0, 3.0], [1.0] * 20), 50.0)

    def test_volume_volume_score_down_ratio(self):
        fg = FearGreedIndex()
        # ratio > 1.2 but price_dir <= 0 -> returns 40.0 (else branch of line 139)
        closes_down = [200.0 - i for i in range(40)]
        vols = [1000.0] * 30 + [1500.0] * 10
        self.assertEqual(fg._volume_volume_score(closes_down, vols), 40.0)


class TestFearGreedSignalAdapter(unittest.TestCase):
    def setUp(self):
        self.adapter = FearGreedSignalAdapter()
        self.assertEqual(self.adapter.name(), "fear_greed")

    def _run(self, classification, value):
        snap = FearGreedSnapshot(value=value, classification=classification)
        self.adapter.fg = unittest.mock.MagicMock()
        self.adapter.fg.compute.return_value = snap
        self.adapter.set_product_id("BTC-USD")
        bars = [make_bar(100.0 + i) for i in range(40)]
        result = None
        for b in bars:
            result = self.adapter.on_bar(b, bars[:-1])
        return result

    def test_no_pid(self):
        bars = [make_bar(100.0 + i) for i in range(40)]
        self.assertIsNone(self.adapter.on_bar(bars[-1], bars[:-1]))

    def test_extreme_fear(self):
        res = self._run("extreme_fear", 10)
        self.assertIsNotNone(res)
        self.assertEqual(res.direction, Direction.LONG)

    def test_fear(self):
        res = self._run("fear", 30)
        self.assertEqual(res.direction, Direction.LONG)

    def test_extreme_greed(self):
        res = self._run("extreme_greed", 90)
        self.assertEqual(res.direction, Direction.SHORT)

    def test_greed(self):
        res = self._run("greed", 70)
        self.assertEqual(res.direction, Direction.SHORT)

    def test_neutral_none(self):
        res = self._run("neutral", 50)
        self.assertIsNone(res)

    def test_zero_atr(self):
        snap = FearGreedSnapshot(value=10, classification="extreme_fear")
        self.adapter.fg = unittest.mock.MagicMock()
        self.adapter.fg.compute.return_value = snap
        self.adapter.set_product_id("BTC-USD")
        bars = [make_bar(100.0) for _ in range(40)]  # flat -> atr 0
        result = None
        for b in bars:
            result = self.adapter.on_bar(b, [])
        self.assertIsNone(result)

    def test_atr_helper(self):
        self.assertEqual(FearGreedSignalAdapter._estimate_atr([1, 2], [2, 3], [1, 2]), 0.0)

    def test_history_trim_above_100(self):
        snap = FearGreedSnapshot(value=10, classification="extreme_fear")
        self.adapter.fg = unittest.mock.MagicMock()
        self.adapter.fg.compute.return_value = snap
        self.adapter.set_product_id("BTC-USD")
        # feed > 100 bars to exercise the trimming branch
        bars = [make_bar(100.0 + i * 0.1) for i in range(120)]
        result = None
        for b in bars:
            result = self.adapter.on_bar(b, bars[:-1])
        self.assertIsNotNone(result)
        self.assertEqual(len(self.adapter._price_history["BTC-USD"]), 100)


if __name__ == "__main__":
    unittest.main()
