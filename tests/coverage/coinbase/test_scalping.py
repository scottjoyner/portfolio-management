import unittest
from unittest import mock

from coinbase.src.strategies.scalping import ScalpingStrategy


def _closes_rising():
    return [100 + i for i in range(19)] + [117]  # last is a small pullback from 118


def _closes_flat_tail():
    return [110, 112, 115, 118, 120, 122] + [121] * 14  # last 14 flat, pullback from 122


class TestScalping(unittest.TestCase):
    def test_success_atr_positive(self):
        s = ScalpingStrategy(min_volume_usd=500_000, max_spread_bps=15.0)
        closes = _closes_rising()
        vols = [1000] * 20
        sig = s.get_signals("ETH-USD", 117.0, closes, vols, 1e7, 116.99, 117.01)
        self.assertIsNotNone(sig)
        self.assertEqual(sig["action"], "BUY")
        self.assertEqual(sig["strategy"], "scalping")
        self.assertGreater(sig["stop_price"], 0)
        self.assertGreater(sig["target_price"], 0)

    def test_success_atr_zero_else_branch(self):
        s = ScalpingStrategy(min_volume_usd=500_000, max_spread_bps=15.0)
        closes = _closes_flat_tail()
        vols = [1000] * 20
        sig = s.get_signals("ETH-USD", 121.0, closes, vols, 1e7, 120.99, 121.01)
        self.assertIsNotNone(sig)
        self.assertGreater(sig["atr_14"], 0)  # _estimate_atr still returns 0 here

    def test_cooldown_returns_none(self):
        s = ScalpingStrategy(min_volume_usd=500_000, max_spread_bps=15.0,
                             product_cooldown_s=1e9)
        closes = _closes_rising()
        vols = [1000] * 20
        s.get_signals("ETH-USD", 117.0, closes, vols, 1e7, 116.99, 117.01)
        sig2 = s.get_signals("ETH-USD", 117.0, closes, vols, 1e7, 116.99, 117.01)
        self.assertIsNone(sig2)

    def test_too_few_closes(self):
        s = ScalpingStrategy()
        sig = s.get_signals("ETH-USD", 117.0, [1, 2], [1, 2], 1e7, 116.0, 118.0)
        self.assertIsNone(sig)

    def test_volume_too_low(self):
        s = ScalpingStrategy(min_volume_usd=500_000)
        closes = _closes_rising()
        vols = [1000] * 20
        sig = s.get_signals("ETH-USD", 117.0, closes, vols, 100.0, 116.99, 117.01)
        self.assertIsNone(sig)

    def test_spread_too_wide(self):
        s = ScalpingStrategy(min_volume_usd=500_000, max_spread_bps=15.0)
        closes = _closes_rising()
        vols = [1000] * 20
        sig = s.get_signals("ETH-USD", 117.0, closes, vols, 1e7, 100.0, 102.0)
        self.assertIsNone(sig)

    def test_spread_zero_bid_ask_else_branch(self):
        s = ScalpingStrategy(min_volume_usd=500_000, max_spread_bps=15.0)
        closes = _closes_rising()
        vols = [1000] * 20
        sig = s.get_signals("ETH-USD", 117.0, closes, vols, 1e7, 0.0, 0.0)
        self.assertIsNone(sig)

    def test_pullback_too_small(self):
        s = ScalpingStrategy(min_volume_usd=500_000, max_spread_bps=15.0)
        # flat series -> pullback 0
        closes = [117.0] * 20
        vols = [1000] * 20
        sig = s.get_signals("ETH-USD", 117.0, closes, vols, 1e7, 116.99, 117.01)
        self.assertIsNone(sig)

    def test_pullback_too_large(self):
        s = ScalpingStrategy(min_volume_usd=500_000, max_spread_bps=15.0)
        closes = [100] * 19 + [98]  # 2% drop -> outside range
        vols = [1000] * 20
        sig = s.get_signals("ETH-USD", 98.0, closes, vols, 1e7, 97.99, 98.01)
        self.assertIsNone(sig)

    def test_volume_dry_up(self):
        s = ScalpingStrategy(min_volume_usd=500_000, max_spread_bps=15.0)
        closes = _closes_rising()
        vols = [1000] * 19 + [100]  # last volume < 50% of avg
        sig = s.get_signals("ETH-USD", 117.0, closes, vols, 1e7, 116.99, 117.01)
        self.assertIsNone(sig)

    def test_cooldown_elapsed_proceeds(self):
        s = ScalpingStrategy(min_volume_usd=500_000, max_spread_bps=15.0,
                             product_cooldown_s=10)
        closes = _closes_rising()
        vols = [1000] * 20
        with mock.patch("coinbase.src.strategies.scalping.time.time", return_value=1000.0):
            s.get_signals("ETH-USD", 117.0, closes, vols, 1e7, 116.99, 117.01)
        with mock.patch("coinbase.src.strategies.scalping.time.time", return_value=2000.0):
            sig = s.get_signals("ETH-USD", 117.0, closes, vols, 1e7, 116.99, 117.01)
        self.assertIsNotNone(sig)

    def test_on_exit(self):
        s = ScalpingStrategy()
        s.on_exit("ETH-USD", 5.0)
        self.assertIn("ETH-USD", s._last_signal)

    def test_estimate_atr_short(self):
        self.assertEqual(ScalpingStrategy._estimate_atr([1, 2, 3], period=14), 0.0)

    def test_estimate_atr_normal(self):
        closes = [100 + i for i in range(20)]
        atr = ScalpingStrategy._estimate_atr(closes, period=14)
        self.assertGreater(atr, 0.0)


if __name__ == "__main__":
    unittest.main()
