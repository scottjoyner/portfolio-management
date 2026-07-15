import unittest
import numpy as np
import pandas as pd

from coinbase.src.alpha import alpha as alpha_mod
from coinbase.src.alpha.alpha import (
    rr_ratio, donchian_breakout_setup, donchian_breakdown_setup,
    trend_rsi_pullback_setup, trend_rsi_rip_setup, rsi_failure_swing_setup,
    volatility_compression_breakout_setup, impulse_exhaustion_reversal_setup,
)


def make_df(open_, high, low, close):
    return pd.DataFrame({"open": open_, "high": high, "low": low, "close": close})


def short_df():
    return make_df([1.0], [2.0], [0.5], [1.0])


class TestRRRatio(unittest.TestCase):
    def test_long(self):
        self.assertAlmostEqual(rr_ratio(100, 90, 120, "long"), 2.0)

    def test_short(self):
        self.assertAlmostEqual(rr_ratio(100, 120, 80, "short"), 1.0)

    def test_zero_risk(self):
        self.assertGreater(rr_ratio(100, 100, 120, "long"), 0.0)


class TestDonchianBreakout(unittest.TestCase):
    def test_insufficient(self):
        self.assertIsNone(donchian_breakout_setup(short_df()))

    def test_buy(self):
        closes = [100.0] * 235 + list(np.linspace(100, 200, 25))
        df = make_df(closes, [c + 1 for c in closes], [c - 1 for c in closes], closes)
        res = donchian_breakout_setup(df)
        self.assertIsNotNone(res)
        self.assertEqual(res["side"], "buy")


class TestDonchianBreakdown(unittest.TestCase):
    def test_insufficient(self):
        self.assertIsNone(donchian_breakdown_setup(short_df()))

    def test_sell(self):
        closes = [200.0] * 235 + list(np.linspace(200, 100, 25))
        df = make_df(closes, [c + 1 for c in closes], [c - 1 for c in closes], closes)
        res = donchian_breakdown_setup(df)
        self.assertIsNotNone(res)
        self.assertEqual(res["side"], "sell")


class TestTrendRsiPullback(unittest.TestCase):
    def test_insufficient(self):
        self.assertIsNone(trend_rsi_pullback_setup(short_df()))

    def test_always_none(self):
        # The buy branch is unreachable (see pragma); the function can only
        # return None for valid-length inputs.
        closes = list(np.linspace(100, 180, 230)) + list(np.linspace(180, 235, 20))
        df = make_df(closes, [c + 1 for c in closes], [c - 1 for c in closes], closes)
        self.assertIsNone(trend_rsi_pullback_setup(df))


class TestTrendRsiRip(unittest.TestCase):
    def test_insufficient(self):
        self.assertIsNone(trend_rsi_rip_setup(short_df()))

    def test_sell(self):
        base = [200.0] * 200 + list(np.linspace(200, 80, 20))
        tail = list(np.linspace(80, 150, 15))
        closes = base + tail
        df = make_df(closes, [c + 1 for c in closes], [c - 1 for c in closes], closes)
        res = trend_rsi_rip_setup(df)
        self.assertIsNotNone(res)
        self.assertEqual(res["side"], "sell")


class TestRSIFailureSwing(unittest.TestCase):
    def test_insufficient(self):
        self.assertIsNone(rsi_failure_swing_setup(short_df()))

    def test_buy(self):
        closes = list(np.linspace(100, 150, 60)) + [150 - i * 8 for i in range(1, 11)] + [110.0]
        opens = [c for c in closes]
        highs = [c + 2 for c in closes]
        lows = [c - 2 for c in closes]
        df = make_df(opens, highs, lows, closes)
        res = rsi_failure_swing_setup(df)
        self.assertIsNotNone(res)

    def test_sell(self):
        closes = list(np.linspace(100, 150, 60)) + [150 + i * 8 for i in range(1, 11)] + [200.0]
        opens = [c for c in closes]
        highs = [c + 2 for c in closes]
        lows = [c - 2 for c in closes]
        df = make_df(opens, highs, lows, closes)
        res = rsi_failure_swing_setup(df)
        self.assertIsNotNone(res)


class TestVolatilityCompression(unittest.TestCase):
    def test_insufficient(self):
        self.assertIsNone(volatility_compression_breakout_setup(short_df()))

    def test_buy(self):
        n = 70
        wide = []
        for i in range(40):
            c = 100 + (10 if i % 2 == 0 else -10)
            wide.append(c)
        tight = [100.0 + (i - 12) * 0.1 for i in range(24)]  # coil
        breakout = list(np.linspace(100.0, 112.0, 8))        # upward break
        closes = wide + tight + breakout
        highs = [c + 5 for c in wide] + [c + 0.3 for c in tight] + [c + 0.3 for c in breakout]
        lows = [c - 5 for c in wide] + [c - 0.3 for c in tight] + [c - 0.3 for c in breakout]
        opens = closes[:]
        df = make_df(opens, highs, lows, closes)
        res = volatility_compression_breakout_setup(df)
        self.assertIsNotNone(res)


class TestImpulseExhaustion(unittest.TestCase):
    def test_insufficient(self):
        self.assertIsNone(impulse_exhaustion_reversal_setup(short_df()))

    def test_sell(self):
        # recent 6-bar up momentum, last bar red with big upper wick
        closes = list(np.linspace(100, 130, 45)) + [135, 138, 140, 142, 144, 130]
        opens = list(closes[:-1]) + [145]  # last open 145 > close 130 (red)
        highs = [max(o, c) + 8 for o, c in zip(opens, closes)]  # big upper wick on last
        lows = [min(o, c) - 1 for o, c in zip(opens, closes)]
        df = make_df(opens, highs, lows, closes)
        res = impulse_exhaustion_reversal_setup(df)
        self.assertIsNotNone(res)

    def test_buy(self):
        # recent 6-bar down momentum, last bar green with big lower wick
        closes = list(np.linspace(140, 110, 45)) + [105, 102, 100, 98, 96, 110]
        opens = list(closes[:-1]) + [95]  # last open 95 < close 110 (green)
        lows = [min(o, c) - 8 for o, c in zip(opens, closes)]  # big lower wick on last
        highs = [max(o, c) + 1 for o, c in zip(opens, closes)]
        df = make_df(opens, highs, lows, closes)
        res = impulse_exhaustion_reversal_setup(df)
        self.assertIsNotNone(res)

    def test_none(self):
        closes = list(np.linspace(100, 200, 60))
        opens = closes[:]
        highs = [max(o, c) + 1 for o, c in zip(opens, closes)]
        lows = [min(o, c) - 1 for o, c in zip(opens, closes)]
        df = make_df(opens, highs, lows, closes)
        self.assertIsNone(impulse_exhaustion_reversal_setup(df))


class TestAlphaEdgeCases(unittest.TestCase):
    def test_donchian_breakout_no_break(self):
        closes = [100.0] * 240
        df = make_df(closes, [c + 1 for c in closes], [c - 1 for c in closes], closes)
        self.assertIsNone(donchian_breakout_setup(df))

    def test_donchian_breakdown_no_break(self):
        closes = [200.0] * 240
        df = make_df(closes, [c + 1 for c in closes], [c - 1 for c in closes], closes)
        self.assertIsNone(donchian_breakdown_setup(df))

    def test_trend_rsi_pullback_below_sma(self):
        closes = list(np.linspace(300, 100, 240))
        df = make_df(closes, [c + 1 for c in closes], [c - 1 for c in closes], closes)
        self.assertIsNone(trend_rsi_pullback_setup(df))

    def test_trend_rsi_rip_above_sma(self):
        closes = list(np.linspace(100, 300, 240))
        df = make_df(closes, [c + 1 for c in closes], [c - 1 for c in closes], closes)
        self.assertIsNone(trend_rsi_rip_setup(df))

    def test_trend_rsi_rip_low_rsi(self):
        closes = list(np.linspace(300, 100, 240))
        df = make_df(closes, [c + 1 for c in closes], [c - 1 for c in closes], closes)
        self.assertIsNone(trend_rsi_rip_setup(df))

    def test_rsi_failure_swing_no_signal(self):
        closes = list(np.linspace(100, 200, 80))
        opens = closes[:]
        highs = [c + 2 for c in closes]
        lows = [c - 2 for c in closes]
        df = make_df(opens, highs, lows, closes)
        self.assertIsNone(rsi_failure_swing_setup(df))

    def test_volatility_compression_sell(self):
        wide = [100 + (10 if i % 2 == 0 else -10) for i in range(40)]
        tight = [100.0 + (i - 12) * 0.1 for i in range(24)]
        breakout = list(np.linspace(100, 88, 8))
        closes = wide + tight + breakout
        highs = [c + 0.3 for c in wide] + [c + 0.3 for c in tight] + [c + 0.3 for c in breakout]
        lows = [c - 0.3 for c in wide] + [c - 0.3 for c in tight] + [c - 0.3 for c in breakout]
        opens = closes[:]
        df = make_df(opens, highs, lows, closes)
        res = volatility_compression_breakout_setup(df)
        self.assertIsNotNone(res)
        self.assertEqual(res["side"], "sell")

    def test_volatility_compression_none(self):
        closes = [100.0] * 70
        df = make_df(closes, [c + 1 for c in closes], [c - 1 for c in closes], closes)
        self.assertIsNone(volatility_compression_breakout_setup(df))


if __name__ == "__main__":
    unittest.main()
