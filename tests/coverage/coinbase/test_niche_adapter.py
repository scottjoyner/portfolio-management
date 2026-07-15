import unittest
from unittest import mock

from coinbase.src.backtest import coinbase_niche_strategies as nmod
from coinbase.src.backtest.niche_adapter import (
    NicheStrategyWrapper, _bar_to_ohlcv, ALL_NICHE_STRATEGIES, wrap_all_niche_strategies,
)
from coinbase.src.backtest.coinbase_niche_strategies import (
    MultiTimeframeRSIMomentumStrategy, BollingerSqueezeBreakoutStrategy,
    RegimeAwareAdaptiveStrategy, LiquidityVacuumReversalStrategy,
)
from coinbase.src.protocols import Direction, Bar, BracketSetup


def make_bars(n, price=100.0):
    return [
        Bar(timestamp=float(i), open=price, high=price * 1.01, low=price * 0.99,
            close=price + i, volume=1e9)
        for i in range(n)
    ]


class TestBarToOhlcv(unittest.TestCase):
    def test_conversion(self):
        bars = make_bars(10)
        ohlcv = _bar_to_ohlcv(bars[-1], bars[:-1])
        self.assertEqual(ohlcv.close, bars[-1].close)
        self.assertEqual(len(ohlcv.close_window), 10)
        self.assertEqual(len(ohlcv.volume_window), 10)


class TestNicheStrategyWrapper(unittest.TestCase):
    def test_name(self):
        w = NicheStrategyWrapper(MultiTimeframeRSIMomentumStrategy())
        self.assertEqual(w.name(), "MultiTimeframeRSIMomentumStrategy")

    def test_buy_and_sell(self):
        w = NicheStrategyWrapper(BollingerSqueezeBreakoutStrategy())
        bars = make_bars(40)
        res = w.on_bar(bars[-1], bars[:-1])
        self.assertTrue(res is None or isinstance(res, BracketSetup))

    def test_action_string_other(self):
        class FakeNiche:
            def on_bar(self, bar):
                return "HOLD"
        w = NicheStrategyWrapper(FakeNiche())
        bars = make_bars(40)
        self.assertIsNone(w.on_bar(bars[-1], bars[:-1]))

    def test_wrap_all_skips_broken(self):
        class Broken:
            def __init__(self):
                raise ValueError("boom")
        orig = ALL_NICHE_STRATEGIES
        NicheStrategyWrapper  # ensure import
        try:
            import coinbase.src.backtest.niche_adapter as nadapter
            nadapter.ALL_NICHE_STRATEGIES = [Broken, MultiTimeframeRSIMomentumStrategy]
            wrapped = wrap_all_niche_strategies()
            self.assertEqual(len(wrapped), 1)
            self.assertEqual(wrapped[0].name(), "MultiTimeframeRSIMomentumStrategy")
        finally:
            nadapter.ALL_NICHE_STRATEGIES = orig

    def test_action_attr(self):
        class FakeNiche:
            def on_bar(self, bar):
                return type("S", (), {"action": "SELL"})()
        w = NicheStrategyWrapper(FakeNiche())
        bars = make_bars(40)
        res = w.on_bar(bars[-1], bars[:-1])
        self.assertEqual(res.direction, Direction.SHORT)

    def test_none_signal(self):
        class FakeNiche:
            def on_bar(self, bar):
                return None
        w = NicheStrategyWrapper(FakeNiche())
        bars = make_bars(40)
        self.assertIsNone(w.on_bar(bars[-1], bars[:-1]))

    def test_unknown_signal(self):
        class FakeNiche:
            def on_bar(self, bar):
                return 123  # not a str, no .action
        w = NicheStrategyWrapper(FakeNiche())
        bars = make_bars(40)
        self.assertIsNone(w.on_bar(bars[-1], bars[:-1]))

    def test_size_zero_atr(self):
        # Provide a niche that returns BUY but with too-short history to compute ATR
        class FakeNiche:
            def on_bar(self, bar):
                return "BUY"
        w = NicheStrategyWrapper(FakeNiche())
        bars = [Bar(timestamp=1.0, open=100, high=101, low=99, close=100, volume=1e9)]
        res = w.on_bar(bars[-1], [])
        self.assertIsNotNone(res)
        self.assertEqual(res.atr, 0.0)

    def test_estimate_atr(self):
        closes = [float(i) for i in range(20)]
        highs = [float(i) + 1 for i in range(20)]
        lows = [float(i) - 1 for i in range(20)]
        atr = NicheStrategyWrapper._estimate_atr(closes, highs, lows)
        self.assertGreater(atr, 0.0)

    def test_estimate_atr_short(self):
        atr = NicheStrategyWrapper._estimate_atr([1.0, 2.0], [1.0, 2.0], [1.0, 2.0])
        self.assertEqual(atr, 0.0)


class TestAllNicheStrategies(unittest.TestCase):
    def test_list_contents(self):
        self.assertIn(MultiTimeframeRSIMomentumStrategy, ALL_NICHE_STRATEGIES)
        self.assertIn(RegimeAwareAdaptiveStrategy, ALL_NICHE_STRATEGIES)
        self.assertIn(LiquidityVacuumReversalStrategy, ALL_NICHE_STRATEGIES)

    def test_wrap_all(self):
        wrapped = wrap_all_niche_strategies()
        self.assertGreater(len(wrapped), 0)
        for w in wrapped:
            self.assertTrue(hasattr(w.on_bar, "__call__"))


if __name__ == "__main__":
    unittest.main()
