import sys
import types
import unittest
from unittest import mock

from coinbase.src.protocols import (
    Direction, InstrumentType, Bar, BracketSetup, BacktestFill, BacktestPosition,
    BaseStrategy, MarketFillModel, SignalToBracketAdapter, BacktesterToBracketAdapter,
    Opportunity, OpportunityAggregator,
)


def make_bar(close, high=None, low=None, open_=None, volume=1000.0, ts=1.0):
    high = high if high is not None else close
    low = low if low is not None else close
    open_ = open_ if open_ is not None else close
    return Bar(timestamp=ts, open=open_, high=high, low=low, close=close, volume=volume)


class FakeSignal:
    def __init__(self, price=100.0, confidence=0.6, reason="r", strategy="s"):
        self.price = price
        self.confidence = confidence
        self.reason = reason
        self.strategy = strategy


class TestEnumsAndDataclasses(unittest.TestCase):
    def test_enums(self):
        self.assertEqual(Direction.LONG.value, "long")
        self.assertEqual(InstrumentType.SPOT.value, "spot")

    def test_bar(self):
        b = make_bar(10)
        self.assertEqual(b.close, 10)

    def test_bracket_setup_defaults(self):
        bs = BracketSetup(direction=Direction.LONG, entry_price=1, stop_price=0.9,
                          target_price=1.1, risk_reward=2, confidence=0.5,
                          reason="x", strategy_name="y")
        self.assertEqual(bs.leverage, 1.0)
        self.assertEqual(bs.metadata, {})

    def test_backtest_fill_and_position(self):
        bf = BacktestFill(timestamp=1, price=2, size=3, fees=0.1, slippage=0.2, partial=True)
        self.assertTrue(bf.partial)
        bp = BacktestPosition(product_id="BTC-USD", direction=Direction.LONG,
                              entry=bf, size=3, stop_price=1, target_price=2)
        self.assertIsNone(bp.realized_pnl)

    def test_base_strategy_cannot_instantiate(self):
        with self.assertRaises(TypeError):
            BaseStrategy()


class TestMarketFillModel(unittest.TestCase):
    def test_long_wide_spread_partial(self):
        fm = MarketFillModel(fee_bps=5, slippage_bps=1, min_fill_pct=0.95)
        bid, ask = 99.0, 101.0
        fill = fm.fill(Direction.LONG, 100.0, 10.0, bid, ask, 1_000_000.0)
        self.assertTrue(fill.partial)
        self.assertLess(fill.size, 10.0)
        self.assertGreater(fill.price, 100.0)

    def test_long_narrow_spread_full(self):
        fm = MarketFillModel(fee_bps=5, slippage_bps=1, min_fill_pct=0.95)
        bid, ask = 99.9, 100.1
        fill = fm.fill(Direction.LONG, 100.0, 10.0, bid, ask, 1_000_000.0)
        self.assertFalse(fill.partial)
        self.assertEqual(fill.size, 10.0)

    def test_short_fill(self):
        fm = MarketFillModel(fee_bps=5, slippage_bps=1)
        fill = fm.fill(Direction.SHORT, 100.0, 10.0, 99.9, 100.1, 1_000_000.0)
        self.assertLess(fill.price, 100.0)

    def test_zero_volume_no_impact(self):
        fm = MarketFillModel(fee_bps=0, slippage_bps=0)
        fill = fm.fill(Direction.LONG, 100.0, 10.0, 99.9, 100.1, 0.0)
        self.assertEqual(fill.slippage, 0.0)


class TestAdapters(unittest.TestCase):
    def test_signal_to_bracket_long_short(self):
        fake = types.ModuleType("strategy_engine")
        fake.Signal = FakeSignal
        with mock.patch.dict(sys.modules, {"strategy_engine": fake}):
            bar = make_bar(100)
            lng = SignalToBracketAdapter.convert(FakeSignal(price=100), bar, [], atr=2.0, direction=Direction.LONG)
            sht = SignalToBracketAdapter.convert(FakeSignal(price=100), bar, [], atr=2.0, direction=Direction.SHORT)
        self.assertEqual(lng.direction, Direction.LONG)
        self.assertEqual(sht.direction, Direction.SHORT)
        self.assertGreater(lng.target_price, lng.entry_price)

    def test_signal_to_bracket_type_error(self):
        fake = types.ModuleType("strategy_engine")
        fake.Signal = FakeSignal
        with mock.patch.dict(sys.modules, {"strategy_engine": fake}):
            with self.assertRaises(TypeError):
                SignalToBracketAdapter.convert("notsignal", make_bar(100), [], atr=2.0, direction=Direction.LONG)

    def test_backtester_to_bracket(self):
        bar = make_bar(100)
        buy = BacktesterToBracketAdapter.convert(("BUY", 100), bar, [], atr=2.0)
        sell = BacktesterToBracketAdapter.convert(("SELL", 100), bar, [], atr=2.0)
        none = BacktesterToBracketAdapter.convert(("HOLD", 100), bar, [], atr=2.0)
        self.assertEqual(buy.direction, Direction.LONG)
        self.assertEqual(sell.direction, Direction.SHORT)
        self.assertIsNone(none)


class TestOpportunity(unittest.TestCase):
    def _opp(self, direction):
        return Opportunity(product_id="BTC-USD", direction=direction,
                           instrument_type=InstrumentType.SPOT, entry_price=100,
                           stop_price=90, target_price=120, risk_reward=2,
                           confidence=0.5, reason="r", strategy_name="s")

    def test_risk_per_unit(self):
        self.assertAlmostEqual(self._opp(Direction.LONG).risk_per_unit(), 10.0)
        self.assertAlmostEqual(self._opp(Direction.SHORT).risk_per_unit(), 10.0)

    def test_compute_size(self):
        opp = self._opp(Direction.LONG)
        opp.compute_size(equity=10000, risk_per_trade=0.01)
        self.assertGreater(opp.base_size, 0)
        self.assertGreater(opp.quote_size, 0)
        self.assertEqual(opp.total_risk_pct, 0.01)

    def test_compute_size_zero_rpu(self):
        opp = self._opp(Direction.LONG)
        opp.stop_price = opp.entry_price
        opp.compute_size(equity=10000)
        self.assertEqual(opp.base_size, 0.0)


class TestOpportunityAggregator(unittest.TestCase):
    def _setup(self, direction, conf=0.5, rr=2.0):
        return BracketSetup(direction=direction, entry_price=100, stop_price=90,
                            target_price=110, risk_reward=rr, confidence=conf,
                            reason="r", strategy_name="s")

    def test_empty(self):
        agg = OpportunityAggregator("BTC-USD", [], 100, 2)
        self.assertIsNone(agg.best())
        self.assertEqual(agg.all_ranked(), [])

    def test_best_and_all(self):
        setups = [self._setup(Direction.LONG, 0.3, 1.0), self._setup(Direction.LONG, 0.9, 3.0)]
        agg = OpportunityAggregator("BTC-USD", setups, 100, 2)
        best = agg.best()
        self.assertEqual(best.confidence, 0.9)
        self.assertEqual(len(agg.all_ranked()), 2)


if __name__ == "__main__":
    unittest.main()
