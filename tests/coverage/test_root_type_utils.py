from __future__ import annotations

import datetime
from decimal import Decimal
from unittest import TestCase

from trading_system.type_defs import (
    Candle,
    OHLCV,
    Position,
    TradingSignal,
    parse_candle_data,
    parse_signal_data,
)
from trading_system import utils as U


class TestTypeDefs(TestCase):
    def test_candle_repr(self):
        c = Candle(timestamp=datetime.datetime(2024, 1, 1), open=100.5, high=101.2, low=99.8, close=101.0)
        self.assertIn("101.00", repr(c))

    def test_ohlcv_default_volume(self):
        o = OHLCV(timestamp=datetime.datetime(2024, 1, 1), open=1, high=2, low=0, close=1)
        self.assertIsNone(o.volume)

    def test_trading_signal_signal_code(self):
        self.assertEqual(TradingSignal(type="BUY").signal_code, 1)
        self.assertEqual(TradingSignal(type="SELL").signal_code, -1)
        self.assertEqual(TradingSignal(type="HOLD").signal_code, 0)
        self.assertEqual(TradingSignal(type="OTHER").signal_code, 0)

    def test_trading_signal_from_code(self):
        self.assertEqual(TradingSignal.from_code(1).type, "BUY")
        self.assertEqual(TradingSignal.from_code(-1).type, "SELL")
        self.assertEqual(TradingSignal.from_code(0).type, "HOLD")

    def test_position(self):
        p = Position(symbol="BTC/USDT", size=0.5, entry_price=45000.0)
        self.assertIsNone(p.unrealized_pnl)

    def test_parse_candle_candle_input(self):
        c = Candle(timestamp=datetime.datetime(2024, 1, 1), open=1, high=2, low=0, close=1)
        self.assertIs(parse_candle_data(c), c)

    def test_parse_candle_dict(self):
        c = parse_candle_data({"timestamp": "2024-01-01T00:00:00Z", "open": 1, "high": 2, "low": 0, "close": 1})
        self.assertEqual(c.open, 1.0)
        self.assertIsInstance(c.timestamp, datetime.datetime)

    def test_parse_candle_dict_alt_timestamp_field(self):
        c = parse_candle_data({"time": "2024-01-01T00:00:00Z", "open": 1, "high": 2, "low": 0, "close": 1},
                              timestamp_field="time")
        self.assertIsInstance(c.timestamp, datetime.datetime)

    def test_parse_candle_dict_bad_timestamp(self):
        c = parse_candle_data({"timestamp": "not-a-date", "open": 1, "high": 2, "low": 0, "close": 1})
        self.assertIsInstance(c.timestamp, datetime.datetime)

    def test_parse_candle_dict_missing_timestamp(self):
        c = parse_candle_data({"open": 1, "high": 2, "low": 0, "close": 1})
        self.assertIsNone(c.timestamp)

    def test_parse_candle_unsupported(self):
        with self.assertRaises(ValueError):
            parse_candle_data(123)

    def test_parse_signal_signal_input(self):
        s = TradingSignal(type="BUY")
        self.assertIs(parse_signal_data(s), s)

    def test_parse_signal_dict(self):
        s = parse_signal_data({"type": "SELL", "strength": 0.5, "reason": "r"})
        self.assertEqual(s.type, "SELL")
        self.assertEqual(s.strength, 0.5)
        self.assertEqual(s.reason, "r")

    def test_parse_signal_dict_defaults(self):
        s = parse_signal_data({"signal": "BUY"})
        self.assertEqual(s.type, "BUY")

    def test_parse_signal_unsupported(self):
        with self.assertRaises(ValueError):
            parse_signal_data(123)


class TestUtils(TestCase):
    def _candles(self, n=10, base=100.0):
        return [{"close": base + i} for i in range(n)]

    def test_sma_insufficient_empty(self):
        @U.sma(5)
        def f(candles, index):
            return None
        self.assertIsNone(f([], -1))

    def test_sma_insufficient_short(self):
        @U.sma(5)
        def f(candles, index):
            return None
        self.assertIsNone(f(self._candles(3), -1))

    def test_sma_sufficient_negative_index(self):
        @U.sma(3)
        def f(candles, index):
            return None
        # NOTE: sma wrapper ignores `index` and always uses the first n candles.
        val = f(self._candles(10, 100.0), -1)
        self.assertAlmostEqual(val, 101.0)

    def test_sma_sufficient_positive_index(self):
        @U.sma(3)
        def f(candles, index):
            return None
        val = f(self._candles(10, 100.0), 4)
        self.assertAlmostEqual(val, 101.0)

    def test_ema_insufficient(self):
        @U.ema(5)
        def f(candles, index):
            return None
        self.assertIsNone(f(self._candles(3), -1))

    def test_ema_negative_index_returns_initial(self):
        @U.ema(3)
        def f(candles, index):
            return None
        val = f(self._candles(10, 100.0), -1)
        self.assertAlmostEqual(val, 101.0)  # SMA of first 3

    def test_ema_positive_index_forward(self):
        @U.ema(3)
        def f(candles, index):
            return None
        val = f(self._candles(10, 100.0), 9)
        self.assertGreater(val, 100.0)

    def test_ema_skips_falsy_candle(self):
        @U.ema(3)
        def f(candles, index):
            return None
        candles = [{"close": 100.0}, {"close": 101.0}, {"close": 102.0}, None, {"close": 104.0}]
        val = f(candles, 4)
        self.assertIsInstance(val, float)

    def test_sma_class(self):
        s = U.SMA(3)
        val = s(self._candles(10, 100.0), -1)
        self.assertAlmostEqual(val, 101.0)

    def test_ema_class(self):
        e = U.EMA(3)
        val = e(self._candles(10, 100.0), -1)
        self.assertAlmostEqual(val, 101.0)

    def test_make_ma(self):
        self.assertIsInstance(U.make_ma("EMA", 5), U.EMA)
        self.assertIsInstance(U.make_ma("SMA", 5), U.SMA)
        self.assertIsInstance(U.make_ma("UNKNOWN", 5), U.SMA)

    def test_create_callable_ma(self):
        self.assertTrue(callable(U.create_callable_ma("EMA", 5)))
        self.assertTrue(callable(U.create_callable_ma("SMA", 5)))
        self.assertTrue(callable(U.create_callable_ma("UNKNOWN", 5)))

    def test_module_level_ma(self):
        self.assertIsNotNone(U.SMA5)
        self.assertIsNotNone(U.EMA60)

    def test_partial(self):
        def add(a, b):
            return a + b
        p = U.partial(add, 1)
        self.assertEqual(p(2), 3)


if __name__ == "__main__":
    import unittest

    unittest.main()
