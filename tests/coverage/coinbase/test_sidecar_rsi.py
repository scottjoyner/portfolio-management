import unittest
from unittest import mock

from coinbase.src.strategies.sidecar_rsi import SidecarRSICrossStrategy
from coinbase.src.protocols import Bar, Direction, InstrumentType, BracketSetup


def _bar(close, high=None, low=None, o=None, ts=0.0):
    return Bar(timestamp=ts, open=o if o is not None else close,
               high=high if high is not None else close,
               low=low if low is not None else close,
               close=close, volume=1.0)


class TestSidecarRSI(unittest.TestCase):
    def test_name_and_set_product(self):
        s = SidecarRSICrossStrategy(product_id="ETH-USD")
        self.assertEqual(s.name(), "sidecar_rsi_cross")
        s.set_product_id("BTC-USD")
        self.assertEqual(s.product_id, "BTC-USD")

    def test_insufficient_bars(self):
        s = SidecarRSICrossStrategy(min_bars=20)
        hist = [_bar(100 + i) for i in range(5)]
        self.assertIsNone(s.on_bar(_bar(106), hist))

    def test_no_cross(self):
        s = SidecarRSICrossStrategy(product_id="ETH-USD", rsi_period=5,
                                    buy_rsi_cross=30.0, min_bars=10)
        # mostly flat / no cross above 30
        closes = [100, 100, 100, 100, 100, 100, 100, 100, 100, 100]
        hist = [_bar(c) for c in closes[:-1]]
        self.assertIsNone(s.on_bar(_bar(closes[-1]), hist))

    def test_cross_above_generates_setup(self):
        s = SidecarRSICrossStrategy(product_id="ETH-USD", rsi_period=5,
                                    buy_rsi_cross=30.0, min_bars=10,
                                    confidence=0.5)
        # build a downtrend then a sharp recovery so RSI crosses above 30
        closes = [100, 95, 90, 85, 80, 75, 70, 65, 60, 90]
        hist = [_bar(c) for c in closes[:-1]]
        setup = s.on_bar(_bar(closes[-1]), hist)
        self.assertIsNotNone(setup)
        self.assertIsInstance(setup, BracketSetup)
        self.assertEqual(setup.direction, Direction.LONG)
        self.assertEqual(setup.instrument_type, InstrumentType.SPOT)
        self.assertGreater(setup.stop_price, 0)
        self.assertGreater(setup.target_price, setup.entry_price)
        self.assertEqual(setup.metadata["product_id"], "ETH-USD")

    def test_risk_zero_returns_none(self):
        s = SidecarRSICrossStrategy(product_id="ETH-USD", rsi_period=5,
                                    buy_rsi_cross=30.0, min_bars=10,
                                    stop_loss_pct=0.0)
        closes = [100, 95, 90, 85, 80, 75, 70, 65, 60, 90]
        hist = [_bar(c) for c in closes[:-1]]
        self.assertIsNone(s.on_bar(_bar(closes[-1]), hist))

    def test_rsi_too_short(self):
        self.assertEqual(SidecarRSICrossStrategy._rsi([1, 2], period=14), 50.0)

    def test_rsi_all_gains(self):
        # monotonic increase -> losses == 0 -> 100.0
        closes = [100 + i for i in range(20)]
        self.assertEqual(SidecarRSICrossStrategy._rsi(closes, 14), 100.0)

    def test_rsi_normal(self):
        closes = [100, 95, 90, 85, 80, 75, 70, 65, 60, 90]
        val = SidecarRSICrossStrategy._rsi(closes, 5)
        self.assertGreaterEqual(val, 0.0)
        self.assertLessEqual(val, 100.0)

    def test_estimate_atr_short(self):
        self.assertEqual(SidecarRSICrossStrategy._estimate_atr([_bar(100)]), 0.0)

    def test_estimate_atr_normal(self):
        bars = [_bar(100 + i, high=101 + i, low=99 + i) for i in range(15)]
        atr = SidecarRSICrossStrategy._estimate_atr(bars)
        self.assertGreater(atr, 0.0)


if __name__ == "__main__":
    unittest.main()
