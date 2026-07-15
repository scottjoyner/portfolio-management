import time
import unittest
from unittest import mock

from coinbase.src.sentiment import macro_risk
from coinbase.src.sentiment.macro_risk import MacroRiskEngine, MacroSignal


def make_fetch(map_):
    def _fetch(ticker, period="5d", interval="1d"):
        return list(map_[ticker])
    return _fetch


class TestMacroSignal(unittest.TestCase):
    def test_to_opportunity(self):
        sig = MacroSignal("SELL", 0.7, 1.2, {"DXY": 105.0}, "MACRO")
        d = sig.to_opportunity()
        self.assertEqual(d["action"], "SELL")
        self.assertIn("DXY", d["components"])


class TestMacroRiskEngine(unittest.TestCase):
    def test_cache_hit(self):
        eng = MacroRiskEngine(cache_ttl=600)
        eng._cached = MacroSignal("HOLD", 0.5, 0.0, {}, "MACRO")
        eng._last_fetch = time.time()
        with mock.patch("coinbase.src.sentiment.macro_risk.retry_call") as rt:
            sig = eng.get_signal()
        self.assertIs(sig, eng._cached)
        rt.assert_not_called()

    def test_breaker_open_no_cache(self):
        eng = MacroRiskEngine()
        eng._breaker.failure_threshold = 1
        eng._breaker.on_failure(RuntimeError())
        self.assertIsNone(eng.get_signal())  # _cached is None

    def test_breaker_open_with_cache(self):
        eng = MacroRiskEngine()
        eng._cached = MacroSignal("BUY", 0.6, -1.0, {}, "MACRO")
        eng._breaker.failure_threshold = 1
        eng._breaker.on_failure(RuntimeError())
        self.assertIs(eng.get_signal(), eng._cached)

    def test_signal_sell(self):
        data = {
            "DX-Y.NYB": [100.0, 130.0],
            "^TNX": [4.0, 6.0],
            "^VIX": [18.0, 40.0],
            "GC=F": [2000.0, 2300.0],
        }
        eng = MacroRiskEngine(cache_ttl=600)
        with mock.patch("coinbase.src.yahoo_chart.fetch_closes", side_effect=make_fetch(data)):
            sig = eng.get_signal()
        self.assertIsNotNone(sig)
        self.assertEqual(sig.action, "SELL")

    def test_signal_buy(self):
        data = {
            "DX-Y.NYB": [100.0, 70.0],
            "^TNX": [4.5, 2.0],
            "^VIX": [25.0, 8.0],
            "GC=F": [2100.0, 1700.0],
        }
        eng = MacroRiskEngine(cache_ttl=600)
        with mock.patch("coinbase.src.yahoo_chart.fetch_closes", side_effect=make_fetch(data)):
            sig = eng.get_signal()
        self.assertEqual(sig.action, "BUY")

    def test_signal_hold_returns_none(self):
        data = {
            "DX-Y.NYB": [100.0, 100.5],
            "^TNX": [4.0, 4.02],
            "^VIX": [18.0, 18.2],
            "GC=F": [2000.0, 2005.0],
        }
        eng = MacroRiskEngine(cache_ttl=600)
        with mock.patch("coinbase.src.yahoo_chart.fetch_closes", side_effect=make_fetch(data)):
            sig = eng.get_signal()
        self.assertIsNone(sig)

    def test_skip_short_series(self):
        data = {
            "DX-Y.NYB": [100.0, 130.0],
            "^TNX": [4.0, 6.0],
            "^VIX": [18.0],  # len < 2 -> skipped
            "GC=F": [2000.0, 2300.0],
        }
        eng = MacroRiskEngine(cache_ttl=600)
        with mock.patch("coinbase.src.yahoo_chart.fetch_closes", side_effect=make_fetch(data)):
            sig = eng.get_signal()
        self.assertEqual(sig.action, "SELL")

    def test_fetch_exception(self):
        eng = MacroRiskEngine(cache_ttl=600)
        with mock.patch("coinbase.src.yahoo_chart.fetch_closes", side_effect=RuntimeError("boom")):
            self.assertIsNone(eng.get_signal())


if __name__ == "__main__":
    unittest.main()
