"""
Latency/efficiency regression tests for EventTraderV4 hot path.

These guard the per-tick caching optimizations in run_trader_v4.py:
  * _get_slices        — materializes streaming buffers (to_list) at most once
                          per product per drain tick.
  * _get_cross_asset_state — caches the global cross-asset regime snapshot.
  * _get_paper_drawdown    — caches paper drawdown for the tick.

Each test uses unittest.mock to count calls to the expensive dependency and
asserts the hot path does not recompute more than necessary on repeated ticks.
"""
import os
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from coinbase.src.run_trader_v4 import EventTraderV4

logging = __import__("logging")
logging.disable(logging.CRITICAL)


def _make_trader(products):
    return EventTraderV4(mode="paper", products=list(products), dry_run=True)


def _fake_streaming(closes_len=50):
    closes = [100.0 + i for i in range(closes_len)]
    volumes = [1.0] * closes_len
    closes_buf = MagicMock()
    closes_buf.to_list.return_value = list(closes)
    volumes_buf = MagicMock()
    volumes_buf.to_list.return_value = list(volumes)

    class _FakeStreaming:
        def try_get(self, pid):
            return self

        def update(self, pid, price, volume=0.0):
            pass

    fs = _FakeStreaming()
    fs.closes = closes_buf
    fs.volumes = volumes_buf
    return fs, closes_buf, volumes_buf


class TestGetSlicesCache(unittest.TestCase):
    def setUp(self):
        self.t = _make_trader(["BTC-USD", "ETH-USD"])
        self.fs, self.cb, self.vb = _fake_streaming()
        self.t.streaming = self.fs
        self.t._slice_cache = {}

    def test_to_list_called_once_per_product(self):
        # First access materializes; second access within the same tick hits cache.
        a = self.t._get_slices("BTC-USD")
        b = self.t._get_slices("BTC-USD")
        self.assertIs(a, b)
        self.cb.to_list.assert_called_once()
        self.vb.to_list.assert_called_once()

    def test_distinct_products_materialize_separately(self):
        self.t._get_slices("BTC-USD")
        self.t._get_slices("ETH-USD")
        self.assertEqual(self.cb.to_list.call_count, 2)

    def test_returns_none_when_streaming_missing(self):
        self.t.streaming = None
        self.assertIsNone(self.t._get_slices("BTC-USD"))

    def test_opens_derived_from_closes(self):
        closes = [1.0, 2.0, 3.0, 4.0]
        volumes = [1.0, 1.0, 1.0, 1.0]
        self.t._slice_cache = {}
        self.cb.to_list.return_value = list(closes)
        self.vb.to_list.return_value = list(volumes)
        closes_out, volumes_out, opens_out = self.t._get_slices("BTC-USD")
        self.assertEqual(closes_out, closes)
        self.assertEqual(opens_out, closes[:-1] + closes[-1:])


class TestCrossAssetStateCache(unittest.TestCase):
    def setUp(self):
        self.t = _make_trader(["BTC-USD"])
        self.snap = MagicMock(return_value={"regime": "mixed", "risk_multiplier": 0.75})
        self.t._cross_asset_regime_snapshot = self.snap
        self.t._car_state_cache = (0.0, None)

    def test_snapshot_called_once_within_ttl(self):
        self.t._get_cross_asset_state()
        self.t._get_cross_asset_state()
        self.snap.assert_called_once()

    def test_cache_miss_after_expiry(self):
        self.t._get_cross_asset_state()
        self.snap.assert_called_once()
        # Force the 1s TTL to expire.
        self.t._car_state_cache = (0.0, None)
        self.t._get_cross_asset_state()
        self.assertEqual(self.snap.call_count, 2)


class TestPaperDrawdownCache(unittest.TestCase):
    def setUp(self):
        self.t = _make_trader(["BTC-USD"])
        self.dd = MagicMock(return_value=0.07)
        self.t._paper_drawdown = self.dd
        self.t._dd_cache = None

    def test_drawdown_computed_once_per_tick(self):
        self.assertEqual(self.t._get_paper_drawdown(), 0.07)
        self.assertEqual(self.t._get_paper_drawdown(), 0.07)
        self.dd.assert_called_once()

    def test_cache_reset_forces_recompute(self):
        self.t._get_paper_drawdown()
        self.dd.assert_called_once()
        self.t._dd_cache = None
        self.t._get_paper_drawdown()
        self.assertEqual(self.dd.call_count, 2)


class TestDrainTickDedup(unittest.TestCase):
    """End-to-end: a single drain tick materializes each product's buffers once."""

    def setUp(self):
        self.t = _make_trader(["BTC-USD", "ETH-USD"])
        self.fs, self.cb, self.vb = _fake_streaming()
        self.t.streaming = self.fs
        self.t._feed_mgr = None
        self.t._cross_asset_regime = None
        self.t._order_flow.evaluate = MagicMock(return_value=None)
        self.t._scalping.get_signals = MagicMock(return_value=None)
        fake_ticker = SimpleNamespace(price=100.0, volume_24h=1e9, bid=99.9, ask=100.1)
        self.t._ticker_cache = SimpleNamespace(get_ticker=MagicMock(return_value=fake_ticker))

    def test_to_list_once_per_product_across_consumers(self):
        with patch("rust_core.evaluate_all_opens_py", return_value=[]):
            self.t._drain_ticker_cache()
        # Every product's buffers are materialized exactly once per drain tick
        # (BTC-USD additionally feeds the btc snapshot, all sharing one cache).
        self.assertEqual(self.cb.to_list.call_count, len(self.t.products))
        self.assertEqual(self.vb.to_list.call_count, len(self.t.products))


class TestEvaluateOutputEquivalence(unittest.TestCase):
    """Passing precomputed slices must yield identical Rust inputs as auto-slices."""

    def setUp(self):
        self.t = _make_trader(["BTC-USD"])
        self.fs, self.cb, self.vb = _fake_streaming()
        self.t.streaming = self.fs
        self.t._cross_asset_regime = None
        self.t._candle_data = {}
        self.raw = [("ema_cross", "BUY", 0.6, "r")]
        self.rust = MagicMock(return_value=self.raw)

    def test_same_rust_args_with_and_without_slices(self):
        with patch("rust_core.evaluate_all_opens_py", self.rust):
            # Auto-slices path.
            self.t._slice_cache = {}
            self.t._evaluate_impl("BTC-USD")
            auto_args = self.rust.call_args
            self.rust.reset_mock()
            # Explicit slices path.
            closes = [100.0 + i for i in range(50)]
            volumes = [1.0] * 50
            opens = closes[:-1] + closes[-1:]
            self.t._evaluate_impl("BTC-USD", slices=(closes, volumes, opens))
            explicit_args = self.rust.call_args
        self.assertEqual(auto_args, explicit_args)

    def test_short_slices_early_return(self):
        # Length guard keeps behavior identical (no Rust call, no crash).
        self.t._evaluate_impl("BTC-USD", slices=([1.0, 2.0], [1.0, 1.0], [1.0, 2.0]))
        self.t.streaming = None
        self.t._evaluate_impl("BTC-USD")


class TestAdaptiveIntervalFallback(unittest.TestCase):
    def setUp(self):
        self.t = _make_trader(["BTC-USD"])

    def test_disabled_returns_min_interval(self):
        self.t._adaptive_eval_enabled = False
        self.assertEqual(self.t._adaptive_eval_interval_for_pid("BTC-USD"), self.t._min_eval_interval)

    def test_fallback_when_slices_none(self):
        # _get_slices returns None but streaming is valid -> real path used.
        self.t._get_slices = MagicMock(return_value=None)

        class _Buf:
            def __len__(self):
                return 50

            def to_list(self):
                return list(range(50))

        stream = SimpleNamespace(closes=_Buf())
        self.t.streaming = SimpleNamespace(try_get=MagicMock(return_value=stream))
        result = self.t._adaptive_eval_interval_for_pid("BTC-USD")
        self.assertIn(result, {5.0, 2.0, 1.0, 0.5, self.t._min_eval_interval})

    def test_short_closes_returns_min_interval(self):
        self.t._get_slices = MagicMock(return_value=([1.0, 2.0], [1.0, 1.0], [1.0, 2.0]))
        self.assertEqual(self.t._adaptive_eval_interval_for_pid("BTC-USD"), self.t._min_eval_interval)


if __name__ == "__main__":
    unittest.main()
