"""Tests for coinbase/src/metrics.py"""
import json
import logging
import threading
import unittest
from unittest import mock

from coinbase.src import metrics as m


class TestStructuredLogger(unittest.TestCase):
    def setUp(self):
        self.sl = m.StructuredLogger(name="test-logger", level=logging.DEBUG)

    def test_set_clear_correlation(self):
        self.sl.set_correlation_id("abc")
        self.assertEqual(self.sl._correlation_id, "abc")
        self.sl.clear_correlation_id()
        self.assertIsNone(self.sl._correlation_id)

    def test_add_clear_context(self):
        self.sl.add_context(foo="bar")
        self.assertIn("foo", self.sl._context)
        self.sl.clear_context()
        self.assertEqual(self.sl._context, {})

    def test_log_levels(self):
        # Just ensure they don't raise
        self.sl.debug("d")
        self.sl.info("i")
        self.sl.warning("w")
        self.sl.error("e")
        self.sl.critical("c")

    def test_log_filters_none(self):
        # correlation id None should be filtered out
        self.sl._correlation_id = None
        self.sl.info("msg", extra_key=None, real=1)


class TestJsonFormatter(unittest.TestCase):
    def test_format(self):
        fmt = m.JsonFormatter()
        import sys
        rec = logging.LogRecord("n", logging.INFO, __file__, 1, "hello", None, None)
        rec.correlation_id = "x"
        out = fmt.format(rec)
        data = json.loads(out)
        self.assertEqual(data["msg"], "hello")
        self.assertEqual(data["correlation_id"], "x")

    def test_format_exc(self):
        fmt = m.JsonFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            import sys
            rec = logging.LogRecord("n", logging.ERROR, __file__, 1, "err", None, sys.exc_info())
        out = fmt.format(rec)
        data = json.loads(out)
        self.assertIn("exception", data)


class TestGetStructuredLogger(unittest.TestCase):
    def test_returns(self):
        self.assertIsInstance(m.get_structured_logger("x"), m.StructuredLogger)


class TestTimed(unittest.TestCase):
    def test_timed_runs(self):
        @m.timed(metric_name="m")
        def f(x):
            return x + 1
        self.assertEqual(f(1), 2)

    def test_timed_no_metric(self):
        @m.timed()
        def g():
            return 42
        self.assertEqual(g(), 42)


class TestRecordFunctions(unittest.TestCase):
    def test_record_prom_available(self):
        if not m.PROMETHEUS_AVAILABLE:
            self.skipTest("prometheus not available")
        m.record_scan("t", 5, 0.1, errors=2)
        m.record_signal("ema", "long")
        m.record_trade("BTC-USD", "buy", "filled")
        m.record_order("BTC-USD", "buy", "market")
        m.record_portfolio(1000, 100, 0.02, 1.0, 3)
        m.record_position("BTC-USD", "buy", 500, 10)
        m.record_daily_pnl(0.01)
        m.record_risk_score(50)
        m.record_slippage("BTC-USD", 5.0)
        m.record_api_latency("/x", 0.01)

    def test_record_prom_unavailable(self):
        with mock.patch.object(m, "PROMETHEUS_AVAILABLE", False):
            m.record_scan("t", 5, 0.1)
            m.record_signal("ema", "long")
            m.record_trade("BTC-USD", "buy", "filled")
            m.record_order("BTC-USD", "buy", "market")
            m.record_portfolio(1000, 100, 0.02, 1.0, 3)
            m.record_position("BTC-USD", "buy", 500, 10)
            m.record_daily_pnl(0.01)
            m.record_risk_score(50)
            m.record_slippage("BTC-USD", 5.0)
            m.record_api_latency("/x", 0.01)

    def test_timed_unavailable(self):
        with mock.patch.object(m, "PROMETHEUS_AVAILABLE", False):
            @m.timed(metric_name="m")
            def h():
                return 1
            self.assertEqual(h(), 1)


class TestMetricsServer(unittest.TestCase):
    def test_start_stop(self):
        srv = m.MetricsServer(port=19091)
        srv.start()
        self.assertIsNotNone(srv._server)
        srv.stop()
        self.assertIsNone(srv._server)

    def test_start_idempotent(self):
        srv = m.MetricsServer(port=19092)
        srv.start()
        srv.start()  # second call returns early
        srv.stop()

    def _get(self, port, path):
        import urllib.request
        import urllib.error
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def test_handler_endpoints(self):
        srv = m.MetricsServer(port=19093)
        srv.start()
        try:
            code, _ = self._get(19093, "/metrics")
            self.assertIn(code, (200, 503))
            code, _ = self._get(19093, "/health")
            self.assertEqual(code, 200)
            code, _ = self._get(19093, "/nope")
            self.assertEqual(code, 404)
        finally:
            srv.stop()

    def test_stop_without_start(self):
        srv = m.MetricsServer(port=19096)
        srv.stop()  # no server -> early return branch

    def test_handler_no_prom(self):
        with mock.patch.object(m, "PROMETHEUS_AVAILABLE", False):
            srv = m.MetricsServer(port=19094)
            srv.start()
            try:
                code, _ = self._get(19094, "/metrics")
                self.assertEqual(code, 503)
            finally:
                srv.stop()


class TestStartMetricsServer(unittest.TestCase):
    def test_global(self):
        srv = m.start_metrics_server(port=19095)
        self.assertIsInstance(srv, m.MetricsServer)
        # second call returns the existing instance (already-set branch)
        srv2 = m.start_metrics_server(port=19097)
        self.assertIs(srv, srv2)
        m._METRICS_SERVER = None


if __name__ == "__main__":
    unittest.main()
