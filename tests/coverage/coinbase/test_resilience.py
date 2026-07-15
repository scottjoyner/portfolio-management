import time
import unittest
import urllib.error
from unittest import mock

from coinbase.src import resilience


class TestIsTransientError(unittest.TestCase):
    def test_timeout(self):
        self.assertTrue(resilience.is_transient_error(TimeoutError()))

    def test_connection(self):
        self.assertTrue(resilience.is_transient_error(ConnectionError()))

    def test_http_codes_transient(self):
        for code in (408, 425, 429, 500, 502, 503, 504):
            self.assertTrue(
                resilience.is_transient_error(
                    urllib.error.HTTPError(None, code, "", {}, None)))

    def test_http_other_code(self):
        self.assertFalse(
            resilience.is_transient_error(
                urllib.error.HTTPError(None, 404, "", {}, None)))

    def test_url_error(self):
        self.assertTrue(resilience.is_transient_error(urllib.error.URLError("boom")))

    def test_message_tokens(self):
        for tok in ("timeout", "temporarily", "rate limit", "reset by peer", "unavailable"):
            self.assertTrue(resilience.is_transient_error(RuntimeError(f"got {tok} now")))

    def test_message_no_token(self):
        self.assertFalse(resilience.is_transient_error(RuntimeError("weird failure")))

    def test_arbitrary_exception(self):
        self.assertFalse(resilience.is_transient_error(ValueError("nope")))


class TestRetryCall(unittest.TestCase):
    def test_success_first_try(self):
        fn = mock.MagicMock(return_value=42)
        self.assertEqual(resilience.retry_call(fn), 42)
        fn.assert_called_once()

    def test_retry_then_success(self):
        fn = mock.MagicMock(side_effect=[ConnectionError("x"), 7])
        with mock.patch.object(resilience, "time") as mt, \
                mock.patch.object(resilience, "random") as mr:
            mt.sleep.return_value = None
            mr.random.return_value = 0.0
            self.assertEqual(resilience.retry_call(fn, attempts=3), 7)
        self.assertEqual(fn.call_count, 2)

    def test_non_transient_raises_immediately(self):
        fn = mock.MagicMock(side_effect=ValueError("bad"))
        with mock.patch.object(resilience, "time") as mt:
            mt.sleep.return_value = None
            with self.assertRaises(ValueError):
                resilience.retry_call(fn, attempts=3, retry_if=resilience.is_transient_error)
        self.assertEqual(fn.call_count, 1)

    def test_retry_exhausted_raises_last(self):
        fn = mock.MagicMock(side_effect=ConnectionError("x"))
        with mock.patch.object(resilience, "time") as mt, \
                mock.patch.object(resilience, "random") as mr:
            mt.sleep.return_value = None
            mr.random.return_value = 0.0
            with self.assertRaises(ConnectionError):
                resilience.retry_call(fn, attempts=2)
        self.assertEqual(fn.call_count, 2)

    def test_attempts_one(self):
        fn = mock.MagicMock(side_effect=ConnectionError("x"))
        with mock.patch.object(resilience, "time") as mt:
            mt.sleep.return_value = None
            with self.assertRaises(ConnectionError):
                resilience.retry_call(fn, attempts=1)
        self.assertEqual(fn.call_count, 1)

    def test_custom_retry_if_false(self):
        fn = mock.MagicMock(side_effect=RuntimeError("x"))
        with mock.patch.object(resilience, "time") as mt:
            mt.sleep.return_value = None
            with self.assertRaises(RuntimeError):
                resilience.retry_call(fn, attempts=3, retry_if=lambda e: False)
        self.assertEqual(fn.call_count, 1)

    def test_max_delay_clamp(self):
        fn = mock.MagicMock(side_effect=ConnectionError("x"))
        with mock.patch.object(resilience, "time") as mt, \
                mock.patch.object(resilience, "random") as mr:
            mt.sleep.return_value = None
            mr.random.return_value = 0.0
            with self.assertRaises(ConnectionError):
                resilience.retry_call(fn, attempts=3, base_delay=100.0, max_delay=8.0)
        self.assertEqual(fn.call_count, 3)


class TestSourceCircuitBreaker(unittest.TestCase):
    def test_allow_closed(self):
        cb = resilience.SourceCircuitBreaker("x")
        self.assertTrue(cb.allow())

    def test_allow_open_not_expired(self):
        cb = resilience.SourceCircuitBreaker("x", reset_timeout_s=300.0)
        cb.failure_count = 5
        cb.state = "open"
        cb.opened_at = time.time()
        self.assertFalse(cb.allow())

    def test_allow_open_expired_to_half_open(self):
        cb = resilience.SourceCircuitBreaker("x", reset_timeout_s=0.0)
        cb.state = "open"
        cb.opened_at = time.time() - 10
        self.assertTrue(cb.allow())
        self.assertEqual(cb.state, "half_open")
        self.assertEqual(cb.success_count, 0)

    def test_on_success_closed(self):
        cb = resilience.SourceCircuitBreaker("x")
        cb.failure_count = 3
        cb.on_success()
        self.assertEqual(cb.failure_count, 0)
        self.assertEqual(cb.state, "closed")

    def test_on_success_half_open_below_threshold(self):
        cb = resilience.SourceCircuitBreaker("x", half_open_success_threshold=2)
        cb.state = "half_open"
        cb.on_success()
        self.assertEqual(cb.state, "half_open")
        self.assertEqual(cb.success_count, 1)

    def test_on_success_half_open_reaches_threshold(self):
        cb = resilience.SourceCircuitBreaker("x", half_open_success_threshold=1)
        cb.state = "half_open"
        cb.on_success()
        self.assertEqual(cb.state, "closed")
        self.assertEqual(cb.success_count, 0)

    def test_on_failure_below_threshold(self):
        cb = resilience.SourceCircuitBreaker("x", failure_threshold=3)
        cb.on_failure("err1")
        self.assertEqual(cb.state, "closed")
        self.assertEqual(cb.failure_count, 1)
        self.assertEqual(cb.last_error, "err1")

    def test_on_failure_exc_object(self):
        cb = resilience.SourceCircuitBreaker("x", failure_threshold=3)
        cb.on_failure(ValueError("boom"))
        self.assertEqual(cb.last_error, "boom")

    def test_on_failure_opens(self):
        cb = resilience.SourceCircuitBreaker("x", failure_threshold=2)
        cb.on_failure("err1")
        cb.on_failure("err2")
        self.assertEqual(cb.state, "open")
        self.assertGreater(cb.opened_at, 0)
        self.assertEqual(cb.success_count, 0)

    def test_snapshot(self):
        cb = resilience.SourceCircuitBreaker("x")
        cb.on_failure("e")
        snap = cb.snapshot()
        self.assertEqual(snap["name"], "x")
        self.assertEqual(snap["state"], "closed")
        self.assertEqual(snap["failure_count"], 1)
        self.assertEqual(snap["last_error"], "e")
        self.assertEqual(snap["success_count"], 0)


if __name__ == "__main__":
    unittest.main()
