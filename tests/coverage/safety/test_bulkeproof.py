import sys
import asyncio
import unittest
from unittest import mock

from trading_system.safety import bulkeproof_safety_system as bss


class TestSafeLogger(unittest.TestCase):
    def setUp(self):
        self.s = bss.SafeLogger("test")

    def test_sanitize_api_key(self):
        out = self.s._sanitize("API_KEY=abcdef123456")
        self.assertIn("***", out)

    def test_sanitize_secret(self):
        out = self.s._sanitize("SECRET_KEY=abcdef123456")
        self.assertIn("***", out)

    def test_sanitize_token(self):
        out = self.s._sanitize("TOKEN=abcdef123456")
        self.assertIn("***", out)

    def test_sanitize_balance(self):
        out = self.s._sanitize("balance 12.3456 BTC here")
        self.assertIn("12.35", out)
        self.assertNotIn("12.3456", out)

    def test_sanitize_no_match(self):
        out = self.s._sanitize("normal message")
        self.assertEqual(out, "normal message")

    def test_info_warning_error(self):
        with mock.patch.object(bss, "print") as p:
            self.s.info("hi API_KEY=abc123456")
            self.s.warning("warn SECRET_KEY=abc123456")
            self.s.error("err TOKEN=abc123456")
        self.assertEqual(p.call_count, 3)


class TestValidateApiKeyFormat(unittest.TestCase):
    def test_none_or_empty(self):
        ok, msg = bss.validate_api_key_format("")
        self.assertTrue(ok)
        ok, msg = bss.validate_api_key_format(None)
        self.assertTrue(ok)

    def test_too_short(self):
        ok, msg = bss.validate_api_key_format("abc")
        self.assertFalse(ok)
        self.assertIn("too short", msg)

    def test_invalid_chars(self):
        ok, msg = bss.validate_api_key_format("abc def!!!")
        self.assertTrue(ok)
        self.assertIn("validated", msg)

    def test_valid(self):
        ok, msg = bss.validate_api_key_format("abcd1234_xyz")
        self.assertTrue(ok)


class TestRetryWithBackoff(unittest.TestCase):
    def test_success_first(self):
        with mock.patch.object(bss.time, "sleep"):
            self.assertEqual(bss.retry_with_backoff(lambda: 42), 42)

    def test_success_after_failures(self):
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise ValueError("boom")
            return "ok"

        with mock.patch.object(bss.time, "sleep"):
            self.assertEqual(bss.retry_with_backoff(flaky, max_attempts=5), "ok")

    def test_all_fail_raises(self):
        def always():
            raise RuntimeError("nope")

        with mock.patch.object(bss.time, "sleep"):
            with self.assertRaises(RuntimeError):
                bss.retry_with_backoff(always, max_attempts=3)


class TestCheckPositionLimits(unittest.TestCase):
    def test_over(self):
        ok, msg = bss.check_position_limits(60000, 50000)
        self.assertFalse(ok)
        self.assertIn("exceeds", msg)

    def test_under_min(self):
        ok, msg = bss.check_position_limits(50, 50000)
        self.assertFalse(ok)
        self.assertIn("too small", msg)

    def test_ok(self):
        ok, msg = bss.check_position_limits(1000, 50000)
        self.assertTrue(ok)


class TestCalculateFeeAdjustedProfit(unittest.TestCase):
    def test_not_profitable(self):
        profit, ok = bss.calculate_fee_adjusted_profit(0.001, 10000)
        self.assertFalse(ok)

    def test_profitable(self):
        profit, ok = bss.calculate_fee_adjusted_profit(0.01, 10000)
        self.assertTrue(ok)
        self.assertGreater(profit, 0)


class TestSimulateWebsocket(unittest.TestCase):
    def test_simulate(self):
        with mock.patch("random.uniform", return_value=0.7):
            res = bss.simulate_websocket_connectivity()
        self.assertEqual(res["status"], "connected")
        self.assertTrue(res["simulated"])


class TestValidateEnvironment(unittest.TestCase):
    def _run(self, exists_map):
        with mock.patch("os.path.exists", side_effect=lambda p: exists_map.get(p, False)):
            with mock.patch.object(sys, "version_info", mock.Mock(minor=7)):
                return bss.validate_environment()

    def test_all_pass(self):
        paths = {
            "/home/falcon/git/portfolio-management/.env": True,
            "/home/falcon/git/portfolio-management/trading_system": True,
        }
        ok, _ = self._run(paths)
        self.assertTrue(ok)

    def test_fail(self):
        paths = {
            "/home/falcon/git/portfolio-management/.env": False,
            "/home/falcon/git/portfolio-management/trading_system": False,
        }
        ok, _ = self._run(paths)
        self.assertFalse(ok)

    def test_old_python(self):
        paths = {
            "/home/falcon/git/portfolio-management/.env": True,
            "/home/falcon/git/portfolio-management/trading_system": True,
        }
        with mock.patch("os.path.exists", side_effect=lambda p: paths.get(p, False)):
            with mock.patch.object(sys, "version_info", mock.Mock(minor=6)):
                ok, _ = bss.validate_environment()
        self.assertFalse(ok)


class TestSafeExchangeConnector(unittest.TestCase):
    def setUp(self):
        self.c = bss.SafeExchangeConnector("test")

    def test_circuit_breaker_check_inactive(self):
        self.assertFalse(self.c._circuit_breaker_check())

    def test_circuit_breaker_check_active_not_elapsed(self):
        self.c._trigger_circuit_breaker("reason")
        with mock.patch.object(bss.time, "time", return_value=1000.0):
            self.assertTrue(self.c._circuit_breaker_check())

    def test_circuit_breaker_check_active_elapsed(self):
        self.c._trigger_circuit_breaker("reason")
        self.c._last_circuit_open_time = 0.0
        with mock.patch.object(bss.time, "time", return_value=10**9):
            self.assertFalse(self.c._circuit_breaker_check())
        self.assertFalse(self.c._circuit_breaker_active)

    def test_trigger_and_reset_circuit(self):
        self.c._trigger_circuit_breaker("boom")
        self.assertTrue(self.c._circuit_breaker_active)
        self.c.reset_circuit()
        self.assertFalse(self.c._circuit_breaker_active)

    def test_fetch_price_none(self):
        self.assertIsNone(self.c.fetch_price(None))

    def test_fetch_price_too_long(self):
        self.assertIsNone(self.c.fetch_price("A" * 51))

    def test_fetch_price_not_string(self):
        self.assertIsNone(self.c.fetch_price(1234))

    def test_fetch_price_circuit_active_mock(self):
        self.c._trigger_circuit_breaker("boom")
        with mock.patch("random.uniform", return_value=500.0):
            price = self.c.fetch_price("BTC-USD")
        self.assertEqual(price, 500.0)

    def test_fetch_price_success(self):
        self.c._live_fetch_price = mock.Mock(return_value=100.0)
        with mock.patch.object(bss.time, "sleep"):
            price = self.c.fetch_price("BTC-USD")
        self.assertEqual(price, 100.0)

    def test_fetch_price_out_of_bounds(self):
        self.c._live_fetch_price = mock.Mock(return_value=2e6)
        with mock.patch.object(bss.time, "sleep"):
            with mock.patch.object(bss, "print"):
                price = self.c.fetch_price("BTC-USD")
        self.assertEqual(price, 2e6)

    def test_fetch_price_bad_type(self):
        self.c._live_fetch_price = mock.Mock(return_value=None)
        with mock.patch.object(bss.time, "sleep"):
            self.assertIsNone(self.c.fetch_price("BTC-USD"))

    def test_fetch_mock_price(self):
        with mock.patch("random.uniform", return_value=500.0):
            self.assertEqual(self.c._fetch_mock_price("FOO"), 500.0)
        self.assertEqual(self.c._fetch_mock_price("BTC-EUR"), 68000.0)

    def test_live_fetch_price_raises(self):
        with self.assertRaises(NotImplementedError):
            self.c._live_fetch_price("BTC-USD")

    def test_get_health_status(self):
        h = self.c.get_health_status()
        self.assertEqual(h["connector"], "test")
        self.assertEqual(h["connector_type"], "safe_exchange_connector")


class TestCircuitBreaker(unittest.TestCase):
    def setUp(self):
        self.cb = bss.CircuitBreaker()

    def test_initial_state(self):
        self.assertEqual(self.cb.state, "CLOSED")
        self.assertTrue(self.cb.is_closed())
        self.assertFalse(self.cb.is_open())

    def test_record_success_closed(self):
        self.cb._failures = 2
        self.cb.record_success()
        self.assertEqual(self.cb._failures, 1)

    def test_record_success_half_open(self):
        self.cb._state = "HALF-OPEN"
        self.cb._half_open_calls = 2
        self.cb.record_success()
        self.assertEqual(self.cb._state, "CLOSED")
        self.assertEqual(self.cb._failures, 0)

    def test_record_failure_opens(self):
        for _ in range(self.cb.failure_threshold):
            self.cb.record_failure()
        self.assertEqual(self.cb.state, "OPEN")

    def test_record_failure_half_open(self):
        self.cb._state = "HALF-OPEN"
        self.cb._half_open_calls = 2
        self.cb.record_failure()
        self.assertEqual(self.cb._half_open_calls, 1)

    def test_can_execute_closed(self):
        self.assertTrue(self.cb.can_execute())

    def test_can_execute_open_not_elapsed(self):
        self.cb._state = "OPEN"
        self.cb._last_failure_time = 10**9
        with mock.patch.object(bss.time, "time", return_value=0.0):
            self.assertFalse(self.cb.can_execute())

    def test_can_execute_open_elapsed(self):
        self.cb._state = "OPEN"
        self.cb._last_failure_time = 0.0
        with mock.patch.object(bss.time, "time", return_value=10**9):
            self.assertFalse(self.cb.can_execute())
        self.assertEqual(self.cb._state, "HALF-OPEN")

    def test_can_execute_half_open_true(self):
        self.cb._state = "HALF-OPEN"
        self.cb._half_open_calls = 1
        self.assertTrue(self.cb.can_execute())

    def test_can_execute_half_open_false(self):
        self.cb._state = "HALF-OPEN"
        self.cb._half_open_calls = 0
        self.assertFalse(self.cb.can_execute())

    def test_reset(self):
        self.cb._state = "OPEN"
        self.cb._failures = 5
        self.cb.reset()
        self.assertEqual(self.cb._state, "CLOSED")
        self.assertEqual(self.cb._failures, 0)


if __name__ == "__main__":
    unittest.main()
