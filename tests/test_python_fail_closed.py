"""Regression tests for fail-open defects found in the Python audit.

Each of these failed before the corresponding fix. They are written as
behavioural tests -- tamper the input, assert the guard refuses -- because the
pattern these bugs share is a guard that passes when it has no information, and
that is only observable by giving it nothing.

A test that asserts the guard is merely present would have passed against every
one of these bugs.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _request():
    """A minimal, low-risk request: any rejection must come from the validations."""
    from trading_system.approval.workflow_engine import ApprovalRequest

    return ApprovalRequest(
        strategy_key="strategy-target",
        version="1",
        risk_level=0.1,
        capital_allocation=100.0,
        target_performance=0.05,
        max_drawdown_tolerance=0.02,
    )


class TestRequiredValidationMustBePresent(unittest.TestCase):
    """A required validation that is absent has not passed."""

    def _router(self):
        from trading_system.approval.workflow_engine import WorkflowEngine

        return WorkflowEngine()

    def test_absent_validation_results_is_rejected(self):
        # Previously: `if name in validation_results` skipped missing keys
        # entirely, so calling with no second argument approved a strategy that
        # had no code review and no security scan.
        router = self._router()
        request = _request()
        result = router.route_strategy(request)
        self.assertEqual(
            result["status"],
            "rejected",
            "a missing required validation must reject, not approve",
        )

    def test_empty_dict_is_rejected(self):
        router = self._router()
        request = _request()
        self.assertEqual(router.route_strategy(request, {})["status"], "rejected")

    def test_partial_results_are_rejected(self):
        router = self._router()
        request = _request()
        result = router.route_strategy(request, {"code_review_passed": True})
        self.assertEqual(
            result["status"],
            "rejected",
            "two of three required validations missing must not approve",
        )

    def test_explicitly_false_is_rejected(self):
        router = self._router()
        request = _request()
        result = router.route_strategy(
            request,
            {
                "code_review_passed": True,
                "security_scan_passed": True,
                "performance_benchmark_met": False,
            },
        )
        self.assertEqual(result["status"], "rejected")

    def test_all_present_and_true_is_not_rejected_for_missing_validations(self):
        # The gate must still be able to pass, otherwise the fix is just a wall.
        router = self._router()
        request = _request()
        result = router.route_strategy(
            request,
            {
                "code_review_passed": True,
                "security_scan_passed": True,
                "performance_benchmark_met": True,
            },
        )
        self.assertNotIn(
            result["status"],
            ("rejected",),
            "a fully validated request must not be rejected for missing validations",
        )


class TestCircuitBreakerUnknownState(unittest.TestCase):
    """An unrecognised breaker state must refuse, not permit."""

    def _breaker(self):
        from trading_system.safety.bulkeproof_safety_system import CircuitBreaker

        return CircuitBreaker()

    def test_unknown_state_refuses(self):
        breaker = self._breaker()
        for state in ("CORRUPT", "CLOSED_ISH", "", None, "half-open"):
            breaker._state = state
            self.assertFalse(
                breaker.can_execute(),
                f"state {state!r} is not a state we can reason about; must refuse",
            )

    def test_closed_allows(self):
        breaker = self._breaker()
        breaker._state = "CLOSED"
        self.assertTrue(breaker.can_execute())

    def test_open_refuses(self):
        breaker = self._breaker()
        breaker._state = "OPEN"
        breaker._last_failure_time = 9e12  # cooldown not elapsed
        self.assertFalse(breaker.can_execute())


class TestLimitManagerFailsClosed(unittest.TestCase):
    """No configured limit must refuse, and non-positive input must refuse."""

    def _manager(self, configured=True):
        from trading_system.risk.limits.service import LimitManager, PositionLimit

        manager = LimitManager()
        if configured:
            manager.set_limit(
                PositionLimit(
                    product_id="BTC-USD",
                    max_size=Decimal("1"),
                    max_notional=Decimal("1000"),
                )
            )
        return manager

    def test_unconfigured_product_refuses(self):
        # Previously "no limit configured" returned True, so any symbol missing
        # from the map traded uncapped.
        manager = self._manager(configured=False)
        ok, reason = manager.check_order("BTC-USD", "buy", Decimal("1"), Decimal("50000"))
        self.assertFalse(ok)
        self.assertIn("no limit configured", reason)

    def test_configured_product_still_allows_within_limits(self):
        manager = self._manager()
        ok, reason = manager.check_order("BTC-USD", "buy", Decimal("0.01"), Decimal("50000"))
        self.assertTrue(ok, f"an in-limit order must still pass: {reason}")

    def test_zero_price_is_refused(self):
        # notional = size * 0 = 0, which passed every notional comparison.
        manager = self._manager()
        ok, _ = manager.check_order("BTC-USD", "buy", Decimal("1"), Decimal("0"))
        self.assertFalse(ok, "a zero price must not produce a zero-notional pass")

    def test_negative_size_is_refused(self):
        manager = self._manager()
        ok, _ = manager.check_order("BTC-USD", "buy", Decimal("-1"), Decimal("50000"))
        self.assertFalse(ok, "a negative size must be refused")

    def test_over_limit_still_refused(self):
        manager = self._manager()
        ok, _ = manager.check_order("BTC-USD", "buy", Decimal("100"), Decimal("50000"))
        self.assertFalse(ok)


class TestWebhookSignatureVerification(unittest.TestCase):
    """A verify_* function must reject a forged signature."""

    def _sign(self, body: bytes, secret: str) -> str:
        return base64.b64encode(
            hmac.new(secret.encode(), body, hashlib.sha256).digest()
        ).decode()

    def test_valid_signature_accepted(self):
        from trading_system.plaid.services import PlaidService

        body = b'{"a":1}'
        svc = PlaidService(client_id="test-client")
        self.assertTrue(asyncio_run(svc.verify_webhook_signature(body, self._sign(body, "s3cret"), "s3cret")))

    def test_forged_signature_rejected(self):
        from trading_system.plaid.services import PlaidService

        svc = PlaidService(client_id="test-client")
        self.assertFalse(asyncio_run(svc.verify_webhook_signature(b'{"a":1}', "not-the-signature", "s3cret")))

    def test_tampered_body_rejected(self):
        from trading_system.plaid.services import PlaidService

        body = b'{"a":1}'
        svc = PlaidService(client_id="test-client")
        signature = self._sign(body, "s3cret")
        self.assertFalse(asyncio_run(svc.verify_webhook_signature(b'{"a":2}', signature, "s3cret")))

    def test_wrong_secret_rejected(self):
        from trading_system.plaid.services import PlaidService

        body = b'{"a":1}'
        svc = PlaidService(client_id="test-client")
        self.assertFalse(asyncio_run(svc.verify_webhook_signature(body, self._sign(body, "other"), "s3cret")))

    def test_missing_secret_rejects(self):
        # No secret means we cannot authenticate anything; returning True here
        # would make the absence of configuration equivalent to success.
        from trading_system.plaid.services import PlaidService

        svc = PlaidService(client_id="test-client")
        self.assertFalse(asyncio_run(svc.verify_webhook_signature(b"{}", "sig", "")))

    def test_empty_signature_rejected(self):
        from trading_system.plaid.services import PlaidService

        svc = PlaidService(client_id="test-client")
        self.assertFalse(asyncio_run(svc.verify_webhook_signature(b"{}", "", "s3cret")))


def asyncio_run(coro):
    import asyncio

    return asyncio.run(coro)


class TestApprovalExpiryIsEnforced(unittest.TestCase):
    """A lapsed human approval must not still execute a trade."""

    def test_iso_timestamp_records_expire(self):
        # Every real writer stamps datetime.now(timezone.utc).isoformat().
        # is_expired used to try only float(), so every production record raised
        # ValueError and was reported not-expired: the 24h TTL never fired once.
        from datetime import datetime, timedelta, timezone
        import approval_server

        now = datetime.now(timezone.utc)
        self.assertTrue(
            approval_server.is_expired({"created_at": (now - timedelta(days=30)).isoformat()})
        )
        self.assertTrue(
            approval_server.is_expired({"created_at": (now - timedelta(hours=25)).isoformat()})
        )
        self.assertFalse(
            approval_server.is_expired({"created_at": (now - timedelta(hours=1)).isoformat()})
        )

    def test_numeric_timestamps_still_work(self):
        import time
        import approval_server

        self.assertTrue(approval_server.is_expired({"created_at": time.time() - 30 * 86400}))
        self.assertFalse(approval_server.is_expired({"created_at": time.time() - 10}))
        self.assertFalse(approval_server.is_expired({"created_at": str(time.time() - 10)}))

    def test_unreadable_timestamp_is_expired_not_valid(self):
        import approval_server

        for record in (
            {"status": "pending"},
            {"created_at": "not-a-time"},
            {"expiry_ts": "not-a-time"},
            {"created_at": ""},
            "not a dict",
        ):
            with self.subTest(record=record):
                self.assertTrue(approval_server.is_expired(record))

    def test_corrupt_record_is_not_handed_a_fresh_ttl(self):
        import approval_server

        stamped = approval_server.stamp_approval_timestamps({"created_at": "corrupt"})
        self.assertTrue(approval_server.is_expired(stamped))

    def test_parse_timestamp_handles_naive_and_zulu(self):
        from datetime import datetime, timezone
        import approval_server

        aware = datetime.now(timezone.utc)
        self.assertIsNotNone(approval_server.parse_timestamp(aware.isoformat()))
        self.assertIsNotNone(
            approval_server.parse_timestamp(aware.replace(tzinfo=None).isoformat() + "Z")
        )
        self.assertIsNone(approval_server.parse_timestamp("garbage"))
        self.assertIsNone(approval_server.parse_timestamp(True))


class TestSpendLimitsRejectNonPositiveAmounts(unittest.TestCase):
    """Every cap is an upper bound, so negatives passed all of them."""

    def _engine(self):
        from trading_system.onchain.wallets.policy_engine.engine import (
            WalletPolicy,
            WalletPolicyEngine,
        )

        engine = WalletPolicyEngine()
        engine.register_policy(WalletPolicy("w", 1000.0, 100.0, 100.0, 100.0, 100.0))
        return engine

    def test_negative_notional_refused(self):
        engine = self._engine()
        ok, reason = engine.approve_spend("w", -1_000_000.0, 0, 0, 0)
        self.assertFalse(ok, reason)
        self.assertEqual(engine.daily_spent["w"], 0.0, "a negative must not move the counter")

    def test_zero_notional_refused(self):
        engine = self._engine()
        ok, _ = engine.approve_spend("w", 0.0, 0, 0, 0)
        self.assertFalse(ok)

    def test_negative_components_refused(self):
        engine = self._engine()
        for kwargs in (
            {"notional": 10.0, "contract_spend": -5.0, "token_spend": 0.0, "allowance_requested": 0.0},
            {"notional": 10.0, "contract_spend": 0.0, "token_spend": 0.0, "allowance_requested": -1.0},
            {"notional": 10.0, "contract_spend": 0.0, "token_spend": 0.0,
             "allowance_requested": 0.0, "bridge_spend": -99.0},
        ):
            with self.subTest(**kwargs):
                ok, reason = engine.approve_spend("w", **kwargs)
                self.assertFalse(ok, reason)

    def test_non_numeric_refused(self):
        engine = self._engine()
        for bad in ("500", True, None):
            with self.subTest(bad=bad):
                ok, _ = engine.approve_spend("w", bad, 0, 0, 0)
                self.assertFalse(ok)

    def test_legitimate_spend_still_approved_and_counted(self):
        engine = self._engine()
        ok, reason = engine.approve_spend("w", 500.0, 10.0, 10.0, 10.0)
        self.assertTrue(ok, reason)
        self.assertEqual(engine.daily_spent["w"], 500.0)

    def test_repeated_negatives_cannot_drain_the_daily_limit(self):
        """The concrete attack the sign check closes."""
        engine = self._engine()
        engine.approve_spend("w", 500.0, 1.0, 1.0, 1.0)
        for _ in range(50):
            engine.approve_spend("w", -10_000.0, 0, 0, 0)
        self.assertGreaterEqual(engine.daily_spent["w"], 0.0)
        self.assertLess(engine.daily_spent["w"], 1000.0)
        ok, _ = engine.approve_spend("w", 900.0, 0, 0, 0)
        self.assertFalse(ok, "the daily limit must still bind")


class TestOneKillSwitchSemantics(unittest.TestCase):
    """Every execution path must agree on whether the switch is engaged."""

    def test_optimizer_agrees_with_the_execution_paths(self):
        import os
        import portfolio_optimizer
        from coinbase.src.config import is_kill_switch_active

        saved = os.environ.get("KILL_SWITCH")
        try:
            for value in ("false", "true", "1", "yes", "on", "y", "t", "TRUE", "ON", "banana"):
                with self.subTest(KILL_SWITCH=value):
                    os.environ["KILL_SWITCH"] = value
                    self.assertEqual(
                        portfolio_optimizer._kill_switch_active(),
                        is_kill_switch_active(),
                        f"optimizer and execution paths disagree on KILL_SWITCH={value!r}",
                    )
        finally:
            if saved is None:
                os.environ.pop("KILL_SWITCH", None)
            else:
                os.environ["KILL_SWITCH"] = saved

    def test_optimizer_loop_actually_halts_on_the_kill_switch(self):
        """run() must consult the shared resolver, not a private copy.

        Asserting only that the helper agrees would pass even if run() still
        used its own weaker inline check, which is the bug this replaced.
        """
        import os
        import portfolio_optimizer
        from coinbase.src.config import KILL_SWITCH_ENV

        class _Stub:
            dry_run = True
            interval = 1
            _tick_count = 0
            _last_tick_ts = 0.0

            def __init__(self):
                self.running = True
                self.ticked = False

            def _tick(self):
                self.ticked = True

        saved = os.environ.get(KILL_SWITCH_ENV)
        try:
            for value in ("true", "ON", "banana"):
                with self.subTest(KILL_SWITCH=value):
                    os.environ[KILL_SWITCH_ENV] = value
                    stub = _Stub()
                    # Exercise the real loop body without the Optimizer's __init__.
                    portfolio_optimizer.PortfolioOptimizer.run(stub)
                    self.assertFalse(stub.running, "the loop must stop")
                    self.assertFalse(
                        stub.ticked,
                        f"KILL_SWITCH={value} must halt before the next tick places trades",
                    )

            os.environ[KILL_SWITCH_ENV] = "false"
            stub = _Stub()
            stub._tick = lambda: (_ for _ in ()).throw(KeyboardInterrupt)
            portfolio_optimizer.PortfolioOptimizer.run(stub)
            self.assertFalse(stub.running, "a non-engaged switch must not halt the loop")
        finally:
            if saved is None:
                os.environ.pop(KILL_SWITCH_ENV, None)
            else:
                os.environ[KILL_SWITCH_ENV] = saved

    def test_optimizer_honours_the_sentinel_file(self):
        """`touch data/trading_kill_switch` must stop the optimizer too."""
        import os
        import portfolio_optimizer
        from pathlib import Path

        from coinbase.src.config import KILL_SWITCH_ENV, KILL_SWITCH_PATH_ENV

        saved_env = os.environ.pop(KILL_SWITCH_ENV, None)
        saved_path = os.environ.get(KILL_SWITCH_PATH_ENV)
        tmp = Path(tempfile.mkdtemp()) / "kill_switch"
        os.environ[KILL_SWITCH_PATH_ENV] = str(tmp)
        try:
            self.assertFalse(portfolio_optimizer._kill_switch_active())
            tmp.parent.mkdir(parents=True, exist_ok=True)
            tmp.touch()
            self.assertTrue(
                portfolio_optimizer._kill_switch_active(),
                "the sentinel file is the documented way to halt trading; "
                "the optimizer used to ignore it",
            )
        finally:
            if saved_env is not None:
                os.environ[KILL_SWITCH_ENV] = saved_env
            if saved_path is None:
                os.environ.pop(KILL_SWITCH_PATH_ENV, None)
            else:
                os.environ[KILL_SWITCH_PATH_ENV] = saved_path


if __name__ == "__main__":
    unittest.main()
