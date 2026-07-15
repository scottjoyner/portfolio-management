"""
Additional offline branch-coverage tests for CBClient error paths and the
BracketManager/engine edge cases not exercised by the happy-path suites.

Run:  python3 -m unittest tests.test_execution_coverage -v
"""

from __future__ import annotations

import os
import subprocess
import time
import unittest
from unittest.mock import patch

from coinbase.src.cb_client import CBClient, RateLimiter
from coinbase.src.execution_v2 import (
    NativeExecutionEngine,
    BracketManager,
    OrderIntent,
    OrderResult,
    OrderType,
    OrderStatus,
)


def raiser(*args, dry_run=False):
    raise RuntimeError("forced cli error")


def make_cb(settlement: str = "USD", override_cli: bool = True):
    cb = CBClient.__new__(CBClient)
    cb.cli = "coinbase"
    cb.cli_env = "live"
    cb.dry_run_cli = False
    cb.settlement_currency = settlement
    cb._rate_limiter = RateLimiter()   # real limiter so _cli_json runs
    cb.timeout = 10
    cb._last_calls = []

    if override_cli:
        def fake_cli(*args, dry_run=False):
            cb._last_calls.append((list(args), dry_run))
            return {"order_id": "O1", "status": "OPEN", "product_id": args[2] if len(args) > 2 else ""}

        cb._cli_json = fake_cli
    return cb


class TestRateLimiter(unittest.TestCase):
    def test_acquire_under_limit(self):
        rl = RateLimiter(max_calls=5, period=0.0001)
        for _ in range(5):
            rl.acquire()   # should not raise / sleep meaningfully

    def test_acquire_over_limit_sleeps(self):
        rl = RateLimiter(max_calls=1, period=0.0005)
        rl.acquire()
        t0 = time.time()
        rl.acquire()       # should wait ~period
        self.assertGreaterEqual(time.time() - t0, 0.0002)


class TestCBClientErrorBranches(unittest.TestCase):
    def test_preview_order_bad_side_raises(self):
        cb = make_cb()
        with self.assertRaises(ValueError):
            cb.preview_order("HODL", "BTC-USD", base_size="1")

    def test_preview_order_sell_missing_base_returns_error_dict(self):
        cb = make_cb()
        res = cb.preview_order("SELL", "BTC-USD")
        self.assertIn("error", res)   # ValueError is swallowed into a synthetic error dict

    def test_preview_order_buy_missing_size_returns_error_dict(self):
        cb = make_cb()
        res = cb.preview_order("BUY", "BTC-USD")
        self.assertIn("error", res)

    def test_cli_json_nonzero_raises(self):
        cb = make_cb(override_cli=False)
        rc = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="boom")
        with patch.object(subprocess, "run", return_value=rc):
            with self.assertRaises(RuntimeError):
                cb._cli_json("balance")

    def test_cli_json_success_parses(self):
        cb = make_cb(override_cli=False)
        out = subprocess.CompletedProcess(args=[], returncode=0,
                                           stdout='{"accounts": []}', stderr="")
        with patch.object(subprocess, "run", return_value=out):
            self.assertEqual(cb._cli_json("balance"), {"accounts": []})

    def test_cli_json_dryrun_parses(self):
        cb = make_cb(override_cli=False)
        body = 'would execute orders_create\n{"product_id": "BTC-USD"}'
        out = subprocess.CompletedProcess(args=[], returncode=0, stdout=body, stderr="")
        with patch.object(subprocess, "run", return_value=out):
            self.assertEqual(cb._cli_json("orders", "create", dry_run=True)["product_id"], "BTC-USD")

    def test_get_fees_error_returns_empty(self):
        cb = make_cb()
        cb._cli_json = raiser
        self.assertEqual(cb.get_fees(), {})

    def test_list_orders_error_returns_empty(self):
        cb = make_cb()
        cb._cli_json = raiser
        self.assertEqual(cb.list_orders(status="OPEN"), [])

    def test_get_order_error_returns_empty(self):
        cb = make_cb()
        cb._cli_json = raiser
        self.assertEqual(cb.get_order("X"), {})

    def test_cancel_order_propagates_error(self):
        cb = make_cb()
        cb._cli_json = raiser
        with self.assertRaises(RuntimeError):
            cb.cancel_order("X")

    def test_best_bid_ask_synthetic_fallback(self):
        cb = make_cb()
        candles = {"candles": [{"close": 100.0, "high": 101.0, "low": 99.0}]}

        def fake_cli(*args, dry_run=False):
            if args[0:2] == ("products", "book"):
                raise RuntimeError("no book")
            if args[0:2] == ("products", "candles"):
                return candles
            return {"order_id": "O1"}
        cb._cli_json = fake_cli
        books = cb.best_bid_ask(["BTC-USD"]).get("pricebooks", [])
        self.assertTrue(books)
        self.assertIn("bids", books[0])

    def test_get_positions_no_portfolios(self):
        cb = make_cb()
        cb._cli_json = lambda *a, dry_run=False: {"portfolios": []}
        self.assertEqual(cb.get_positions(), [])

    def test_get_positions_error_returns_empty(self):
        cb = make_cb()
        cb._cli_json = raiser
        self.assertEqual(cb.get_positions(), [])

    def test_list_accounts_returns_accounts(self):
        cb = make_cb()
        cb._cli_json = lambda *a, dry_run=False: {"accounts": [{"currency": "USD"}]}
        self.assertEqual(cb.list_accounts()["accounts"][0]["currency"], "USD")

    def test_detect_settlement_env_override(self):
        with patch.dict(os.environ, {"COINBASE_SETTLEMENT_CURRENCY": "EUR"}, clear=False):
            cb = make_cb()
            self.assertEqual(cb._detect_settlement_currency(), "EUR")

    def test_detect_settlement_usd_only(self):
        cb = make_cb()

        def usd_only(*args, dry_run=False):
            return {"accounts": [
                {"currency": "USD", "available_balance": {"value": "500"}},
                {"currency": "BTC", "available_balance": {"value": "0.1"}},
            ]}
        cb._cli_json = usd_only
        self.assertEqual(cb._detect_settlement_currency(), "USD")

    def test_detect_settlement_error_fallback(self):
        cb = make_cb()
        cb._cli_json = raiser
        self.assertEqual(cb._detect_settlement_currency(), "USD")


class TestEngineBranches(unittest.TestCase):
    def setUp(self):
        self.cb = make_cb()
        self.eng = NativeExecutionEngine(self.cb, dry_run=True)
        self.bm = BracketManager(self.eng)

    def test_preview_limit(self):
        res = self.eng._preview(OrderIntent(side="BUY", product_id="BTC-USD",
                                            order_type=OrderType.LIMIT, base_size="0.001",
                                            limit_price="50000"))
        self.assertTrue(res.success)

    def test_preview_stop_market_simulated(self):
        res = self.eng._preview(OrderIntent(side="SELL", product_id="BTC-USD",
                                            order_type=OrderType.STOP_MARKET, base_size="0.001",
                                            stop_price="59000"))
        self.assertTrue(res.success)

    def test_execute_invalid_type_fails(self):
        self.eng.dry_run = False
        intent = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET)
        intent.order_type = "bogus"  # bypass enum to hit the else branch
        res = self.eng.place(intent)
        self.assertFalse(res.success)

    def test_poll_status_cache_hit(self):
        cached = OrderResult(success=True, order_id="X", status=OrderStatus.FILLED)
        self.eng._orders["X"] = cached
        self.assertEqual(self.eng.poll_status("X"), cached)

    def test_poll_status_get_order_empty(self):
        self.cb.get_order = lambda oid: {}
        self.assertIsNone(self.eng.poll_status("missing"))

    def test_check_bracket_stop_only(self):
        b = self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=130)
        bid = b["entry_order"].client_order_id
        b["stop_order_id"] = "S1"
        b["target_order_id"] = None   # only stop is tracked
        self.cb.get_order = lambda oid: {"order_id": oid, "status": "FILLED",
                                         "average_filled_price": "90", "filled_size": "0.001"}
        closed = self.bm._check_bracket_status(bid, b)
        self.assertTrue(closed)
        self.assertEqual(b["exit_reason"], "stop")

    def test_check_bracket_target_only(self):
        b = self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=130)
        bid = b["entry_order"].client_order_id
        b["stop_order_id"] = None
        b["target_order_id"] = "T1"   # only target is tracked
        self.cb.get_order = lambda oid: {"order_id": oid, "status": "FILLED",
                                         "average_filled_price": "130", "filled_size": "0.001"}
        closed = self.bm._check_bracket_status(bid, b)
        self.assertTrue(closed)
        self.assertEqual(b["exit_reason"], "target")

    def test_force_flatten_market_fallback(self):
        # close_position exists but raises -> force_flatten must fall back to a market order.
        self.cb.close_position = lambda *a, **k: raiser()
        eng = NativeExecutionEngine(self.cb, dry_run=False)
        bm = BracketManager(eng)
        b = bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                             entry_price=100, stop_price=90, target_price=130)
        bid = b["entry_order"].client_order_id
        res = bm.force_flatten_bracket(bid, reason="test")
        self.assertEqual(res["status"], "CLOSED")

    def test_trailing_skips_non_open_bracket(self):
        b = self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=130)
        bid = b["entry_order"].client_order_id
        b["status"] = "FAILED"
        self.assertFalse(self.bm.update_trailing_stop(bid, current_price=115, highest_price=120,
                                                      lowest_price=95, initial_stop_dist=10,
                                                      r_multiple=2.0, max_hold_s=86400, age_s=3600))

    def test_trailing_rejects_bad_side(self):
        b = self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=130)
        bid = b["entry_order"].client_order_id
        self.bm._brackets[bid]["side"] = "HODL"
        self.assertFalse(self.bm.update_trailing_stop(bid, current_price=115, highest_price=120,
                                                      lowest_price=95, initial_stop_dist=10,
                                                      r_multiple=2.0, max_hold_s=86400, age_s=3600))

    def test_reconcile_fresh_no_events(self):
        b = self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=130)
        events = self.bm.reconcile_open_brackets(stale_after_s=15, force_flatten_after_s=60)
        self.assertEqual(events, [])
        self.assertIn(b["entry_order"].client_order_id, self.bm.active_brackets())

    def test_reconcile_cancels_stale(self):
        b = self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=130)
        bid = b["entry_order"].client_order_id
        b["stop_order_id"] = "S1"
        b["target_order_id"] = "T1"
        b["timestamp"] = int(time.time()) - 30  # stale but < force_flatten
        events = self.bm.reconcile_open_brackets(stale_after_s=15, force_flatten_after_s=60)
        self.assertEqual(events, [])  # not yet force-flattened
        self.assertEqual(b["status"], "OPEN")  # still open after cancel


if __name__ == "__main__":
    unittest.main(verbosity=2)
