"""
Offline unit tests for the execution engine + CBClient.

These do NOT touch the network. CBClient is built via ``__new__`` (bypassing the
network-aware ``__init__``) and its ``_cli_json`` is replaced with a recorder so we
can assert the exact CLI command strings the engine assembles. This is what catches
endpoint-shape regressions (e.g. ``type=stop`` vs ``type=stop_limit``, ``status==``
vs ``order_status=``, ``portfolios`` vs ``portfolio``) deterministically in CI.

Run:  python3 -m unittest tests.test_execution_unit -v
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from coinbase.src.cb_client import CBClient
from coinbase.src.execution_v2 import (
    NativeExecutionEngine,
    BracketManager,
    OrderIntent,
    OrderType,
    OrderStatus,
)


def make_cb(settlement: str = "USD"):
    """Build a CBClient without running __init__ (no network / no rate limiter)."""
    cb = CBClient.__new__(CBClient)
    cb.cli_env = "live"
    cb.dry_run_cli = False
    cb.settlement_currency = settlement
    cb._rate_limiter = None
    cb._last_calls = []

    def fake_cli(*args, dry_run=False):
        cb._last_calls.append((list(args), dry_run))
        # Default plausible success payload.
        return {"order_id": "O1", "status": "OPEN", "product_id": args[2] if len(args) > 2 else ""}

    cb._cli_json = fake_cli
    return cb


class TestCommandShapeContracts(unittest.TestCase):
    """Assert the exact CLI invocations match the real CDP CLI contract."""

    def test_create_stop_market_is_stop_limit(self):
        cb = make_cb()
        cb.create_stop_market_order("SELL", "BTC-USD", base_size="0.001",
                                    stop_price="59000", stop_direction="up")
        args, _ = cb._last_calls[-1]
        self.assertIn("orders", args)
        self.assertIn("create", args)
        self.assertIn("type=stop_limit", args)          # bare `stop` is rejected by CLI
        self.assertIn("limit_price=59000", args)        # emulated as stop_limit
        self.assertIn("stop_price=59000", args)
        self.assertIn("stop_direction=up", args)        # not stop_direction_stop_up

    def test_create_stop_limit_direction_down(self):
        cb = make_cb()
        cb.create_stop_limit_order("BUY", "BTC-USD", base_size="0.001",
                                   limit_price="50000", stop_price="55000",
                                   stop_direction="down")
        args, _ = cb._last_calls[-1]
        self.assertIn("type=stop_limit", args)
        self.assertIn("stop_direction=down", args)
        self.assertIn("limit_price=50000", args)
        self.assertIn("stop_price=55000", args)

    def test_create_limit_type(self):
        cb = make_cb()
        cb.create_limit_order("BUY", "BTC-USD", base_size="0.001", price="50000")
        args, _ = cb._last_calls[-1]
        self.assertIn("type=limit", args)
        self.assertIn("limit_price=50000", args)
        self.assertNotIn("stop_price", " ".join(args))

    def test_list_orders_uses_status_query_syntax(self):
        cb = make_cb()
        cb.list_orders(product_id="BTC-USD", status="OPEN")
        args, _ = cb._last_calls[-1]
        self.assertIn("orders", args)
        self.assertIn("list", args)
        self.assertIn("status==OPEN", args)             # not order_status=OPEN
        self.assertIn("product_ids=BTC-USD", args)

    def test_get_positions_uses_portfolios_not_portfolio(self):
        cb = make_cb()
        seq = []

        def fake_cli(*args, dry_run=False):
            seq.append(list(args))
            if args[0] == "portfolios" and args[1] == "list":
                return {"portfolios": [{"uuid": "PF1", "type": "DEFAULT", "name": "Default"}]}
            if args[0] == "portfolios" and args[1] == "get":
                return {"spot_positions": [
                    {"asset": "BTC", "total_balance_crypto": "0.5",
                     "total_balance_fiat": "30000", "available_to_trade_fiat": "30000",
                     "is_cash": False}]}
            return {}

        cb._cli_json = fake_cli
        positions = cb.get_positions()
        self.assertTrue(any(a[:2] == ["portfolios", "list"] for a in seq))
        self.assertTrue(any(a[:2] == ["portfolios", "get"] for a in seq))
        # Must NOT call the nonexistent `portfolio list`.
        self.assertFalse(any(a[:1] == ["portfolio"] for a in seq))
        self.assertEqual(positions[0]["product_id"], "BTC-USD")
        self.assertEqual(positions[0]["side"], "LONG")
        self.assertEqual(positions[0]["size"], 0.5)

    def test_dry_run_flag_propagates(self):
        cb = make_cb()
        cb.dry_run_cli = True
        cb.create_market_order("SELL", "BTC-USD", base_size="0.0001")
        args, dry = cb._last_calls[-1]
        self.assertTrue(dry, "mutating call must append --dry-run when dry_run_cli=True")

    def test_settlement_remap_in_commands(self):
        cb = make_cb(settlement="USDC")
        cb.create_market_order("BUY", "BTC-USD", quote_size="10")
        args, _ = cb._last_calls[-1]
        self.assertIn("product_id=BTC-USDC", args)      # remapped to settlement pair
        self.assertIn("type=market", args)


class TestParseHelpers(unittest.TestCase):
    def test_parse_cli_output_empty(self):
        self.assertEqual(CBClient._parse_cli_output("", False), {})

    def test_parse_cli_output_nonjson_raises(self):
        with self.assertRaises(RuntimeError):
            CBClient._parse_cli_output("not json", False)

    def test_parse_cli_output_dryrun_without_brace(self):
        # "--dry-run" output has no JSON body in some error cases.
        self.assertEqual(CBClient._parse_cli_output("would execute orders_create", True), {})

    def test_parse_cli_output_dryrun_json(self):
        out = 'would execute orders_create\n{\n  "product_id": "BTC-USD"\n}'
        self.assertEqual(CBClient._parse_cli_output(out, True).get("product_id"), "BTC-USD")

    def test_remap_helper(self):
        cb = make_cb(settlement="USDC")
        self.assertEqual(cb._remap("BTC-USD"), "BTC-USDC")
        self.assertEqual(cb._remap("ETH-USD"), "ETH-USDC")
        self.assertEqual(cb._remap("BTC-USDC"), "BTC-USDC")
        self.assertEqual(cb._remap("ETH-EUR"), "ETH-EUR")
        cb2 = make_cb(settlement="USD")
        self.assertEqual(cb2._remap("BTC-USD"), "BTC-USD")

    def test_detect_settlement_fallback_on_error(self):
        cb = make_cb()

        def boom(*a, dry_run=False):
            raise RuntimeError("no auth")
        cb._cli_json = boom
        self.assertEqual(cb._detect_settlement_currency(), "USD")


class TestEngineExecutePaths(unittest.TestCase):
    """Exercise NativeExecutionEngine with a fake client (no network)."""

    def setUp(self):
        self.cb = make_cb()

    def test_place_generates_client_order_id(self):
        eng = NativeExecutionEngine(self.cb, dry_run=True)
        intent = OrderIntent(side="BUY", product_id="BTC-USD",
                             order_type=OrderType.MARKET, quote_size="10")
        self.assertEqual(intent.client_order_id, "")
        res = eng.place(intent)
        self.assertNotEqual(intent.client_order_id, "")     # auto-generated
        self.assertEqual(res.client_order_id, intent.client_order_id)

    def test_execute_market_preview_error_fails(self):
        # Simulate a failing BUY preview (e.g. insufficient fund).
        def fake_cli(*args, dry_run=False):
            if args[0:2] == ("orders", "preview"):
                return {"status": "preview_error", "error": "insufficient fund"}
            return {"order_id": "O1", "status": "OPEN"}
        self.cb._cli_json = fake_cli
        eng = NativeExecutionEngine(self.cb, dry_run=False)
        res = eng.place(OrderIntent(side="BUY", product_id="BTC-USD",
                                    order_type=OrderType.MARKET, quote_size="10"))
        self.assertFalse(res.success)
        self.assertIn("insufficient fund", res.error)

    def test_execute_limit_success(self):
        eng = NativeExecutionEngine(self.cb, dry_run=False)
        res = eng.place(OrderIntent(side="BUY", product_id="BTC-USD",
                                    order_type=OrderType.LIMIT, base_size="0.001",
                                    limit_price="50000"))
        self.assertTrue(res.success)
        self.assertEqual(res.order_id, "O1")

    def test_cancel_returns_bool(self):
        eng = NativeExecutionEngine(self.cb, dry_run=False)
        self.assertTrue(eng.cancel("real-id"))
        # Force cancel to fail.
        def boom(*a, dry_run=False):
            raise RuntimeError("fail")
        self.cb._cli_json = boom
        self.assertFalse(eng.cancel("real-id"))

    def test_poll_status_from_get_order(self):
        eng = NativeExecutionEngine(self.cb, dry_run=False)
        self.cb.get_order = lambda oid: {"order_id": oid, "status": "FILLED",
                                         "average_filled_price": "111",
                                         "filled_size": "0.001", "total_fees": "0.1"}
        res = eng.poll_status("O9")
        self.assertIsNotNone(res)
        self.assertEqual(res.status, OrderStatus.FILLED)
        self.assertEqual(res.fill_price, 111.0)


class TestBracketValidation(unittest.TestCase):
    def setUp(self):
        self.cb = make_cb()
        self.eng = NativeExecutionEngine(self.cb, dry_run=False)
        self.bm = BracketManager(self.eng)

    def _seed(self, b, stop_id="S", target_id="T"):
        b["stop_order_id"] = stop_id
        b["target_order_id"] = target_id

    def test_long_bad_stop_raises(self):
        with self.assertRaises(ValueError):
            self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=100, target_price=120)

    def test_long_bad_target_raises(self):
        with self.assertRaises(ValueError):
            self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=100)

    def test_short_bad_stop_raises(self):
        with self.assertRaises(ValueError):
            self.bm.place_bracket("BTC-USD", "SELL", base_size=0.001,
                                  entry_price=100, stop_price=100, target_price=80)

    def test_zero_size_raises(self):
        with self.assertRaises(ValueError):
            self.bm.place_bracket("BTC-USD", "BUY", base_size=0.0,
                                  entry_price=100, stop_price=90, target_price=120)

    def test_invalid_side_raises(self):
        with self.assertRaises(ValueError):
            self.bm.place_bracket("BTC-USD", "HODL", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=120)


class TestBracketTrailingLogic(unittest.TestCase):
    def setUp(self):
        self.cb = make_cb()
        self.eng = NativeExecutionEngine(self.cb, dry_run=False)
        self.bm = BracketManager(self.eng)

    def _open_long(self):
        b = self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=130,
                                  strategy_id="ut")
        bid = b["entry_order"].client_order_id
        b["stop_order_id"] = "S1"
        b["target_order_id"] = "T1"
        return bid, b

    def test_long_trailing_tightens_up(self):
        bid, b = self._open_long()
        ok = self.bm.update_trailing_stop(bid, current_price=115, highest_price=120,
                                          lowest_price=95, initial_stop_dist=10,
                                          r_multiple=2.0, max_hold_s=86400, age_s=3600)
        self.assertTrue(ok)
        self.assertGreater(b["stop_price"], 90)   # moved up toward entry

    def test_long_trailing_never_loosens(self):
        bid, b = self._open_long()
        # First tighten.
        self.bm.update_trailing_stop(bid, current_price=115, highest_price=120,
                                     lowest_price=95, initial_stop_dist=10,
                                     r_multiple=2.0, max_hold_s=86400, age_s=3600)
        tightened = b["stop_price"]
        # Worse price action must NOT move the stop back down.
        ok = self.bm.update_trailing_stop(bid, current_price=108, highest_price=108,
                                          lowest_price=95, initial_stop_dist=10,
                                          r_multiple=0.5, max_hold_s=86400, age_s=3600)
        self.assertFalse(ok)
        self.assertEqual(b["stop_price"], tightened)

    def test_long_breakeven_at_1_5r(self):
        bid, b = self._open_long()
        # highest low enough that the trailing stop sits below entry, so breakeven
        # must pull it up to the entry price.
        self.bm.update_trailing_stop(bid, current_price=104, highest_price=104,
                                     lowest_price=95, initial_stop_dist=10,
                                     r_multiple=1.5, max_hold_s=86400, age_s=3600)
        self.assertTrue(b["breakeven_set"])
        self.assertEqual(b["stop_price"], 100.0)   # pulled to entry

    def test_high_volatility_widens_trailing(self):
        bid, b = self._open_long()
        self.bm.update_trailing_stop(bid, current_price=120, highest_price=120,
                                     lowest_price=95, initial_stop_dist=10,
                                     r_multiple=2.0, max_hold_s=86400, age_s=3600,
                                     regime="high_volatility")
        vol_stop = b["stop_price"]
        # Reset and run identical inputs in normal regime.
        bid2, b2 = self._open_long()
        self.bm.update_trailing_stop(bid2, current_price=120, highest_price=120,
                                     lowest_price=95, initial_stop_dist=10,
                                     r_multiple=2.0, max_hold_s=86400, age_s=3600,
                                     regime="trending")
        self.assertNotEqual(vol_stop, b2["stop_price"])  # wider trailing dist

    def test_long_trailing_tp_moves_up(self):
        bid, b = self._open_long()
        ok = self.bm.update_trailing_take_profit(bid, current_price=145, highest_price=145,
                                                 lowest_price=95, initial_stop_dist=10,
                                                 r_multiple=2.5, max_hold_s=86400, age_s=3600)
        self.assertTrue(ok)
        self.assertGreater(b["target_price"], 130)

    def test_trailing_tp_requires_r_threshold(self):
        bid, b = self._open_long()
        ok = self.bm.update_trailing_take_profit(bid, current_price=120, highest_price=120,
                                                 lowest_price=95, initial_stop_dist=10,
                                                 r_multiple=1.0, max_hold_s=86400, age_s=3600)
        self.assertFalse(ok)   # below 2R threshold

    def test_trailing_blocked_in_dry_run(self):
        dry_eng = NativeExecutionEngine(self.cb, dry_run=True)
        bm = BracketManager(dry_eng)
        b = bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                             entry_price=100, stop_price=90, target_price=130)
        b["stop_order_id"] = "S1"
        bid = b["entry_order"].client_order_id
        ok = bm.update_trailing_stop(bid, current_price=115, highest_price=120,
                                     lowest_price=95, initial_stop_dist=10,
                                     r_multiple=2.0, max_hold_s=86400, age_s=3600)
        self.assertFalse(ok)   # live trailing only acts when not dry_run


class TestBracketReconcile(unittest.TestCase):
    def setUp(self):
        self.cb = make_cb()
        self.eng = NativeExecutionEngine(self.cb, dry_run=False)
        self.bm = BracketManager(self.eng)

    def test_check_bracket_status_target_wins(self):
        b = self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=130)
        bid = b["entry_order"].client_order_id
        b["stop_order_id"] = "S1"
        b["target_order_id"] = "T1"
        # Both filled in same tick.
        self.cb.get_order = lambda oid: {"order_id": oid, "status": "FILLED",
                                         "average_filled_price": "130", "filled_size": "0.001"}
        closed = self.bm._check_bracket_status(bid, b)
        self.assertTrue(closed)
        self.assertEqual(b["exit_reason"], "target")   # target takes precedence

    def test_force_flatten_closes(self):
        b = self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=130)
        bid = b["entry_order"].client_order_id
        res = self.bm.force_flatten_bracket(bid, reason="test")
        self.assertEqual(res["status"], "CLOSED")
        self.assertNotIn(bid, self.bm.active_brackets())

    def test_reconcile_force_flattens_stale(self):
        b = self.bm.place_bracket("BTC-USD", "BUY", base_size=0.001,
                                  entry_price=100, stop_price=90, target_price=130)
        bid = b["entry_order"].client_order_id
        # Backdate the bracket far in the past.
        b["timestamp"] = int(__import__("time").time()) - 120
        events = self.bm.reconcile_open_brackets(stale_after_s=15, force_flatten_after_s=60)
        self.assertTrue(any(e["bracket_id"] == bid for e in events))
        self.assertEqual(b["status"], "CLOSED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
