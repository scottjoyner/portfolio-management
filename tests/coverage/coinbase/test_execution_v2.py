"""Tests for coinbase/src/execution_v2.py"""
from __future__ import annotations

import time
import threading
import uuid
from unittest.mock import MagicMock

import pytest

from coinbase.src.execution_v2 import (
    NativeExecutionEngine,
    BracketManager,
    OrderIntent,
    OrderResult,
    OrderType,
    OrderStatus,
    _fmt_base,
    _fmt_quote,
    _fmt_price,
)


class FakeCB:
    """Lightweight stand-in for CBClient for execution tests."""

    def __init__(self):
        self.calls = []
        self.settlement_currency = "USD"
        self._order_id = 0
        self.preview_result = {"preview_id": "p1", "status": "success"}
        self.market_result = None
        self.close_position_result = {"order_id": "close1", "status": "FILLED"}
        self.have_close_position = True

    def _next_id(self):
        self._order_id += 1
        return f"ord{self._order_id}"

    def preview_order(self, side, pid, **kw):
        self.calls.append(("preview_order", side, pid, kw))
        return dict(self.preview_result)

    def market_order(self, side, pid, **kw):
        self.calls.append(("market_order", side, pid, kw))
        if self.market_result is not None:
            return self.market_result
        return {
            "order_id": self._next_id(),
            "status": "FILLED",
            "average_filled_price": 100.0,
            "filled_size": 1.0,
            "total_fees": 0.5,
        }

    def create_limit_order(self, side, pid, **kw):
        self.calls.append(("create_limit_order", side, pid, kw))
        return {"order_id": self._next_id(), "status": "OPEN"}

    def create_stop_limit_order(self, side, pid, **kw):
        self.calls.append(("create_stop_limit_order", side, pid, kw))
        return {"order_id": self._next_id(), "status": "OPEN"}

    def create_stop_market_order(self, side, pid, **kw):
        self.calls.append(("create_stop_market_order", side, pid, kw))
        return {"order_id": self._next_id(), "status": "OPEN"}

    def cancel_order(self, order_id, **kw):
        self.calls.append(("cancel_order", order_id))
        return {"order_id": order_id}

    def get_order(self, order_id, **kw):
        self.calls.append(("get_order", order_id))
        return {}

    def close_position(self, pid, size=None, client_order_id=""):
        self.calls.append(("close_position", pid))
        if self.have_close_position:
            return dict(self.close_position_result)
        return None


# ── Formatting helpers ──────────────────────────────────────────────
def test_fmt_helpers():
    assert _fmt_base(1.5) == "1.5"
    assert _fmt_base(1.0) == "1"
    assert _fmt_quote(10.5) == "10.5"
    assert _fmt_price(10.1234) == "10.12"


# ── Engine: dry_run / preview ───────────────────────────────────────
def test_engine_place_dry_run_market_buy_quote():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=True)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100")
    res = eng.place(it)
    assert res.success
    assert it.client_order_id
    assert cb.calls  # preview called


def test_engine_place_dry_run_market_buy_base():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=True)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, base_size="0.1")
    res = eng.place(it)
    assert res.success


def test_engine_place_dry_run_market_buy_missing():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=True)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET)
    res = eng.place(it)
    assert not res.success


def test_engine_place_dry_run_market_sell_base():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=True)
    it = OrderIntent(side="SELL", product_id="BTC-USD", order_type=OrderType.MARKET, base_size="0.1")
    res = eng.place(it)
    assert res.success


def test_engine_place_dry_run_non_market():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=True)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.LIMIT,
                     base_size="0.1", limit_price="100")
    res = eng.place(it)
    assert res.success
    # No preview call for non-market in dry_run
    assert not any(c[0] == "preview_order" for c in cb.calls)


def test_engine_place_dry_run_exception():
    cb = FakeCB()
    cb.preview_order = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    eng = NativeExecutionEngine(cb, dry_run=True)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100")
    res = eng.place(it)
    assert not res.success


# ── Engine: live execute ────────────────────────────────────────────
def test_engine_execute_market_buy():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100")
    res = eng.place(it)
    assert res.success
    assert res.order_id
    assert res.fill_price == 100.0
    assert res.fees == 0.5


def test_engine_execute_market_buy_missing():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET)
    res = eng.place(it)
    assert not res.success


def test_engine_execute_market_sell():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    it = OrderIntent(side="SELL", product_id="BTC-USD", order_type=OrderType.MARKET, base_size="0.1")
    res = eng.place(it)
    assert res.success


def test_engine_execute_preview_error():
    cb = FakeCB()
    cb.preview_result = {"error": "bad", "status": "preview_error"}
    eng = NativeExecutionEngine(cb, dry_run=False)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100")
    res = eng.place(it)
    assert not res.success


def test_engine_execute_limit():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.LIMIT,
                     base_size="0.1", limit_price="100")
    res = eng.place(it)
    assert res.success


def test_engine_execute_stop_limit():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    it = OrderIntent(side="SELL", product_id="BTC-USD", order_type=OrderType.STOP_LIMIT,
                     base_size="0.1", limit_price="90", stop_price="95")
    res = eng.place(it)
    assert res.success


def test_engine_execute_stop_market():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    it = OrderIntent(side="SELL", product_id="BTC-USD", order_type=OrderType.STOP_MARKET,
                     base_size="0.1", stop_price="95")
    res = eng.place(it)
    assert res.success


def test_engine_execute_unknown_type():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type="WEIRD")
    res = eng.place(it)
    assert not res.success


def test_engine_execute_exception():
    cb = FakeCB()
    cb.market_order = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    eng = NativeExecutionEngine(cb, dry_run=False)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100")
    res = eng.place(it)
    assert not res.success


def test_engine_status_parsing():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    it = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100")
    res = eng.place(it)
    # poll_status finds from cache
    cached = eng.poll_status(res.order_id)
    assert cached is not None
    # unknown order id falls back to get_order (returns {})
    assert eng.poll_status("nope") is None


def test_engine_parse_listed_order():
    o = {"order_id": "x", "client_order_id": "c", "status": "FILLED",
         "average_filled_price": 50.0, "filled_size": 2.0, "total_fees": 1.0}
    r = NativeExecutionEngine._parse_listed_order(o)
    assert r.status == OrderStatus.FILLED
    assert r.fill_price == 50.0


def test_engine_parse_listed_order_bad_status():
    o = {"status": "NONSENSE"}
    r = NativeExecutionEngine._parse_listed_order(o)
    assert r.status == OrderStatus.OPEN


def test_engine_cancel():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    assert eng.cancel("id1") is True
    cb.cancel_order = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    assert eng.cancel("id1") is False


# ── BracketManager ──────────────────────────────────────────────────
def _new_bracket_manager(dry_run=True):
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=dry_run)
    return BracketManager(eng), cb, eng


def test_bracket_stop_polling():
    bm, _, _ = _new_bracket_manager()
    bm.stop_polling()
    assert bm._stop_polling.is_set()


def test_place_bracket_buy_dry_run():
    bm, cb, eng = _new_bracket_manager(dry_run=True)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    assert b["status"] == "OPEN"
    assert b["entry_price"] == 100.0
    # dry_run: no stop/tp orders placed
    assert b["stop_order_id"] is None


def test_place_bracket_buy_live():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    assert b["status"] == "OPEN"
    assert b["stop_order_id"] is not None
    assert b["target_order_id"] is not None


def test_place_bracket_use_fill_price():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    cb.market_result = {"order_id": "o1", "status": "FILLED", "average_filled_price": 105.0}
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    assert b["entry_price"] == 105.0


def test_place_bracket_buy_bad_stop():
    bm, cb, eng = _new_bracket_manager()
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 100.0, 120.0)  # stop >= entry
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, -1.0, 120.0)   # stop <= 0
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 100.0)   # target <= entry
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, -5.0)    # target <= 0


def test_place_bracket_sell_dry_run():
    bm, cb, eng = _new_bracket_manager(dry_run=True)
    b = bm.place_bracket("BTC-USD", "SELL", 0.1, 100.0, 110.0, 80.0)
    assert b["status"] == "OPEN"


def test_place_bracket_sell_bad():
    bm, cb, eng = _new_bracket_manager()
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "SELL", 0.1, 100.0, 90.0, 80.0)   # stop <= entry
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "SELL", 0.1, 100.0, 110.0, 100.0) # target >= entry
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "SELL", 0.1, 100.0, 110.0, -5.0)  # target <= 0


def test_place_bracket_bad_side():
    bm, cb, eng = _new_bracket_manager()
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "HOLD", 0.1, 100.0, 90.0, 120.0)


def test_place_bracket_zero_size():
    bm, cb, eng = _new_bracket_manager()
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "BUY", 0.0, 100.0, 90.0, 120.0)


def test_place_bracket_entry_failed():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    cb.market_order = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    assert b["status"] == "FAILED"


def test_active_brackets():
    bm, cb, eng = _new_bracket_manager()
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bm._brackets[b["entry_order"].client_order_id]["status"] = "CLOSED"
    assert bm.active_brackets() == {}


def test_reconcile_open_brackets():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    # Make it look stale
    bm._brackets[bid]["timestamp"] = int(time.time()) - 100
    events = bm.reconcile_open_brackets(stale_after_s=10, force_flatten_after_s=20)
    assert events


def test_force_flatten_close_position():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    out = bm.force_flatten_bracket(bid, reason="test")
    assert out["status"] == "CLOSED"
    assert bm._brackets[bid]["status"] == "CLOSED"


def test_force_flatten_market_fallback():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    cb.have_close_position = False  # close_position returns None -> market fallback
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    out = bm.force_flatten_bracket(bid)
    assert out["status"] == "CLOSED"


def test_force_flatten_missing():
    bm, cb, eng = _new_bracket_manager()
    out = bm.force_flatten_bracket("nope")
    assert out["status"] == "MISSING"


def test_force_flatten_not_open():
    bm, cb, eng = _new_bracket_manager()
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["status"] = "CLOSED"
    out = bm.force_flatten_bracket(bid)
    assert out["status"] == "CLOSED"


def test_force_flatten_failed():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    cb.have_close_position = False
    cb.market_order = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = list(bm._brackets.keys())[0]
    out = bm.force_flatten_bracket(bid)
    assert out["status"] == "FAILED"


def test_cancel_bracket_orders():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["stop_order_id"] = "s1"
    bm._brackets[bid]["target_order_id"] = "t1"
    bm._cancel_bracket_orders(bm._brackets[bid])
    assert ("cancel_order", "s1") in cb.calls or any(c[0] == "cancel_order" for c in cb.calls)
    # empty ids -> no error
    bm._cancel_bracket_orders({"stop_order_id": None, "target_order_id": None})


def test_check_bracket_status_none():
    bm, cb, eng = _new_bracket_manager()
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    # No order ids -> returns False
    assert bm._check_bracket_status(bid, bm._brackets[bid]) is False


def _set_order_status(eng, order_id, status, fill_price):
    cid = next(k for k, v in eng._orders.items() if v.order_id == order_id)
    eng._orders[cid] = OrderResult(success=True, order_id=order_id,
                                   status=status, fill_price=fill_price)


def test_check_bracket_status_stop_filled():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    soid = bm._brackets[bid]["stop_order_id"]
    _set_order_status(eng, soid, OrderStatus.FILLED, 91.0)
    assert bm._check_bracket_status(bid, bm._brackets[bid]) is True
    assert bm._brackets[bid]["exit_reason"] == "stop"


def test_check_bracket_status_target_filled():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    toid = bm._brackets[bid]["target_order_id"]
    _set_order_status(eng, toid, OrderStatus.FILLED, 121.0)
    assert bm._check_bracket_status(bid, bm._brackets[bid]) is True
    assert bm._brackets[bid]["exit_reason"] == "target"


def test_check_bracket_status_both_filled_prefers_target():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    soid = bm._brackets[bid]["stop_order_id"]
    toid = bm._brackets[bid]["target_order_id"]
    _set_order_status(eng, soid, OrderStatus.FILLED, 91.0)
    _set_order_status(eng, toid, OrderStatus.FILLED, 121.0)
    assert bm._check_bracket_status(bid, bm._brackets[bid]) is True
    assert bm._brackets[bid]["exit_reason"] == "target"


def test_poll_brackets_runs_once():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bm.poll_brackets(poll_secs=0)  # breaks immediately


def test_update_trailing_stop_dry_run_returns_false():
    bm, cb, eng = _new_bracket_manager(dry_run=True)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    assert bm.update_trailing_stop(bid, 110, 110, 110, 10, 2.0, 100, 10) is False


def test_update_trailing_stop_missing():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    assert bm.update_trailing_stop("nope", 110, 110, 110, 10, 2.0, 100, 10) is False


def test_update_trailing_stop_bad_side():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["side"] = "HOLD"
    assert bm.update_trailing_stop(bid, 110, 110, 110, 10, 2.0, 100, 10) is False


def test_update_trailing_stop_no_stop_order():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["stop_order_id"] = None
    assert bm.update_trailing_stop(bid, 110, 110, 110, 10, 2.0, 100, 10) is False


def test_update_trailing_stop_buy_breakeven():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    # high vol, r=2, age 0 -> should tighten and place new stop
    updated = bm.update_trailing_stop(bid, 120, 120, 100, 10, 2.0, 100, 10, regime="high_volatility")
    assert updated is True
    assert bm._brackets[bid]["breakeven_set"] is True


def test_update_trailing_stop_buy_no_tighten():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["stop_price"] = 95.0
    # price low so new stop below old -> no tighten
    updated = bm.update_trailing_stop(bid, 96, 96, 96, 5, 0.5, 100, 10)
    assert updated is False


def test_update_trailing_stop_sell():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "SELL", 0.1, 100.0, 110.0, 80.0)
    bid = b["entry_order"].client_order_id
    updated = bm.update_trailing_stop(bid, 85, 90, 85, 10, 2.0, 100, 10, regime="high_volatility")
    assert updated is True


def test_update_trailing_stop_sell_no_tighten():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "SELL", 0.1, 100.0, 110.0, 80.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["stop_price"] = 108.0
    updated = bm.update_trailing_stop(bid, 105, 105, 105, 5, 0.5, 100, 10)
    assert updated is False


def test_update_trailing_take_profit_dry_run():
    bm, cb, eng = _new_bracket_manager(dry_run=True)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    assert bm.update_trailing_take_profit(bid, 120, 120, 100, 10, 2.5, 100, 10) is False


def test_update_trailing_take_profit_missing():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    assert bm.update_trailing_take_profit("nope", 120, 120, 100, 10, 2.5, 100, 10) is False


def test_update_trailing_take_profit_bad_side():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["side"] = "HOLD"
    assert bm.update_trailing_take_profit(bid, 120, 120, 100, 10, 2.5, 100, 10) is False


def test_update_trailing_take_profit_no_target():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["target_order_id"] = None
    assert bm.update_trailing_take_profit(bid, 120, 120, 100, 10, 2.5, 100, 10) is False


def test_update_trailing_take_profit_below_threshold():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    # r<2 -> returns False
    assert bm.update_trailing_take_profit(bid, 110, 110, 100, 10, 1.0, 100, 10) is False


def test_update_trailing_take_profit_buy():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["target_price"] = 110.0
    updated = bm.update_trailing_take_profit(bid, 130, 130, 100, 10, 2.5, 100, 10)
    assert updated is True
    assert bm._brackets[bid]["trailing_tp_activated"] is True


def test_update_trailing_take_profit_buy_no_tighten():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "BUY", 0.1, 100.0, 90.0, 120.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["target_price"] = 130.0
    updated = bm.update_trailing_take_profit(bid, 125, 125, 100, 10, 3.0, 100, 10)
    assert updated is False


def test_update_trailing_take_profit_sell():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "SELL", 0.1, 100.0, 110.0, 80.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["target_price"] = 95.0
    updated = bm.update_trailing_take_profit(bid, 70, 90, 70, 10, 2.5, 100, 10)
    assert updated is True


def test_update_trailing_take_profit_sell_no_tighten():
    bm, cb, eng = _new_bracket_manager(dry_run=False)
    b = bm.place_bracket("BTC-USD", "SELL", 0.1, 100.0, 110.0, 80.0)
    bid = b["entry_order"].client_order_id
    bm._brackets[bid]["target_price"] = 70.0
    updated = bm.update_trailing_take_profit(bid, 75, 90, 75, 10, 3.0, 100, 10)
    assert updated is False
