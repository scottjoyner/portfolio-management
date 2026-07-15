"""Extra branch-coverage tests for coinbase/src/execution_v2.py (target >=90%)."""
from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import pytest

from coinbase.src.execution_v2 import (
    NativeExecutionEngine,
    BracketManager,
    OrderIntent,
    OrderResult,
    OrderType,
    OrderStatus,
)


class FlexCB:
    """Configurable fake CBClient covering failure paths per order type."""

    def __init__(self):
        self.calls = []
        self._oid = 0
        self.settlement_currency = "USD"
        self.raise_on_preview = False
        self.raise_on_market = False
        self.raise_on_limit = False
        self.raise_on_stop_market = False
        self.raise_on_cancel = False
        self.fail_preview = False
        self.close_position = self._close_position

    def _new_id(self):
        self._oid += 1
        return f"o{self._oid}"

    def _close_position(self, product_id, size=None, client_order_id=""):
        return {"order_id": self._new_id(), "status": "FILLED"}

    def preview_order(self, side, product_id, **kw):
        if self.raise_on_preview:
            raise RuntimeError("preview boom")
        if self.fail_preview:
            return {"error": "bad", "status": "preview_error"}
        return {"preview_id": f"p{self._oid + 1}", "status": "success"}

    def market_order(self, side, product_id, **kw):
        if self.raise_on_market:
            raise RuntimeError("market boom")
        return {"order_id": self._new_id(), "status": "FILLED",
                "average_filled_price": 100.0, "filled_size": 1.0, "total_fees": 0.1}

    def create_limit_order(self, side, product_id, **kw):
        if self.raise_on_limit:
            raise RuntimeError("limit boom")
        return {"order_id": self._new_id(), "status": "OPEN"}

    def create_stop_limit_order(self, side, product_id, **kw):
        return {"order_id": self._new_id(), "status": "OPEN"}

    def create_stop_market_order(self, side, product_id, **kw):
        if self.raise_on_stop_market:
            raise RuntimeError("stop boom")
        return {"order_id": self._new_id(), "status": "OPEN"}

    def cancel_order(self, order_id):
        if self.raise_on_cancel:
            raise RuntimeError("cancel boom")
        return {"order_id": order_id}

    def get_order(self, order_id):
        return {"order_id": order_id, "status": "FILLED", "average_filled_price": 100.0,
                "filled_size": 1.0, "total_fees": 0.0}


def _bid(bm, b):
    for k, v in bm._brackets.items():
        if v is b:
            return k
    return None


# ---------------------------------------------------------------------------
def test_poll_status_generic_exception():
    cb = FlexCB()
    cb.get_order = MagicMock(side_effect=RuntimeError("boom"))
    eng = NativeExecutionEngine(cb, dry_run=True)
    assert eng.poll_status("xyz") is None  # cache miss + get_order raises


def test_place_bracket_sell_target_ge_entry():
    eng = NativeExecutionEngine(FlexCB(), dry_run=True)
    bm = BracketManager(eng)
    # stop valid (>entry), target >= entry -> raises at target validation
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "SELL", 1.0, 100.0, 110.0, 100.0)


def test_place_bracket_take_profit_placement_failure():
    cb = FlexCB()
    cb.raise_on_limit = True
    eng = NativeExecutionEngine(cb, dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    # entry succeeded, tp placement failed -> warning path, brackets still OPEN
    assert b["status"] == "OPEN"
    assert b["target_order_id"] is None
    assert b["stop_order_id"] is not None


def test_poll_brackets_loop_and_wait():
    eng = NativeExecutionEngine(FlexCB(), dry_run=True)
    bm = BracketManager(eng)
    open_b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    # add a non-OPEN bracket to exercise the `continue` branch
    closed_b = bm.place_bracket("ETH-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bm._brackets[_bid(bm, closed_b)]["status"] = "CLOSED"

    t = threading.Thread(target=bm.poll_brackets, kwargs={"poll_secs": 1}, daemon=True)
    t.start()
    time.sleep(0.05)
    bm.stop_polling()
    t.join(timeout=5)
    assert not t.is_alive()


def test_reconcile_closed_event():
    cb = FlexCB()
    cb.get_order = lambda oid: {"order_id": oid, "status": "FILLED",
                                "average_filled_price": "90", "filled_size": "1"}
    eng = NativeExecutionEngine(cb, dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    bm._brackets[bid]["stop_order_id"] = "S1"
    bm._brackets[bid]["timestamp"] = int(time.time()) - 5
    # also include a non-OPEN bracket to exercise the `continue` branch
    closed = bm.place_bracket("ETH-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bm._brackets[_bid(bm, closed)]["status"] = "CLOSED"
    events = bm.reconcile_open_brackets(stale_after_s=15, force_flatten_after_s=60)
    assert events and events[0]["event"] == "closed"


def test_reconcile_cancel_exception_swallowed():
    cb = FlexCB()
    cb.raise_on_cancel = True
    cb.get_order = lambda oid: {"order_id": oid, "status": "OPEN"}
    eng = NativeExecutionEngine(cb, dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    bm._brackets[bid]["stop_order_id"] = "S1"
    bm._brackets[bid]["target_order_id"] = "T1"
    bm._brackets[bid]["timestamp"] = int(time.time()) - 30  # stale
    # cancel raises -> exception swallowed, no force_flatten (age < 60)
    events = bm.reconcile_open_brackets(stale_after_s=15, force_flatten_after_s=60)
    assert events == []
    assert bm._brackets[bid]["status"] == "OPEN"


def test_force_flatten_no_close_position_attr():
    cb = FlexCB()
    del cb.close_position  # force fallback to market order
    eng = NativeExecutionEngine(cb, dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    bm._brackets[bid]["timestamp"] = int(time.time()) - 2000
    res = bm.force_flatten_bracket(bid)
    assert res["status"] == "CLOSED"


def test_force_flatten_failed_no_close_position():
    cb = FlexCB()
    del cb.close_position
    cb.raise_on_market = True
    eng = NativeExecutionEngine(cb, dry_run=False)
    bm = BracketManager(eng)
    # Insert a bracket directly so the entry (market) order does not fail.
    bm._brackets["FB"] = {
        "product_id": "BTC-USD", "side": "BUY", "base_size": 1.0,
        "entry_price": 100.0, "stop_price": 90.0, "target_price": 110.0,
        "strategy_id": "bracket", "status": "OPEN", "entry_order": None,
        "stop_order_id": None, "target_order_id": None, "trailing_stop": None,
        "breakeven_set": False, "initial_stop_dist": 10.0,
        "highest_price": 100.0, "lowest_price": 100.0,
        "timestamp": int(time.time()) - 2000,
    }
    res = bm.force_flatten_bracket("FB")
    assert res["status"] == "FAILED"
    assert res["reason"] == "force_flatten_failed"


# ---- BUY trailing age-tighten thresholds -----------------------------------
@pytest.mark.parametrize("age_s,hold", [
    (90, 100),   # ratio .9 -> 0.2
    (75, 100),   # ratio .75 -> 0.4
    (50, 100),   # ratio .5 -> 0.6
    (25, 100),   # ratio .25 -> 0.8
])
def test_update_trailing_stop_buy_age_tighten(age_s, hold):
    eng = NativeExecutionEngine(FlexCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    # move highest high so new stop tightens
    ok = bm.update_trailing_stop(bid, 100.0, 130.0, 130.0, 10.0, 1.0, hold, age_s)
    assert ok is True


# ---- SHORT trailing stop (full branch coverage) ----------------------------
def test_update_trailing_stop_short_breakeven_and_tighten():
    eng = NativeExecutionEngine(FlexCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "SELL", 1.0, 100.0, 110.0, 90.0)
    bid = _bid(bm, b)
    # r>=1.5 breakeven + age ratio .9 tighten + cancel old + place new
    ok = bm.update_trailing_stop(bid, 100.0, 80.0, 80.0, 10.0, 2.5, 100.0, 90.0)
    assert ok is True
    assert bm._brackets[bid]["stop_price"] < 110.0


@pytest.mark.parametrize("age_s", [75, 50, 25])
def test_update_trailing_stop_short_age_tighten(age_s):
    eng = NativeExecutionEngine(FlexCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "SELL", 1.0, 100.0, 110.0, 90.0)
    bid = _bid(bm, b)
    # low r so no breakeven; age ratio .75/.5/.25 tighten
    ok = bm.update_trailing_stop(bid, 100.0, 80.0, 80.0, 10.0, 0.5, 100.0, age_s)
    assert ok is True


def test_update_trailing_stop_short_not_tighten():
    eng = NativeExecutionEngine(FlexCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "SELL", 1.0, 100.0, 110.0, 90.0)
    bid = _bid(bm, b)
    # lowest price near entry -> new stop >= old stop -> bail (no tighten)
    ok = bm.update_trailing_stop(bid, 100.0, 105.0, 105.0, 10.0, 0.5, 100.0, 1.0)
    assert ok is False


def test_update_trailing_stop_short_place_fail():
    cb = FlexCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "SELL", 1.0, 100.0, 110.0, 90.0)
    bid = _bid(bm, b)
    assert bm._brackets[bid]["status"] == "OPEN"
    # Now make the replacement stop order fail (bracket stays OPEN).
    cb.raise_on_stop_market = True
    ok = bm.update_trailing_stop(bid, 100.0, 80.0, 80.0, 10.0, 2.5, 100.0, 90.0)
    assert ok is False


def test_update_trailing_stop_short_no_old_stop():
    # stop_order_id falsy -> early return False (guard) before reaching cancel
    eng = NativeExecutionEngine(FlexCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "SELL", 1.0, 100.0, 110.0, 90.0)
    bid = _bid(bm, b)
    bm._brackets[bid]["stop_order_id"] = None
    ok = bm.update_trailing_stop(bid, 100.0, 80.0, 80.0, 10.0, 2.5, 100.0, 90.0)
    assert ok is False


# ---- SHORT trailing take-profit --------------------------------------------
def test_update_trailing_tp_short():
    eng = NativeExecutionEngine(FlexCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "SELL", 1.0, 100.0, 110.0, 90.0)
    bid = _bid(bm, b)
    # at 2R: trail TP down as lowest falls below 80 (new target < old 90)
    ok = bm.update_trailing_take_profit(bid, 100.0, 70.0, 70.0, 10.0, 2.0, 100.0, 20.0)
    assert ok is True
    assert bm._brackets[bid]["target_price"] < 90.0


def test_update_trailing_tp_long_not_tighten():
    eng = NativeExecutionEngine(FlexCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    # highest price low -> new target <= old target -> bail (no tighten)
    ok = bm.update_trailing_take_profit(bid, 100.0, 100.0, 100.0, 10.0, 2.0, 100.0, 20.0)
    assert ok is False


def test_update_trailing_tp_short_not_tighten():
    eng = NativeExecutionEngine(FlexCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "SELL", 1.0, 100.0, 110.0, 90.0)
    bid = _bid(bm, b)
    # lowest price high -> new target >= old -> bail
    ok = bm.update_trailing_take_profit(bid, 100.0, 95.0, 95.0, 10.0, 2.0, 100.0, 20.0)
    assert ok is False


def test_update_trailing_tp_short_no_target_id():
    eng = NativeExecutionEngine(FlexCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "SELL", 1.0, 100.0, 110.0, 90.0)
    bid = _bid(bm, b)
    bm._brackets[bid]["target_order_id"] = None
    ok = bm.update_trailing_take_profit(bid, 100.0, 80.0, 80.0, 10.0, 2.0, 100.0, 20.0)
    assert ok is False


def test_update_trailing_tp_short_place_fail():
    cb = FlexCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "SELL", 1.0, 100.0, 110.0, 90.0)
    bid = _bid(bm, b)
    assert bm._brackets[bid]["status"] == "OPEN"
    # Now make the replacement TP order fail (bracket stays OPEN).
    cb.raise_on_limit = True
    ok = bm.update_trailing_take_profit(bid, 100.0, 70.0, 70.0, 10.0, 2.0, 100.0, 20.0)
    assert ok is False


def test_engine_execute_unknown_status_from_listed():
    cb = FlexCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    o = NativeExecutionEngine._parse_listed_order(
        {"status": "PARTIALLY_FILLED", "average_filled_price": 1, "filled_size": 2, "total_fees": 3})
    assert o.status == OrderStatus.OPEN


def test_place_bracket_validates_before_entry():
    cb = FlexCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    bm = BracketManager(eng)
    with pytest.raises(ValueError):
        # stop >= entry is invalid for a BUY bracket
        bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 100.0, 110.0)
    # Correct behaviour: the entry order must NOT have been placed/stored
    # before validation passed. The entry is cancelled on validation failure.
    assert len(eng._orders) == 0
