"""Coverage tests for coinbase/src/execution_v2.py"""
from __future__ import annotations

import time
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


def _bid(bm, b):
    for k, v in bm._brackets.items():
        if v is b:
            return k
    return None


def test_fmt():
    assert _fmt_base(1.5) == "1.5"
    assert _fmt_base(1.0) == "1"
    assert _fmt_base(0.0) == "0"
    assert _fmt_quote(1.5) == "1.5"
    assert _fmt_price(1.5) == "1.50"


class FakeCB:
    def __init__(self):
        self.calls = []
        self.settlement_currency = "USD"
        self._oid = 0
        self.fail_preview = False
        self.raise_on_preview = False
        self.raise_on_market = False
        self.raise_on_stop_market = False

    def _new_id(self):
        self._oid += 1
        return f"o{self._oid}"

    def preview_order(self, side, product_id, **kw):
        self.calls.append(("preview", side, product_id, kw))
        if self.raise_on_preview:
            raise RuntimeError("preview boom")
        if self.fail_preview:
            return {"error": "bad preview", "status": "preview_error"}
        return {"preview_id": f"p{self._oid + 1}", "status": "success"}

    def market_order(self, side, product_id, **kw):
        self.calls.append(("market", side, product_id, kw))
        if self.raise_on_market:
            raise RuntimeError("market boom")
        return {"order_id": self._new_id(), "status": "FILLED",
                "average_filled_price": 100.0, "filled_size": 1.0, "total_fees": 0.1}

    def create_limit_order(self, side, product_id, **kw):
        self.calls.append(("limit", side, product_id, kw))
        return {"order_id": self._new_id(), "status": "OPEN"}

    def create_stop_limit_order(self, side, product_id, **kw):
        self.calls.append(("stop_limit", side, product_id, kw))
        return {"order_id": self._new_id(), "status": "OPEN"}

    def create_stop_market_order(self, side, product_id, **kw):
        self.calls.append(("stop_market", side, product_id, kw))
        if self.raise_on_stop_market:
            raise RuntimeError("stop boom")
        return {"order_id": self._new_id(), "status": "OPEN"}

    def cancel_order(self, order_id):
        self.calls.append(("cancel", order_id))
        return {"order_id": order_id}

    def get_order(self, order_id):
        return {"order_id": order_id, "status": "FILLED", "average_filled_price": 100.0,
                "filled_size": 1.0, "total_fees": 0.0}

    def close_position(self, product_id, size=None, client_order_id=""):
        return {"order_id": self._new_id(), "status": "FILLED"}


# ---------------------------------------------------------------------------
# NativeExecutionEngine
# ---------------------------------------------------------------------------
def test_engine_place_dry_run_preview():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=True)
    intent = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET,
                         quote_size="100")
    res = eng.place(intent)
    assert res.success
    assert intent.client_order_id  # auto uuid
    # non-market -> simulated
    intent2 = OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.LIMIT,
                          base_size="1", limit_price="100")
    res2 = eng.place(intent2)
    assert res2.success


def test_engine_preview_branches():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=True)
    # BUY quote
    r = eng._preview(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100"))
    assert r.success
    # BUY base
    r = eng._preview(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, base_size="1"))
    assert r.success
    # BUY neither -> raise -> FAILED
    r = eng._preview(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET))
    assert not r.success
    # SELL base
    r = eng._preview(OrderIntent(side="SELL", product_id="BTC-USD", order_type=OrderType.MARKET, base_size="1"))
    assert r.success
    # preview exception (raise) -> FAILED
    cb.raise_on_preview = True
    r = eng._preview(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100"))
    assert not r.success
    cb.raise_on_preview = False


def test_engine_execute_market():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    # BUY quote
    r = eng._execute(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100"))
    assert r.success and r.order_id
    # BUY base
    r = eng._execute(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, base_size="1"))
    assert r.success
    # BUY neither -> FAILED
    r = eng._execute(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET))
    assert not r.success
    # SELL base
    r = eng._execute(OrderIntent(side="SELL", product_id="BTC-USD", order_type=OrderType.MARKET, base_size="1"))
    assert r.success
    # preview error
    cb.fail_preview = True
    r = eng._execute(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100"))
    assert not r.success
    cb.fail_preview = False
    # preview_id passthrough
    r = eng._execute(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET,
                                 quote_size="100", preview_id="pid"))


def test_engine_execute_limit_stop():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    r = eng._execute(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.LIMIT,
                                 base_size="1", limit_price="100"))
    assert r.success
    r = eng._execute(OrderIntent(side="SELL", product_id="BTC-USD", order_type=OrderType.STOP_LIMIT,
                                 base_size="1", limit_price="90", stop_price="89"))
    assert r.success
    r = eng._execute(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.STOP_MARKET,
                                 base_size="1", stop_price="110", stop_direction="up"))
    assert r.success
    # unknown type
    r = eng._execute(OrderIntent(side="BUY", product_id="BTC-USD", order_type="weird"))
    assert not r.success


def test_engine_execute_status_parsing():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    # invalid status string -> OPEN
    eng._orders.clear()
    cb.calls.clear()
    # craft a raw with bad status via market_order override
    orig = cb.market_order
    cb.market_order = lambda *a, **k: {"order_id": "x", "status": "WEIRD",
                                       "average_filled_price": 5, "filled_size": 1, "total_fees": 0}
    r = eng._execute(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100"))
    assert r.status == OrderStatus.OPEN
    cb.market_order = orig
    # parse listed order invalid status
    o = NativeExecutionEngine._parse_listed_order({"status": "ZZZ"})
    assert o.status == OrderStatus.OPEN
    o2 = NativeExecutionEngine._parse_listed_order({"status": "FILLED", "average_filled_price": 1,
                                                     "filled_size": 2, "total_fees": 3})
    assert o2.filled_size == 2


def test_engine_place_execute_exception():
    cb = FakeCB()
    cb.raise_on_market = True
    eng = NativeExecutionEngine(cb, dry_run=False)
    r = eng.place(OrderIntent(side="BUY", product_id="BTC-USD", order_type=OrderType.MARKET, quote_size="100"))
    assert not r.success


def test_engine_cancel_and_poll():
    cb = FakeCB()
    eng = NativeExecutionEngine(cb, dry_run=False)
    # cache hit
    res = OrderResult(success=True, order_id="abc")
    eng._orders["cid"] = res
    eng._orders["abc"] = res
    assert eng.poll_status("abc") is res
    # fallback get_order
    assert eng.poll_status("xyz").order_id == "xyz"
    # get_order empty -> None
    cb.get_order = lambda oid: {}
    assert eng.poll_status("nope") is None
    # cancel success/fail
    assert eng.cancel("abc") is True
    cb.cancel_order = lambda oid: (_ for _ in ()).throw(RuntimeError("x"))
    assert eng.cancel("abc") is False


# ---------------------------------------------------------------------------
# BracketManager (dry_run)
# ---------------------------------------------------------------------------
def test_bracket_stop_polling():
    eng = NativeExecutionEngine(FakeCB(), dry_run=True)
    bm = BracketManager(eng)
    bm.stop_polling()
    assert bm._stop_polling.is_set()


def test_place_bracket_dry_run():
    eng = NativeExecutionEngine(FakeCB(), dry_run=True)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    assert b["status"] == "OPEN"
    assert b["entry_price"] == 100.0
    assert b["stop_order_id"] is None  # dry_run -> not placed
    assert any(x["product_id"] == "BTC-USD" for x in bm.active_brackets().values())


def test_place_bracket_entry_failed():
    cb = FakeCB()
    cb.raise_on_preview = True
    eng = NativeExecutionEngine(cb, dry_run=True)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    assert b["status"] == "FAILED"


@pytest.mark.parametrize("side,stop,target", [
    ("BUY", 100.0, 110.0),   # stop >= entry
    ("BUY", 90.0, 100.0),     # target <= entry
    ("SELL", 100.0, 90.0),    # stop >= entry (sell)
    ("SELL", 90.0, 100.0),    # target <= entry (sell)
])
def test_place_bracket_validation(side, stop, target):
    eng = NativeExecutionEngine(FakeCB(), dry_run=True)
    bm = BracketManager(eng)
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", side, 1.0, 100.0, stop, target)


def test_place_bracket_invalid_side_and_size():
    eng = NativeExecutionEngine(FakeCB(), dry_run=True)
    bm = BracketManager(eng)
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "HOLD", 1.0, 100.0, 90.0, 110.0)
    with pytest.raises(ValueError):
        bm.place_bracket("BTC-USD", "BUY", 0.0, 100.0, 90.0, 110.0)


def test_place_bracket_live_places_orders():
    eng = NativeExecutionEngine(FakeCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    assert b["status"] == "OPEN"
    assert b["stop_order_id"] is not None
    assert b["target_order_id"] is not None


def test_poll_brackets():
    eng = NativeExecutionEngine(FakeCB(), dry_run=True)
    bm = BracketManager(eng)
    bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    # poll_secs <= 0 -> breaks immediately
    bm.poll_brackets(poll_secs=0)


def test_reconcile_and_flatten():
    eng = NativeExecutionEngine(FakeCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    # not stale, no action
    ev = bm.reconcile_open_brackets(stale_after_s=15, force_flatten_after_s=60)
    assert ev == []
    # force flatten via age
    bm._brackets[bid]["timestamp"] = int(time.time()) - 1000
    ev = bm.reconcile_open_brackets(stale_after_s=15, force_flatten_after_s=60)
    assert ev and ev[0]["status"] == "CLOSED"


def test_force_flatten_missing_and_not_open():
    eng = NativeExecutionEngine(FakeCB(), dry_run=True)
    bm = BracketManager(eng)
    assert bm.force_flatten_bracket("nope")["status"] == "MISSING"
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bm._brackets[_bid(bm, b)]["status"] = "CLOSED"
    assert bm.force_flatten_bracket(_bid(bm, b))["status"] == "CLOSED"


def test_force_flatten_fallback_market():
    cb = FakeCB()
    cb.close_position = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x"))
    eng = NativeExecutionEngine(cb, dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bm._brackets[_bid(bm, b)]["timestamp"] = int(time.time()) - 2000
    res = bm.force_flatten_bracket(_bid(bm, b))
    assert res["status"] == "CLOSED"


def test_force_flatten_fail():
    cb = FakeCB()
    cb.close_position = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x"))
    eng = NativeExecutionEngine(cb, dry_run=False)
    eng._orders = {}
    # make market_order fail
    orig = cb.market_order
    cb.market_order = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x"))
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bm._brackets[_bid(bm, b)]["timestamp"] = int(time.time()) - 2000
    res = bm.force_flatten_bracket(_bid(bm, b))
    assert res["status"] == "FAILED"
    cb.market_order = orig


def test_check_bracket_status():
    eng = NativeExecutionEngine(FakeCB(), dry_run=True)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    # only stop set -> stop filled
    bm._brackets[bid]["stop_order_id"] = "stop1"
    assert bm._check_bracket_status(bid, bm._brackets[bid]) is True
    assert bm._brackets[bid]["exit_reason"] == "stop"
    # only target set -> target filled
    b2 = bm.place_bracket("ETH-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid2 = _bid(bm, b2)
    bm._brackets[bid2]["stop_order_id"] = None
    bm._brackets[bid2]["target_order_id"] = "t2"
    assert bm._check_bracket_status(bid2, bm._brackets[bid2]) is True
    assert bm._brackets[bid2]["exit_reason"] == "target"
    # both filled -> target wins
    bm._brackets[bid2]["status"] = "OPEN"
    bm._brackets[bid2]["stop_order_id"] = "s2"
    assert bm._check_bracket_status(bid2, bm._brackets[bid2]) is True
    assert bm._brackets[bid2]["exit_reason"] == "target"


def test_update_trailing_stop_live_long():
    eng = NativeExecutionEngine(FakeCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    # breakeven + tighten at 2.5R
    ok = bm.update_trailing_stop(bid, 100.0, 120.0, 120.0, 10.0, 2.5, 100.0, 20.0)
    assert ok is True
    assert bm._brackets[bid]["stop_price"] > 90.0


def test_update_trailing_stop_not_loosen():
    eng = NativeExecutionEngine(FakeCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    # highest below entry -> new stop would loosen -> bail
    ok = bm.update_trailing_stop(bid, 100.0, 95.0, 95.0, 10.0, 0.5, 100.0, 20.0)
    assert ok is False


def test_update_trailing_stop_age_tighten():
    eng = NativeExecutionEngine(FakeCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    ok = bm.update_trailing_stop(bid, 100.0, 130.0, 130.0, 10.0, 1.0, 100.0, 95.0)
    assert ok is True


def test_update_trailing_stop_place_fail():
    cb = FakeCB()
    cb.raise_on_stop_market = True
    eng = NativeExecutionEngine(cb, dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    ok = bm.update_trailing_stop(bid, 100.0, 120.0, 120.0, 10.0, 2.5, 100.0, 20.0)
    assert ok is False


def test_update_trailing_stop_short():
    eng = NativeExecutionEngine(FakeCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "SELL", 1.0, 100.0, 110.0, 90.0)
    bid = _bid(bm, b)
    ok = bm.update_trailing_stop(bid, 100.0, 80.0, 80.0, 10.0, 2.5, 100.0, 20.0)
    assert ok is True


def test_update_trailing_stop_invalid_and_dry():
    eng = NativeExecutionEngine(FakeCB(), dry_run=True)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    assert bm.update_trailing_stop(bid, 1, 1, 1, 10, 1, 1, 1) is False  # dry_run
    # missing
    assert bm.update_trailing_stop("nope", 1, 1, 1, 10, 1, 1, 1) is False
    # invalid side
    bm._brackets[bid]["side"] = "HOLD"
    assert bm.update_trailing_stop(bid, 1, 1, 1, 10, 1, 1, 1) is False


def test_update_trailing_tp_live():
    eng = NativeExecutionEngine(FakeCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    # at 2R -> start trailing, raise highest
    ok = bm.update_trailing_take_profit(bid, 100.0, 130.0, 130.0, 10.0, 2.0, 100.0, 20.0)
    assert ok is True


def test_update_trailing_tp_branches():
    eng = NativeExecutionEngine(FakeCB(), dry_run=False)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    # below 2R -> no trailing
    assert bm.update_trailing_take_profit(bid, 100.0, 105.0, 105.0, 10.0, 1.0, 100.0, 20.0) is False
    # 3R tighter
    assert bm.update_trailing_take_profit(bid, 100.0, 140.0, 140.0, 10.0, 3.0, 100.0, 20.0) is True


def test_update_trailing_tp_invalid_and_dry():
    eng = NativeExecutionEngine(FakeCB(), dry_run=True)
    bm = BracketManager(eng)
    b = bm.place_bracket("BTC-USD", "BUY", 1.0, 100.0, 90.0, 110.0)
    bid = _bid(bm, b)
    assert bm.update_trailing_take_profit(bid, 1, 1, 1, 10, 2, 1, 1) is False  # dry_run
    assert bm.update_trailing_take_profit("nope", 1, 1, 1, 10, 2, 1, 1) is False  # missing
    bm._brackets[bid]["side"] = "HOLD"
    assert bm.update_trailing_take_profit(bid, 1, 1, 1, 10, 2, 1, 1) is False  # invalid side
    bm._brackets[bid]["side"] = "BUY"
    bm._brackets[bid]["target_order_id"] = None
    assert bm.update_trailing_take_profit(bid, 1, 1, 1, 10, 2, 1, 1) is False  # no target id
