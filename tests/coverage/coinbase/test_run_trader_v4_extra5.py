"""Coverage for EventTraderV4 live trailing-stop + live exit-check logic."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

from coinbase.src.run_trader_v4 import EventTraderV4  # noqa: E402


def _make_trader(**kw):
    kw.setdefault("dry_run", True)
    mode = kw.pop("mode", "paper")
    return EventTraderV4(mode=mode, products=["BTC-USD", "ETH-USD", "SOL-USD"], **kw)


def _bm(active):
    m = MagicMock()
    m._brackets = active
    m.active_brackets.return_value = active
    m.update_trailing_stop = MagicMock()
    m.force_flatten_bracket = MagicMock()
    return m


# ----------------------------------------------------------- minute live trailing
def test_minute_live_trailing_happy():
    t = _make_trader()
    t._last_price = {"BTC-USD": 100.0}
    t._live_positions = {"BTC-USD": {"entry_price": 90.0, "size": 1.0, "side": "LONG"}}
    t.health_status = {"market_regime": "trending"}
    t.max_hold_s = 3600
    b = {"product_id": "BTC-USD", "timestamp": time.time(), "highest_price": 95.0,
         "lowest_price": 88.0, "stop_price": 80.0, "initial_stop_dist": 10.0}
    t._bracket_mgr = _bm({"b1": b})
    t._minute_live_trailing()
    t._bracket_mgr.update_trailing_stop.assert_called_once()


def test_minute_live_trailing_no_mgr():
    t = _make_trader()
    t._bracket_mgr = None
    t._minute_live_trailing()  # no-op


def test_minute_live_trailing_no_active():
    t = _make_trader()
    m = _bm({})
    m.active_brackets.return_value = {}
    t._bracket_mgr = m
    t._minute_live_trailing()  # no-op


def test_minute_live_trailing_no_live_pos():
    t = _make_trader()
    t._last_price = {"BTC-USD": 100.0}
    t._live_positions = {}
    b = {"product_id": "BTC-USD", "timestamp": time.time()}
    t._bracket_mgr = _bm({"b1": b})
    t._minute_live_trailing()
    t._bracket_mgr.update_trailing_stop.assert_not_called()


# ------------------------------------------------------------- minute live exit
def test_minute_live_exit_time():
    t = _make_trader()
    t.max_hold_s = 1.0
    t.paper_min_hold_s = 0
    b = {"product_id": "BTC-USD", "timestamp": time.time() - 100}
    t._bracket_mgr = _bm({"b1": b})
    t._live_positions = {"BTC-USD": {"entry_price": 90, "size": 1, "side": "LONG"}}
    t._minute_live_exit_check([])
    t._bracket_mgr.force_flatten_bracket.assert_called_once()


def test_minute_live_exit_reverse():
    t = _make_trader()
    t.max_hold_s = 10 ** 9
    t.paper_min_hold_s = 0
    b = {"product_id": "BTC-USD", "timestamp": time.time()}
    t._bracket_mgr = _bm({"b1": b})
    t._live_positions = {"BTC-USD": {"entry_price": 90, "size": 1, "side": "LONG"}}
    opps = [{"product_id": "BTC-USD", "action": "SELL", "confidence": 0.9, "strategy": "x"}]
    t._minute_live_exit_check(opps)
    t._bracket_mgr.force_flatten_bracket.assert_called_once()


def test_minute_live_exit_consensus():
    t = _make_trader()
    t.max_hold_s = 10 ** 9
    t.paper_min_hold_s = 0
    b = {"product_id": "BTC-USD", "timestamp": time.time()}
    t._bracket_mgr = _bm({"b1": b})
    t._live_positions = {"BTC-USD": {"entry_price": 90, "size": 1, "side": "LONG"}}
    opps = [{"product_id": "BTC-USD", "action": "SELL", "confidence": 0.5, "strategy": "x"} for _ in range(3)]
    t._minute_live_exit_check(opps)
    t._bracket_mgr.force_flatten_bracket.assert_called_once()


def test_minute_live_exit_no_signal():
    t = _make_trader()
    t.max_hold_s = 10 ** 9
    t.paper_min_hold_s = 0
    b = {"product_id": "BTC-USD", "timestamp": time.time()}
    t._bracket_mgr = _bm({"b1": b})
    t._live_positions = {"BTC-USD": {"entry_price": 90, "size": 1, "side": "LONG"}}
    t._minute_live_exit_check([])
    t._bracket_mgr.force_flatten_bracket.assert_not_called()


def test_minute_live_exit_no_mgr():
    t = _make_trader()
    t._bracket_mgr = None
    t._minute_live_exit_check([])  # no-op
