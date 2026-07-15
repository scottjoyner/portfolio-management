"""Coverage for EventTraderV4 core-holding rebalance trim (paper + live paths)."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

from coinbase.src.run_trader_v4 import EventTraderV4, CoreHolding  # noqa: E402


def _make_trader(**kw):
    kw.setdefault("dry_run", True)
    mode = kw.pop("mode", "paper")
    return EventTraderV4(mode=mode, products=["BTC-USD", "ETH-USD", "SOL-USD"], **kw)


def _holding(qty=1.0, price=100.0):
    return CoreHolding(
        product_id="BTC-USD", qty=qty, cost_basis=price, total_cost=price * qty,
        total_qty=qty, target_value=1000.0,
    )


def test_rebalance_trim_paper():
    t = _make_trader()
    t._last_price = {"BTC-USD": 100.0}
    h = _holding()
    t._core_holdings = {"BTC-USD": h}
    t._core_dca_cooldown_s = 10.0
    t._paper_equity = lambda: 100000.0
    t.paper_cash = 0.0
    t._rebalance_trim("BTC-USD", gap_value=500.0, cfg={})
    assert t.paper_cash > 0


def test_rebalance_trim_no_price():
    t = _make_trader()
    t._last_price = {}
    t._core_holdings = {"BTC-USD": _holding()}
    before = t.paper_cash
    t._rebalance_trim("BTC-USD", 500.0, cfg={})
    assert t.paper_cash == before


def test_rebalance_trim_no_holding():
    t = _make_trader()
    t._last_price = {"BTC-USD": 100.0}
    t._core_holdings = {}
    before = t.paper_cash
    t._rebalance_trim("BTC-USD", 500.0, cfg={})
    assert t.paper_cash == before


def test_rebalance_trim_cooldown():
    t = _make_trader()
    t._last_price = {"BTC-USD": 100.0}
    h = _holding()
    h.last_buy_ts = time.time()
    t._core_holdings = {"BTC-USD": h}
    t._core_dca_cooldown_s = 10.0
    before = t.paper_cash
    t._rebalance_trim("BTC-USD", 500.0, cfg={})
    assert t.paper_cash == before


def test_rebalance_trim_tiny_gap():
    t = _make_trader()
    t._last_price = {"BTC-USD": 100.0}
    t._core_holdings = {"BTC-USD": _holding()}
    t._core_dca_cooldown_s = 10.0
    before = t.paper_cash
    t._rebalance_trim("BTC-USD", gap_value=0.5, cfg={})
    assert t.paper_cash == before


def test_rebalance_trim_live():
    t = _make_trader(mode="live")
    t._last_price = {"BTC-USD": 100.0}
    t._core_holdings = {"BTC-USD": _holding()}
    t._core_dca_cooldown_s = 10.0
    t._exec_engine = MagicMock()
    cb = MagicMock()
    cb.preview_order.return_value = {"order_id": "p1"}
    cb.market_order.return_value = {"status": "DONE", "filled_size": 0.125,
                                    "avg_price": 100.0, "fees": 0.1}
    t._cb_client = cb
    t._cb_breached = False
    t._rebalance_trim("BTC-USD", gap_value=500.0, cfg={})
    cb.market_order.assert_called_once()
