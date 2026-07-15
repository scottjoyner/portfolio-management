"""Coverage for EventTraderV4 paper position opening + edge-skip branches."""
from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock

from coinbase.src.run_trader_v4 import EventTraderV4, Position  # noqa: E402


def _make_trader(**kw):
    kw.setdefault("dry_run", True)
    mode = kw.pop("mode", "paper")
    return EventTraderV4(mode=mode, products=["BTC-USD", "ETH-USD", "SOL-USD"], **kw)


class _Fill:
    entry_price = 100.0
    partial_fill_pct = 1.0


def _wire(t):
    t._perf_tracker = MagicMock()
    t._perf_tracker.get.return_value = None
    t._perf_tracker.kelly.return_value = 0.5
    t._perf_tracker.strategy_aggregate.return_value = {"trades": 0, "win_rate": 0.0}
    t._fill_model = MagicMock()
    t._fill_model.is_maker.return_value = False
    t._fill_model.estimate.return_value = _Fill()
    t._paper_equity = lambda: 100000.0
    t._paper_edge_model = lambda c, wr, s: {"gross_bps": 100, "fee_bps": 10,
                                            "latency_bps": 1, "net_bps": 89}
    t._paper_score_multiplier = lambda c, wr, s: 1.0
    t._btc_momentum_multiplier = lambda: 1.0
    t._fee_tier = lambda: (0, 60, 10)
    t._portfolio_risk = None
    t._feed_mgr = None
    t._last_volume_24h = {"BTC-USD": 1e9}
    t._last_price = {"BTC-USD": 100.0}
    t.paper_min_confidence = 0.55
    t.paper_min_win_rate = 0.60
    t.paper_min_sharpe = 0.8
    t.paper_min_edge_bps = 15.0
    t.paper_min_trade_usd = 100.0
    t.paper_max_position_pct = 0.2
    t.paper_max_new_positions = 12
    t.paper_product_cooldown_s = 1800.0
    t.max_leverage = 1.0
    t.enable_leverage = False
    t.paper_positions = {}
    t.paper_last_trade_ts = {}


def _opp(action="BUY", conf=0.7, wr=0.65, sharpe=1.0):
    return {"strategy": "ema_cross", "action": action, "confidence": conf,
            "win_rate": wr, "sharpe": sharpe, "atr_14": 2.0, "regime": "trending"}


def test_paper_open_long_happy():
    t = _make_trader()
    _wire(t)
    t._paper_open_position("BTC-USD", 100.0, _opp())
    assert "BTC-USD" in t.paper_positions
    assert t.paper_positions["BTC-USD"].is_long is True


def test_paper_open_short_happy():
    t = _make_trader()
    _wire(t)
    t._paper_open_position("BTC-USD", 100.0, _opp(action="SELL"))
    assert "BTC-USD" in t.paper_positions
    assert t.paper_positions["BTC-USD"].is_long is False


def test_paper_open_zero_price():
    t = _make_trader()
    _wire(t)
    t._paper_open_position("BTC-USD", 0.0, _opp())
    assert "BTC-USD" not in t.paper_positions


def test_paper_open_max_positions():
    t = _make_trader()
    _wire(t)
    t.paper_positions = {f"P{i}-USD": None for i in range(t.paper_max_new_positions)}
    t._paper_open_position("BTC-USD", 100.0, _opp())
    assert "BTC-USD" not in t.paper_positions


def test_paper_open_cooldown():
    t = _make_trader()
    _wire(t)
    t.paper_last_trade_ts["BTC-USD"] = time.time()
    t._paper_open_position("BTC-USD", 100.0, _opp())
    assert "BTC-USD" not in t.paper_positions


def test_paper_open_low_confidence():
    t = _make_trader()
    _wire(t)
    t._paper_open_position("BTC-USD", 100.0, _opp(conf=0.1))
    assert "BTC-USD" not in t.paper_positions


def test_paper_open_kelly_negative():
    t = _make_trader()
    _wire(t)
    t._perf_tracker.kelly.return_value = -0.1
    t._paper_open_position("BTC-USD", 100.0, _opp())
    assert "BTC-USD" not in t.paper_positions


def test_paper_open_low_edge():
    t = _make_trader()
    _wire(t)
    t._paper_edge_model = lambda c, wr, s: {"gross_bps": 10, "fee_bps": 9,
                                           "latency_bps": 5, "net_bps": -4}
    t._paper_open_position("BTC-USD", 100.0, _opp())
    assert "BTC-USD" not in t.paper_positions


def test_paper_open_with_portfolio_risk():
    t = _make_trader()
    _wire(t)
    pr = MagicMock()
    pr.get_cluster.return_value = "core"
    pr.check_pre_trade.return_value = (True, "ok", 25000.0)
    pr.update_positions.return_value = None
    pr.update_equity.return_value = None
    t._portfolio_risk = pr
    t._paper_open_position("BTC-USD", 100.0, _opp())
    assert "BTC-USD" in t.paper_positions
