"""Coverage for EventTraderV4 paper position opening + edge-skip branches."""
from __future__ import annotations

import time

import pytest
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


class _PartialFill:
    entry_price = 100.0
    partial_fill_pct = 0.5


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


def test_paper_open_low_confidence_unvetted():
    """The confidence gate blocks only when no vetted floor applies.

    The floor at run_trader_v4.py:3179 comes from the opportunity's own win_rate
    and sharpe against paper_min_win_rate/paper_min_sharpe. _opp() defaults
    (wr=0.65, sharpe=1.0) clear both, so this opportunity is vetted and its
    confidence is raised to the floor -- reported 0.1 is not what gets compared to
    paper_min_confidence. Drop win_rate and sharpe below the mins so the floor
    stays 0.0 and the gate actually sees the reported 0.1.
    """
    t = _make_trader()
    _wire(t)
    t._paper_open_position("BTC-USD", 100.0, _opp(conf=0.1, wr=0.10, sharpe=0.0))
    assert "BTC-USD" not in t.paper_positions


def test_vetted_floor_overrides_low_reported_confidence():
    """A vetted opportunity trades despite a low self-reported confidence.

    Deliberate: blocking it triggers the death spiral the guard documents --
    loses, live win rate drops, reported confidence is shrunk, notional falls under
    min_trade, and the bot can never re-enter. The floor is
    0.30 + wr*0.4 + sharpe*0.05, capped at 0.95.
    """
    t = _make_trader()
    _wire(t)
    t._paper_open_position("BTC-USD", 100.0, _opp(conf=0.1))
    assert "BTC-USD" in t.paper_positions
    assert abs(min(0.95, 0.30 + 0.65 * 0.4 + 1.0 * 0.05) - 0.61) < 1e-9


def test_paper_open_negative_kelly_falls_back_to_base_sizing():
    """Negative Kelly reduces sizing to base; it does not block entry.

    The kelly branch at run_trader_v4.py:3193 handles this on purpose: with sparse
    samples a negative Kelly is not evidence of a negative edge, and refusing to
    trade on it stalls the bot out entirely. Positive Kelly still sizes at
    half-Kelly.
    """
    t = _make_trader()
    _wire(t)
    t._perf_tracker.kelly.return_value = -0.1
    t._paper_open_position("BTC-USD", 100.0, _opp())
    assert "BTC-USD" in t.paper_positions


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


def test_paper_open_partial_fill_scales_qty():
    t = _make_trader()
    _wire(t)
    t._fill_model.estimate.return_value = _PartialFill()
    t._paper_open_position("BTC-USD", 100.0, _opp())
    partial_pos = t.paper_positions["BTC-USD"]

    # Compare against a full fill rather than hardcoding a number. Sizing is
    # fee-aware, so entry_notional is the fee-grossed notional scaled by the fill
    # fraction -- not the raw 0.5 * 14000 the old assertion expected. What matters
    # is that a 50% fill yields exactly half the full-fill position.
    full = _make_trader()
    _wire(full)
    full._fill_model.estimate.return_value = _Fill()
    full._paper_open_position("BTC-USD", 100.0, _opp())
    full_pos = full.paper_positions["BTC-USD"]

    assert partial_pos.qty == pytest.approx(full_pos.qty * 0.5)
    assert partial_pos.entry_notional == pytest.approx(full_pos.entry_notional * 0.5)
