"""Coverage + behaviour tests for trend.simple_momentum (was import/runtime broken)."""
import math

import pytest

from trading_system.strategies.factory import Signal
from trading_system.strategies.trend.simple_momentum import (
    MomentumConfig,
    SimpleMomentumStrategy,
)


def _fresh():
    s = SimpleMomentumStrategy()
    s.init({})
    return s


def test_init_validation():
    s = SimpleMomentumStrategy(MomentumConfig(momentum_periods=3))
    with pytest.raises(ValueError):
        s.init({})
    s = SimpleMomentumStrategy(MomentumConfig(momentum_threshold_pct=0))
    with pytest.raises(ValueError):
        s.init({})


def test_entry_signal():
    s = _fresh()
    sig = s.on_bar({"high": 110, "low": 90, "close": 200})
    assert sig.action == "BUY" and sig.signal_type == "MOMENTUM_BREAKOUT"


def test_no_entry_hold():
    s = _fresh()
    s.recent_highs = [100.0] * 10
    sig = s.on_bar({"high": 100, "low": 99, "close": 100})
    assert sig.action == "HOLD"


def test_recent_highs_grows_when_short():
    s = _fresh()
    s.recent_highs = [100.0, 100.0, 100.0]  # shorter than momentum_periods
    s.on_bar({"high": 105, "low": 95, "close": 100})
    assert len(s.recent_highs) == 4


def test_invalid_prices():
    s = _fresh()
    assert s.on_bar({"high": 1, "low": 1, "close": 0}).action == "HOLD"
    assert s.on_bar({"high": 1, "low": 1, "close": math.nan}).action == "HOLD"
    assert s.on_bar({"high": 1, "low": 1, "close": math.inf}).action == "HOLD"
    assert s.on_bar({"high": "x", "low": 1, "close": 1}).action == "HOLD"


def test_zero_avg_fallback():
    s = _fresh()
    s.recent_highs = [0.0] * 10  # avg == 0 -> fallback branch
    sig = s.on_bar({"high": 0, "low": 0, "close": 50})
    assert sig.action in ("BUY", "HOLD")


def test_exit_trailing_stop():
    s = _fresh()
    s.position = Signal(action="BUY", price=100, quantity=1.0)
    s.entry_price = 100.0
    sig = s.on_bar({"high": 130, "low": 120, "close": 125})
    assert sig.action == "SELL" and sig.signal_type == "MOMENTUM_TRAILING_STOP"


def test_exit_no_trailing_when_flat():
    s = _fresh()
    s.position = Signal(action="BUY", price=100, quantity=1.0)
    s.entry_price = 100.0
    # loss -> current_pct_gain <= 0 -> no trailing stop -> HOLD
    sig = s.on_bar({"high": 90, "low": 80, "close": 85})
    assert sig.action == "HOLD"


def test_calculate_quantity_edges():
    s = _fresh()
    assert s._calculate_quantity(100) == round(1500.0 / 100, 8)
    assert s._calculate_quantity(0) is None


def test_trailing_stop_none_when_no_gain():
    s = _fresh()
    s.position = Signal(action="BUY", price=100, quantity=1.0)
    assert s._calculate_trailing_stop(100, -5) is None
    # non-numeric price triggers the guarded TypeError/ValueError branch
    assert s._calculate_trailing_stop("bad", 5) is None


def test_get_name_and_finalize():
    s = _fresh()
    assert s.get_name() == "SimpleMomentum"
    assert s.finalize()["total_trades"] == 0
    s.position = Signal(action="BUY", price=100, quantity=1.0)
    out = s.finalize()
    assert "final_pnl" in out
