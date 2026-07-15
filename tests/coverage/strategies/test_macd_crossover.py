"""Coverage + behaviour tests for trend.macd_crossover (was syntax/runtime broken)."""
import pytest

from trading_system.strategies.factory import Signal
from trading_system.strategies.trend.macd_crossover import (
    MACDConfig,
    MACDCrossoverStrategy,
)


def _series(strat, prices):
    out = []
    for p in prices:
        out.append(strat.on_bar({"close": p}))
    return out


def test_init_validation():
    s = MACDCrossoverStrategy(MACDConfig(fast_ema_period=4))
    with pytest.raises(ValueError):
        s.init({})
    s = MACDCrossoverStrategy(MACDConfig(fast_ema_period=12, slow_ema_period=10))
    with pytest.raises(ValueError):
        s.init({})
    s = MACDCrossoverStrategy(MACDConfig(signal_ema_period=2))
    with pytest.raises(ValueError):
        s.init({})
    # valid
    MACDCrossoverStrategy().init({})


def test_invalid_prices_hold():
    s = MACDCrossoverStrategy()
    assert s.on_bar({"close": "bad"}).action == "HOLD"
    assert s.on_bar({"close": 0}).action == "HOLD"
    assert s.on_bar({"close": -5}).action == "HOLD"


def test_warmup_holds():
    s = MACDCrossoverStrategy()
    # first few bars: not enough history for slow EMA
    for p in range(100, 110):
        assert s.on_bar({"close": p}).action == "HOLD"


def test_crossovers_generate_both_signals():
    s = MACDCrossoverStrategy()
    # rise, fall, rise -> produces bullish and bearish crossovers
    prices = list(range(100, 160)) + list(range(160, 100, -1)) + list(range(100, 160))
    sigs = _series(s, prices)
    actions = {sig.action for sig in sigs if sig}
    assert "BUY" in actions
    assert "SELL" in actions


def test_ema_helper():
    s = MACDCrossoverStrategy()
    assert s._calculate_ema([1.0], 12, []) is None  # too short
    val = s._calculate_ema([float(i) for i in range(30)], 12, [])
    assert val is not None
    # len < period branch
    val2 = s._calculate_ema([1.0, 2.0, 3.0], 12, [])
    assert val2 is not None


def test_position_exit_bearish():
    s = MACDCrossoverStrategy()
    # warm up in an uptrend
    _series(s, list(range(100, 160)))
    s.position = Signal(action="BUY", quantity=1.0)
    # now downtrend -> bearish exit for a long
    sigs = _series(s, list(range(160, 100, -1)))
    assert any(sig and sig.action == "SELL" for sig in sigs)


def test_position_exit_short_side():
    s = MACDCrossoverStrategy()
    _series(s, list(range(160, 100, -1)))
    s.position = Signal(action="SELL", quantity=1.0)
    sigs = _series(s, list(range(100, 160)))
    assert any(sig and sig.action == "BUY" for sig in sigs)


def test_trailing_exit_helper():
    s = MACDCrossoverStrategy()
    s.position = None
    assert s._check_trailing_exit(100, 1.0) is False  # no position -> exception path
    s.position = Signal(action="BUY", quantity=1.0)
    s.macd_values = [10.0, 9.0, 8.0]
    # current_price*indicator very small vs recent max -> triggers
    assert s._check_trailing_exit(0.0001, 0.0001) is True


def test_calculate_quantity():
    s = MACDCrossoverStrategy()
    assert s._calculate_quantity(100) is not None
    assert s._calculate_quantity(0) is None


def test_exception_path(monkeypatch):
    s = MACDCrossoverStrategy(MACDConfig(enable_logging=True))
    _series(s, list(range(100, 140)))

    def boom(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr(s, "_calculate_ema", boom)
    assert s.on_bar({"close": 150}).action == "HOLD"


def test_get_name_finalize():
    s = MACDCrossoverStrategy()
    assert s.get_name() == "MACDCrossover"
    assert s.finalize()["fast_period"] == s.fast_period
    s.position = Signal(action="BUY", price=100, quantity=1.0)
    assert "final_pnl" in s.finalize()
