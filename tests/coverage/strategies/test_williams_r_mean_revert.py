import pytest

from trading_system.strategies.mean_reversion.williams_r_mean_revert import (
    WilliamsRMeanReversionStrategy,
    WilliamsRConfig,
)


def _hist(n=25, base=100.0):
    return [{"open": base, "high": base + 2, "low": base - 2, "close": base,
             "volume": 1000.0 + i} for i in range(n)]


def test_init_ok():
    s = WilliamsRMeanReversionStrategy()
    s.init(_hist(25))
    assert s.williams_r_values


def test_init_empty_raises():
    with pytest.raises(ValueError):
        WilliamsRMeanReversionStrategy().init([])


def test_init_too_short_raises():
    with pytest.raises(ValueError):
        WilliamsRMeanReversionStrategy().init(_hist(5))


def test_on_bar_returns_signal():
    s = WilliamsRMeanReversionStrategy()
    s.init(_hist(25))
    sig = s.on_bar({"close": 100.0, "high": 102, "low": 98, "open": 100})
    assert sig is not None and sig["action"] in ("BUY", "SELL")


def test_invalid_close_none():
    s = WilliamsRMeanReversionStrategy()
    s.init(_hist(25))
    assert s.on_bar({"close": 0}) is None
    assert s.on_bar({"close": float("nan")}) is None


def test_handle_signal_and_metrics():
    s = WilliamsRMeanReversionStrategy()
    s.init(_hist(25))
    out = s.handle_signal({"action": "SELL"})
    assert out["position_closed"] is True
    m = s.get_performance_metrics()
    assert m["total_signals"] == 1
    assert m["win_rate"] == 100.0


def test_metrics_empty():
    s = WilliamsRMeanReversionStrategy()
    s.init(_hist(25))
    m = s.get_performance_metrics()
    assert m["total_signals"] == 0
