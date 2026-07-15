from trading_system.strategies.mean_reversion.williams_r_mean_revert import (
    WilliamsRMeanReversionStrategy,
    WilliamsRConfig,
)


def hist(n=25, v=100.0):
    return [{"close": v, "high": v + 2, "low": v - 2, "open": v} for _ in range(n)]


def test_config_defaults():
    cfg = WilliamsRConfig()
    assert cfg.period == 14


def test_init_ok():
    s = WilliamsRMeanReversionStrategy()
    s.init(hist(25))
    assert s.williams_r_values


def test_init_empty_raises():
    s = WilliamsRMeanReversionStrategy()
    try:
        s.init([])
        assert False
    except ValueError:
        pass


def test_init_too_short_raises():
    s = WilliamsRMeanReversionStrategy()
    try:
        s.init(hist(5))
        assert False
    except ValueError:
        pass


def test_on_bar_returns_signal():
    s = WilliamsRMeanReversionStrategy()
    s.init(hist(25))
    sig = s.on_bar({"close": 100.0, "high": 102, "low": 98, "open": 100})
    assert sig is not None and sig["action"] in ("BUY", "SELL")


def test_invalid_close_none():
    s = WilliamsRMeanReversionStrategy()
    s.init(hist(25))
    assert s.on_bar({"close": 0}) is None
    assert s.on_bar({"close": float("nan")}) is None


def test_handle_signal_buy():
    s = WilliamsRMeanReversionStrategy()
    s.init(hist(25))
    out = s.handle_signal({"action": "BUY", "entry_price": 100.0})
    assert out["position_opened"] is True


def test_handle_signal_sell():
    s = WilliamsRMeanReversionStrategy()
    s.init(hist(25))
    out = s.handle_signal({"action": "SELL"})
    assert out["position_closed"] is True
    m = s.get_performance_metrics()
    assert m["total_signals"] == 1
    assert m["win_rate"] == 100.0


def test_metrics_empty():
    s = WilliamsRMeanReversionStrategy()
    s.init(hist(25))
    m = s.get_performance_metrics()
    assert m["total_signals"] == 0
