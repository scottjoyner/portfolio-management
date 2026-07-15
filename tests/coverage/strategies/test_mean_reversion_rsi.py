from types import SimpleNamespace

from trading_system.strategies.mean_reversion.rsi_mean_revert import (
    RSIMeanReversionStrategy,
    RSIConfig,
    RSIPosition,
)


def hist(n=30, pattern="inc"):
    if pattern == "inc":
        return [{"close": float(100 + i)} for i in range(n)]
    if pattern == "dec":
        return [{"close": float(200 - i)} for i in range(n)]
    if pattern == "osc":
        return [{"close": 100.0 + 10 * ((i % 2) * 2 - 1)} for i in range(n)]
    return [{"close": 100.0} for _ in range(n)]


def test_config_defaults():
    cfg = RSIConfig()
    assert cfg.rsi_period == 14


def test_init_empty_raises():
    s = RSIMeanReversionStrategy()
    try:
        s.init([])
        assert False
    except ValueError:
        pass


def test_init_too_short_raises():
    s = RSIMeanReversionStrategy()
    try:
        s.init(hist(10))
        assert False
    except ValueError:
        pass


def test_init_ok():
    s = RSIMeanReversionStrategy()
    s.init(hist(30))
    assert s.rsi_values


def test_calc_rsi_empty():
    s = RSIMeanReversionStrategy()
    assert s._calculate_rsi([]) == []


def test_on_bar_zero_none():
    s = RSIMeanReversionStrategy()
    s.init(hist(30))
    assert s.on_bar({"close": 0}) is None


def test_on_bar_nan_none():
    s = RSIMeanReversionStrategy()
    s.init(hist(30))
    assert s.on_bar({"close": float("nan")}) is None


def test_on_bar_sell_overbought():
    s = RSIMeanReversionStrategy()
    s.init(hist(30, "inc"))
    sig = s.on_bar({"close": 300.0})
    assert sig["action"] == "SELL"
    assert sig["signal_type"] == "RSI_OVERBOUGHT_SELL_SIGNAL"


def test_on_bar_hold():
    s = RSIMeanReversionStrategy()
    s.init(hist(30, "osc"))
    assert s.on_bar({"close": 100.0}) is None


def test_on_bar_position_present_holds():
    s = RSIMeanReversionStrategy()
    s.init(hist(30, "inc"))
    s.position = RSIPosition(entry_price=100.0, rsi_at_entry=70.0, quantity=10)
    assert s.on_bar({"close": 300.0}) is None


def test_handle_signal_buy():
    s = RSIMeanReversionStrategy()
    s.init(hist(30))
    s.handle_signal({"action": "BUY", "entry_price": 100.0})
    assert isinstance(s.position, RSIPosition)
    assert s.position.rsi_at_entry == s.config.rsi_oversold_threshold


def test_handle_signal_sell_success():
    s = RSIMeanReversionStrategy()
    s.init(hist(30))
    s.handle_signal({"action": "BUY", "entry_price": 100.0})
    s.handle_signal({"action": "SELL", "entry_price": 110.0})
    m = s.get_performance_metrics()
    assert m["successful_trades"] == 1


def test_handle_signal_sell_fail():
    s = RSIMeanReversionStrategy()
    s.init(hist(30))
    s.handle_signal({"action": "BUY", "entry_price": 100.0})
    s.handle_signal({"action": "SELL", "entry_price": 90.0})
    m = s.get_performance_metrics()
    assert m["failed_trades"] == 1


def test_handle_signal_sell_no_position():
    s = RSIMeanReversionStrategy()
    s.init(hist(30))
    assert s.handle_signal({"action": "SELL"}) is None


def test_get_current_position():
    s = RSIMeanReversionStrategy()
    s.init(hist(30))
    assert s.get_current_position() is None


def test_metrics_empty():
    s = RSIMeanReversionStrategy()
    s.init(hist(30))
    assert s.get_performance_metrics()["total_signals"] == 0
