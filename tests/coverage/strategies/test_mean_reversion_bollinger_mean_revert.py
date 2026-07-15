import math

from trading_system.strategies.mean_reversion.bollinger_mean_revert import (
    BollingerBandMeanReversionStrategy,
    BollingerBandMeanRevertConfig,
    BBPosition,
)


def hist(n=40, v=100.0):
    return [{"close": v} for _ in range(n)]


def bar(close):
    return {"close": close}


def test_config_defaults():
    cfg = BollingerBandMeanRevertConfig()
    assert cfg.period == 20
    assert cfg.num_std == 2.0


def test_init_too_short_raises():
    s = BollingerBandMeanReversionStrategy()
    try:
        s.init(hist(25))
        assert False
    except ValueError:
        pass


def test_init_empty_raises():
    s = BollingerBandMeanReversionStrategy()
    try:
        s.init([])
        assert False
    except ValueError:
        pass


def test_init_ok():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    assert s.middle_band and s.band_widths


def test_compute_empty():
    s = BollingerBandMeanReversionStrategy()
    assert s._compute_bollinger_statistics([]) == ([], [])


def test_on_bar_zero_close_none():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    assert s.on_bar(bar(0)) is None


def test_on_bar_nan_close_none():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    assert s.on_bar(bar(float("nan"))) is None


def test_on_bar_buy():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    sig = s.on_bar(bar(50.0))
    assert sig["action"] == "BUY"
    assert sig["entry_price"] == 50.0
    assert sig["signal_type"] == "BOLLINGER_LOWER_BAND_BREACH"
    assert sig["stop_loss"] is not None


def test_on_bar_sell():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    sig = s.on_bar(bar(200.0))
    assert sig["action"] == "SELL"
    assert sig["signal_type"] == "BOLLINGER_UPPER_BAND_BREACH"


def test_on_bar_hold():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    assert s.on_bar(bar(100.0)) is None


def test_on_bar_position_present_holds():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    s.position = BBPosition(entry_price=100.0, entry_z_score=-1.8, quantity=10)
    assert s.on_bar(bar(50.0)) is None


def test_handle_signal_buy():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    s.handle_signal({"action": "BUY", "entry_price": 100.0})
    assert isinstance(s.position, BBPosition)
    assert s.position.entry_price == 100.0


def test_handle_signal_sell_success():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    s.handle_signal({"action": "BUY", "entry_price": 100.0})
    s.handle_signal({"action": "SELL", "entry_price": 110.0})
    m = s.get_performance_metrics()
    assert m["successful_trades"] == 1
    assert m["win_rate"] == 100.0


def test_handle_signal_sell_fail():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    s.handle_signal({"action": "BUY", "entry_price": 100.0})
    s.handle_signal({"action": "SELL", "entry_price": 90.0})
    m = s.get_performance_metrics()
    assert m["failed_trades"] == 1
    assert m["win_rate"] == 0.0


def test_handle_signal_sell_no_position():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    assert s.handle_signal({"action": "SELL"}) is None


def test_get_current_position():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    assert s.get_current_position() is None


def test_metrics_empty():
    s = BollingerBandMeanReversionStrategy()
    s.init(hist(40))
    m = s.get_performance_metrics()
    assert m["total_signals"] == 0
