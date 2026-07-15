from types import SimpleNamespace

from trading_system.strategies.mean_reversion.zscore_statistical_arb import (
    ZScoreStatisticalArbStrategy,
    ZScoreStatisticalArbConfig,
    ZScorePosition,
)


def hist(n=40, v=100.0):
    return [{"close": v} for _ in range(n)]


def bar(close, high=None, low=None, open_=None):
    return {"close": close, "high": high if high is not None else close,
            "low": low if low is not None else close,
            "open": open_ if open_ is not None else close}


def test_config_defaults():
    cfg = ZScoreStatisticalArbConfig()
    assert cfg.lookback_period == 20


def test_init_too_short_raises():
    s = ZScoreStatisticalArbStrategy()
    try:
        s.init(hist(25))
        assert False
    except ValueError:
        pass


def test_init_empty_raises():
    s = ZScoreStatisticalArbStrategy()
    try:
        s.init([])
        assert False
    except ValueError:
        pass


def test_init_ok():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    assert s.rolling_means and s.rolling_stds


def test_compute_empty():
    s = ZScoreStatisticalArbStrategy()
    assert s._compute_rolling_statistics([]) == ([], [])


def test_on_bar_zero_none():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    assert s.on_bar(bar(0)) is None


def test_on_bar_nan_none():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    assert s.on_bar(bar(float("nan"))) is None


def test_on_bar_buy():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    sig = s.on_bar(bar(50.0))
    assert sig["action"] == "BUY"
    assert sig["signal_type"] == "ZSCORE_BELOW_MEAN"
    assert sig["stop_loss"] is not None


def test_on_bar_sell():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    sig = s.on_bar(bar(160.0))
    assert sig["action"] == "SELL"
    assert sig["signal_type"] == "ZSCORE_ABOVE_MEAN"


def test_on_bar_hold():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    assert s.on_bar(bar(100.0)) is None


def test_on_bar_position_reversion_target():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    s.position = SimpleNamespace(entry_price=100.0, unrealized_pnl_pct=0.5,
                                entry_z_score=-1.5, quantity=1)
    sig = s.on_bar(bar(100.0))
    assert sig["action"] == "SELL"
    assert sig["signal_type"] == "ZSCORE_REVERSION_TARGET"


def test_on_bar_position_no_target_holds():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    s.position = SimpleNamespace(entry_price=100.0, unrealized_pnl_pct=0.0,
                                entry_z_score=-1.5, quantity=1)
    assert s.on_bar(bar(100.0)) is None


def test_handle_signal_buy():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    s.handle_signal({"action": "BUY", "entry_price": 100.0})
    assert isinstance(s.position, ZScorePosition)
    assert s.position.entry_price == 100.0


def test_handle_signal_sell_success():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    s.handle_signal({"action": "BUY", "entry_price": 100.0})
    s.handle_signal({"action": "SELL", "entry_price": 110.0})
    m = s.get_performance_metrics()
    assert m["successful_trades"] == 1


def test_handle_signal_sell_fail():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    s.handle_signal({"action": "BUY", "entry_price": 100.0})
    s.handle_signal({"action": "SELL", "entry_price": 90.0})
    m = s.get_performance_metrics()
    assert m["failed_trades"] == 1


def test_handle_signal_sell_no_position():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    assert s.handle_signal({"action": "SELL"}) is None


def test_get_current_position():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    assert s.get_current_position() is None


def test_metrics_empty():
    s = ZScoreStatisticalArbStrategy()
    s.init(hist(40))
    assert s.get_performance_metrics()["total_signals"] == 0
