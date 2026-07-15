import math
from types import SimpleNamespace

from trading_system.strategies.mean_reversion.zscore_mean_reversion import (
    ZScoreMeanReversionStrategy,
    ZScoreConfig,
)


def attach(s):
    """Provide the missing _log_error mixin method this StrategyBase subclass expects."""
    s._log_error = lambda msg: None
    return s


def const_prices(n=60, v=100.0):
    return [{"close": v} for _ in range(n)]


def make_bar(close, open_=None):
    return {"close": close, "open": open_ if open_ is not None else close,
            "high": close, "low": close}


def test_init_valid():
    s = attach(ZScoreMeanReversionStrategy())
    s.init(const_prices(60))
    assert len(s.price_buffer) == 60


def test_init_dict_single():
    s = attach(ZScoreMeanReversionStrategy())
    s.init({"close": 100.0})
    assert s.price_buffer == [100.0]


def test_init_dict_zero_close_ignored():
    s = attach(ZScoreMeanReversionStrategy())
    s.init({"close": 0})
    assert s.price_buffer == []


def test_init_insufficient_valid_prices_logs_error():
    errs = []
    s = ZScoreMeanReversionStrategy()
    s._log_error = errs.append
    s.init(const_prices(20))
    assert errs
    assert len(s.price_buffer) == 20


def test_init_bad_data_calls_log_error():
    errs = []
    s = ZScoreMeanReversionStrategy()
    s._log_error = errs.append
    s._calculate_position_size = lambda p: 1.0
    s.init([{"close": "not_a_number"}])
    assert errs


def test_config_validate_bounds():
    import pytest
    with pytest.raises(ValueError):
        ZScoreConfig(lookback_bars=10).validate()
    with pytest.raises(ValueError):
        ZScoreConfig(z_score_threshold=1.0).validate()


def test_get_available_strategies():
    from trading_system.strategies.mean_reversion.zscore_mean_reversion import (
        get_available_strategies,
    )
    names = get_available_strategies()
    assert "ZScoreMeanReversionStrategy" in names


def test_init_dict_buffer_short_returns():
    s = attach(ZScoreMeanReversionStrategy())
    s.init({"close": 100.0})
    # buffer still below lookback -> no statistics, no raise
    assert len(s.price_buffer) == 1


def test_on_bar_zero_close_holds():
    s = attach(ZScoreMeanReversionStrategy())
    s.init(const_prices(60))
    assert s.on_bar({"close": 0}).action == "HOLD"


def test_on_bar_buy_entry():
    s = attach(ZScoreMeanReversionStrategy())
    s.init(const_prices(60))
    sig = s.on_bar(make_bar(50.0, open_=100.0))
    assert sig.action == "BUY"
    assert sig.signal_type == "ZSCORE_MEAN_REVERSION"
    assert sig.confidence > 0
    assert sig.stop_loss is not None


def test_on_bar_near_stop_skips():
    s = attach(ZScoreMeanReversionStrategy())
    s.init(const_prices(60))
    sig = s.on_bar(make_bar(60.0, open_=50.0))
    assert sig.action == "HOLD"


def test_on_bar_std_zero_holds():
    s = attach(ZScoreMeanReversionStrategy())
    s.init(const_prices(60))
    # all identical -> std 0
    sig = s.on_bar(make_bar(100.0, open_=100.0))
    assert sig.action == "HOLD"


def test_on_bar_exit_take_profit():
    s = attach(ZScoreMeanReversionStrategy())
    s.init(const_prices(60))
    s.position = SimpleNamespace(price=100.0, quantity=1)
    sig = s.on_bar(make_bar(120.0))
    assert sig.action == "CLOSE"
    assert sig.confidence == 0.95


def test_on_bar_exit_stop_loss():
    s = attach(ZScoreMeanReversionStrategy())
    s.init(const_prices(60))
    s.position = SimpleNamespace(price=100.0, quantity=1)
    sig = s.on_bar(make_bar(89.0))
    assert sig.action == "CLOSE"
    assert sig.confidence == 1.0


def test_on_bar_exit_max_drawdown():
    s = attach(ZScoreMeanReversionStrategy())
    s.init(const_prices(60))
    s.position = SimpleNamespace(price=100.0, quantity=1)
    sig = s.on_bar(make_bar(97.0))
    assert sig.action == "CLOSE"
    assert sig.confidence == 0.85


def test_on_bar_position_no_exit_holds():
    s = attach(ZScoreMeanReversionStrategy())
    s.init(const_prices(60))
    s.position = SimpleNamespace(price=100.0, quantity=1)
    assert s.on_bar(make_bar(100.0)).action == "HOLD"


def test_on_bar_position_sizing_none_holds():
    s = attach(ZScoreMeanReversionStrategy())
    s.config.position_size_usd = 0.0
    s.init(const_prices(60))
    sig = s.on_bar(make_bar(50.0, open_=100.0))
    assert sig.action == "HOLD"


def test_calc_statistics_returns_none_on_short():
    s = attach(ZScoreMeanReversionStrategy())
    s.price_buffer = []
    assert s._calculate_statistics() == (None, None)


def test_calc_position_size_vol_scaling_on():
    s = attach(ZScoreMeanReversionStrategy())
    s.init(const_prices(60))
    q = s._calculate_position_size(100.0)
    assert q is not None and q > 0


def test_calc_position_size_vol_scaling_off():
    s = attach(ZScoreMeanReversionStrategy())
    s.config.volatility_scaling = False
    s.init(const_prices(60))
    q = s._calculate_position_size(100.0)
    assert q is not None


def test_calc_position_size_no_usd():
    s = attach(ZScoreMeanReversionStrategy())
    s.config.position_size_usd = 0.0
    assert s._calculate_position_size(100.0) is None


def test_finalize_returns_dict():
    s = attach(ZScoreMeanReversionStrategy())
    s.init(const_prices(60))
    out = s.finalize()
    assert "total_trades" in out
    assert s.price_buffer == []
