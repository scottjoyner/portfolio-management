from types import SimpleNamespace

from trading_system.strategies.mean_reversion.keltner_channel_range_bound import (
    KeltnerChannelRangeBoundStrategy,
    KeltnerChannelRangeBoundConfig,
)


def attach(s):
    s._log_error = lambda msg: None
    s._calculate_position_size = lambda p: 1000.0 / p if p and p > 0 else None
    return s


def varying_prices(n=40):
    return [{"close": float(100 + (i % 5) * 2)} for i in range(n)]


def bar(close, high=None, low=None, open_=None):
    return {"close": close, "high": high if high is not None else close,
            "low": low if low is not None else close,
            "open": open_ if open_ is not None else close}


def test_config_defaults():
    cfg = KeltnerChannelRangeBoundConfig()
    assert cfg.donchian_period == 20


def test_init_list():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init(varying_prices(40))
    assert s.atr_value is not None
    assert s.ma_donchian is not None


def test_init_dict():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init({"close": 100.0})
    assert s.price_buffer == [100.0]


def test_init_dict_zero_ignored():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init({"close": 0})
    assert s.price_buffer == []


def test_init_bad_data_logs_error():
    errs = []
    s = KeltnerChannelRangeBoundStrategy()
    s._log_error = errs.append
    s._calculate_position_size = lambda p: 1.0
    s.init([{"close": "bad"}])
    assert errs


def test_on_bar_insufficient_buffer_holds():
    s = attach(KeltnerChannelRangeBoundStrategy())
    assert s.on_bar(bar(100.0)).action == "HOLD"


def test_on_bar_flat_holds():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init([{"close": 100.0}] * 40)
    # constant prices -> zero price ranges -> HOLD
    assert s.on_bar(bar(100.0)).action == "HOLD"


def test_on_bar_zero_price_in_buffer_holds():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init(varying_prices(40))
    s.price_buffer[-1] = 0.0
    assert s.on_bar(bar(100.0)).action == "HOLD"


def test_on_bar_all_zero_buffer_holds():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init(varying_prices(40))
    s.price_buffer = [0.0] * len(s.price_buffer)
    assert s.on_bar(bar(50.0)).action == "HOLD"


def test_on_bar_short_buffer_fill():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.price_buffer = [100.0] * 5
    sig = s.on_bar(bar(50.0))
    assert sig.action in ("BUY", "HOLD", "SELL")


def test_on_bar_buy_entry():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init(varying_prices(40))
    sig = s.on_bar(bar(50.0))
    assert sig.action == "BUY"
    assert sig.confidence > 0


def test_on_bar_sell_entry():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init(varying_prices(40))
    sig = s.on_bar(bar(160.0))
    assert sig.action == "SELL"
    assert sig.confidence > 0


def test_on_bar_no_touch_holds():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init(varying_prices(40))
    assert s.on_bar(bar(100.0)).action == "HOLD"


def test_on_bar_exit_take_profit():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init(varying_prices(40))
    s.position = SimpleNamespace(price=100.0, quantity=1)
    sig = s.on_bar(bar(112.0))
    assert sig.action == "CLOSE"
    assert sig.confidence == 0.92


def test_on_bar_exit_stop_loss():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init(varying_prices(40))
    s.position = SimpleNamespace(price=100.0, quantity=1)
    sig = s.on_bar(bar(87.0))
    assert sig.action == "CLOSE"
    assert sig.confidence == 1.0


def test_on_bar_position_no_exit_holds():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init(varying_prices(40))
    s.position = SimpleNamespace(price=100.0, quantity=1)
    assert s.on_bar(bar(100.0)).action == "HOLD"


def test_finalize_clears_state():
    s = attach(KeltnerChannelRangeBoundStrategy())
    s.init(varying_prices(40))
    out = s.finalize()
    assert isinstance(out, dict)
    assert s.price_buffer == []
