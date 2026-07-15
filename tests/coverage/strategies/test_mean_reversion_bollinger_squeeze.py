import math
from types import SimpleNamespace

from trading_system.strategies.mean_reversion.bollinger_band_squeeze import (
    BollingerBandSqueezeStrategy,
    BollingerBandSqueezeConfig,
)


def attach(s):
    s._log_error = lambda msg: None
    s._calculate_position_size = lambda p: 1000.0 / p if p and p > 0 else None
    return s


def warm(s):
    """Seed a low-volatility history then one normal bar so prev_bb_width is small."""
    s.init([{"close": 100.0}] * 40)
    s.on_bar({"close": 100.0, "open": 100.0})


def test_config_defaults():
    cfg = BollingerBandSqueezeConfig()
    assert cfg.bb_period == 20


def test_init_list():
    s = attach(BollingerBandSqueezeStrategy())
    s.init([{"close": 100.0}] * 40)
    assert len(s.price_buffer) == 40


def test_init_dict():
    s = attach(BollingerBandSqueezeStrategy())
    s.init({"close": 100.0})
    assert s.price_buffer == [100.0]


def test_init_dict_zero_ignored():
    s = attach(BollingerBandSqueezeStrategy())
    s.init({"close": 0})
    assert s.price_buffer == []


def test_init_bad_data_logs_error():
    errs = []
    s = BollingerBandSqueezeStrategy()
    s._log_error = errs.append
    s._calculate_position_size = lambda p: 1.0
    s.init([{"close": "bad"}])
    assert errs


def test_on_bar_insufficient_buffer_holds():
    s = attach(BollingerBandSqueezeStrategy())
    # price_buffer empty -> < bb_period
    sig = s.on_bar({"close": 100.0, "open": 100.0})
    assert sig.action == "HOLD"


def test_on_bar_zero_close_holds():
    s = attach(BollingerBandSqueezeStrategy())
    s.init([{"close": 100.0}] * 40)
    assert s.on_bar({"close": 0}).action == "HOLD"


def test_on_bar_buy_entry():
    s = attach(BollingerBandSqueezeStrategy())
    warm(s)
    sig = s.on_bar({"close": 50.0, "open": 100.0})
    assert sig.action == "BUY"
    assert sig.squeeze is True or sig.squeeze is False
    assert sig.confidence > 0


def test_on_bar_sell_entry():
    s = attach(BollingerBandSqueezeStrategy())
    warm(s)
    sig = s.on_bar({"close": 160.0, "open": 100.0})
    assert sig.action == "SELL"
    assert sig.confidence > 0


def test_on_bar_no_touch_holds():
    s = attach(BollingerBandSqueezeStrategy())
    warm(s)
    sig = s.on_bar({"close": 100.0, "open": 100.0})
    assert sig.action == "HOLD"


def test_on_bar_exit_take_profit():
    s = attach(BollingerBandSqueezeStrategy())
    s.init([{"close": 100.0}] * 40)
    s.position = SimpleNamespace(price=100.0, quantity=1)
    sig = s.on_bar({"close": 106.0, "open": 100.0})
    assert sig.action == "CLOSE"
    assert sig.confidence == 0.90


def test_on_bar_exit_stop_loss():
    s = attach(BollingerBandSqueezeStrategy())
    s.init([{"close": 100.0}] * 40)
    s.position = SimpleNamespace(price=100.0, quantity=1)
    sig = s.on_bar({"close": 89.0, "open": 100.0})
    assert sig.action == "CLOSE"
    assert sig.confidence == 1.0


def test_on_bar_position_no_exit_holds():
    s = attach(BollingerBandSqueezeStrategy())
    s.init([{"close": 100.0}] * 40)
    s.position = SimpleNamespace(price=100.0, quantity=1)
    assert s.on_bar({"close": 100.0, "open": 100.0}).action == "HOLD"


def test_finalize_clears_state():
    s = attach(BollingerBandSqueezeStrategy())
    s.init([{"close": 100.0}] * 40)
    out = s.finalize()
    assert isinstance(out, dict)
    assert s.price_buffer == []
