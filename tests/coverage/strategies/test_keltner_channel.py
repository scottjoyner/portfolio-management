"""Coverage tests for trend.keltner_channel (was on_bar IndexError)."""
import pytest

from trading_system.strategies.trend.keltner_channel import (
    KeltnerChannelBreakoutStrategy,
    KeltnerChannelConfig,
)


def _hist(n=50):
    return [
        {"high": 100 + i, "low": 90 + i, "close": 100 + i, "volume": 1000}
        for i in range(n)
    ]


def test_init_errors_and_config_override():
    s = KeltnerChannelBreakoutStrategy()
    with pytest.raises(ValueError):
        s.init([])
    with pytest.raises(ValueError):
        s.init(_hist(10))
    s.init(_hist(50), config=KeltnerChannelConfig(ema_period=10, atr_lookback_periods=10))
    assert len(s.ema_values) == len(s.high_prices) == 50


def test_calculate_atr_short_history():
    s = KeltnerChannelBreakoutStrategy()
    s.ema_values = [100.0]
    s.high_prices = [100.0]
    s.low_prices = [90.0]
    assert s._calculate_atr() >= 0
    s.high_prices = []
    assert s._calculate_atr() == 1.0


def test_bands_none_without_data():
    s = KeltnerChannelBreakoutStrategy()
    assert s._calculate_keltner_bands() == (None, None, 0.0)
    assert s.on_bar({"close": 100}) is None


def test_on_bar_invalid_price():
    s = KeltnerChannelBreakoutStrategy()
    s.init(_hist())
    assert s.on_bar({"close": 0}) is None


def test_on_bar_buy_then_sell():
    s = KeltnerChannelBreakoutStrategy()
    s.init(_hist())
    # strong breakout -> BUY
    buy = None
    for i in range(10):
        buy = s.on_bar({"high": 400 + i * 20, "low": 200, "close": 400 + i * 20, "volume": 1000})
        if buy:
            break
    assert buy["action"] == "BUY"
    s.handle_signal(buy)
    assert s.get_current_position() is not None
    # crash the price -> SELL (breakdown or stop loss)
    sell = None
    for c in (100, 50, 10, 1):
        sell = s.on_bar({"high": c, "low": c, "close": c, "volume": 1000})
        if sell:
            break
    assert sell is not None and sell["action"] == "SELL"


def test_handle_signal_and_metrics():
    s = KeltnerChannelBreakoutStrategy()
    assert s.get_performance_metrics()["total_signals"] == 0
    s.handle_signal({"action": "BUY", "entry_price": 100, "upper_band": 110,
                     "lower_band": 90, "middle_ema": 100})
    s.handle_signal({"action": "SELL", "entry_price": 120})  # win
    s.handle_signal({"action": "BUY", "entry_price": 100, "upper_band": 110,
                     "lower_band": 90, "middle_ema": 100})
    s.handle_signal({"action": "SELL", "entry_price": 80})  # loss
    s.handle_signal({"action": "SELL", "entry_price": 50})  # no position -> noop
    m = s.get_performance_metrics()
    assert m["failed_trades"] == 1 and m["successful_trades"] >= 1


def test_no_signal_hold():
    s = KeltnerChannelBreakoutStrategy()
    s.init(_hist())
    # a middling price near the channel -> no breakout, no position
    assert s.on_bar({"high": 150, "low": 148, "close": 149, "volume": 1000}) is None


def test_on_bar_buy_then_stop_loss():
    s = KeltnerChannelBreakoutStrategy()
    s.init(_hist())
    buy = None
    for i in range(10):
        buy = s.on_bar({"high": 400 + i * 20, "low": 200, "close": 400 + i * 20, "volume": 1000})
        if buy:
            break
    s.handle_signal(buy)
    # price above the lower-band sell threshold but below the hard stop level
    # -> STOP_LOSS_HIT branch (not the breakdown branch).
    sell = s.on_bar({"high": 300, "low": 300, "close": 300, "volume": 1000})
    assert sell is not None and sell["signal_type"] == "STOP_LOSS_HIT"


def test_on_bar_buy_low_high_invalid():
    s = KeltnerChannelBreakoutStrategy()
    s.init(_hist())
    # default bar helper uses close 0 -> invalid -> None
    assert s.on_bar({"close": 0}) is None
