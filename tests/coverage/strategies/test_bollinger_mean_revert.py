from trading_system.strategies.mean_reversion.bollinger_mean_revert import (
    BollingerBandMeanReversionStrategy,
)


def _hist(n=40, base=100.0):
    return [{"open": base, "high": base * 1.01, "low": base * 0.99,
             "close": base, "volume": 1000.0 + i} for i in range(n)]


def test_init_validates():
    s = BollingerBandMeanReversionStrategy()
    s.init(_hist(40))
    assert s.middle_band and s.band_widths


def test_init_empty_raises():
    import pytest
    with pytest.raises(ValueError):
        BollingerBandMeanReversionStrategy().init([])


def test_init_too_short_raises():
    import pytest
    with pytest.raises(ValueError):
        BollingerBandMeanReversionStrategy().init(_hist(5))


def test_buy_breach():
    s = BollingerBandMeanReversionStrategy()
    s.init(_hist(40))
    sig = s.on_bar({"close": 90.0, "open": 90, "high": 90.5, "low": 89.5, "volume": 1000})
    assert sig is not None and sig["action"] == "BUY"


def test_sell_breach():
    s = BollingerBandMeanReversionStrategy()
    s.init(_hist(40))
    sig = s.on_bar({"close": 110.0, "open": 110, "high": 110.5, "low": 109.5, "volume": 1000})
    assert sig is not None and sig["action"] == "SELL"


def test_no_signal_in_band():
    s = BollingerBandMeanReversionStrategy()
    s.init(_hist(40))
    assert s.on_bar({"close": 100.0, "open": 100, "high": 100.5, "low": 99.5, "volume": 1000}) is None


def test_invalid_close_none():
    s = BollingerBandMeanReversionStrategy()
    s.init(_hist(40))
    assert s.on_bar({"close": 0}) is None


def test_performance_empty():
    s = BollingerBandMeanReversionStrategy()
    s.init(_hist(40))
    m = s.get_performance_metrics()
    assert m["total_signals"] == 0
    assert m["win_rate"] == 0.0
