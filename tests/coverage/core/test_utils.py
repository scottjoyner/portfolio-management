import pytest
from trading_system.utils import (
    sma, ema, SMA, EMA, make_ma, create_callable_ma, partial,
)


def _candles(n, start=100.0, step=1.0):
    return [{'close': start + i * step} for i in range(n)]


def test_sma_decorator_insufficient():
    @sma(5)
    def f(candles, index):
        return None
    assert f([], -1) is None
    assert f(_candles(3), -1) is None


def test_sma_decorator_sufficient_negative_index():
    @sma(5)
    def f(candles, index):
        return None
    candles = _candles(10)
    # start_idx ignores index; uses first n closes 100..104 -> avg = 102
    assert f(candles, -1) == pytest.approx(102.0)


def test_sma_decorator_sufficient_positive_index():
    @sma(5)
    def f(candles, index):
        return None
    candles = _candles(10)
    # indices 0..4 closes 100..104 avg = 102
    assert f(candles, 4) == pytest.approx(102.0)


def test_sma_decorator_positive_index_overflow():
    @sma(5)
    def f(candles, index):
        return None
    candles = _candles(10)
    # index beyond range is clamped; still first 5 closes avg = 102
    assert f(candles, 999) == pytest.approx(102.0)


def test_ema_decorator_insufficient():
    @ema(5)
    def f(candles, index):
        return None
    assert f([], -1) is None
    assert f(_candles(3), -1) is None


def test_ema_decorator_negative_index_no_loop():
    @ema(5)
    def f(candles, index):
        return None
    candles = _candles(10)
    # index < 0 -> skip loop, return initial SMA of first 5 = 102
    assert f(candles, -1) == pytest.approx(102.0)


def test_ema_decorator_positive_index_loop():
    @ema(5)
    def f(candles, index):
        return None
    candles = _candles(10)
    # index 9 -> loop to last; just ensure a float > 0
    val = f(candles, 9)
    assert isinstance(val, float)
    assert val > 0


def test_ema_decorator_falsy_candle_skipped():
    @ema(5)
    def f(candles, index):
        return None
    candles = _candles(10)
    candles[7] = {}  # falsy -> skipped in loop
    val = f(candles, 9)
    assert isinstance(val, float)


def test_sma_class():
    s = SMA(5)
    assert s(_candles(10), -1) == pytest.approx(102.0)
    assert s([], -1) is None
    assert callable(s.callable)


def test_ema_class():
    e = EMA(5)
    assert e(_candles(10), -1) == pytest.approx(102.0)
    assert e([], -1) is None
    assert callable(e.callable)


def test_make_ma_ema():
    m = make_ma('EMA', 10)
    assert isinstance(m, EMA)


def test_make_ma_sma():
    m = make_ma('SMA', 10)
    assert isinstance(m, SMA)


def test_make_ma_unknown_defaults_sma():
    m = make_ma('BOGUS', 10)
    assert isinstance(m, SMA)


def test_create_callable_ma_ema():
    m = create_callable_ma('EMA', 10)
    assert callable(m)


def test_create_callable_ma_sma():
    m = create_callable_ma('SMA', 10)
    assert callable(m)


def test_create_callable_ma_unknown():
    m = create_callable_ma('BOGUS', 10)
    assert callable(m)


def test_partial():
    @sma(5)
    def f(candles, index):
        return None
    p = partial(f, _candles(10), -1)
    assert p() == pytest.approx(102.0)
