import time
from unittest.mock import MagicMock

import pytest

import coinbase.src.data as d
from coinbase.src.data import (
    _MiniSeries,
    _MiniFrame,
    compute_atr,
    rolling_high,
    rolling_low,
    rsi,
    fetch_candles_df,
    invalidate_cache,
    _cache_key,
)


def test_mini_series_arithmetic():
    s = _MiniSeries([1.0, 2.0, 3.0])
    assert list(s + 1) == [2.0, 3.0, 4.0]
    assert list(s - 1) == [0.0, 1.0, 2.0]
    assert list(s * 2) == [2.0, 4.0, 6.0]
    assert list(s / 2) == [0.5, 1.0, 1.5]
    assert list(s.__rtruediv__(6.0)) == [6.0, 3.0, 2.0]


def test_mini_series_sub_list():
    s = _MiniSeries([1.0, 2.0, 3.0])
    assert list(s - _MiniSeries([0.5, 0.5, 0.5])) == [0.5, 1.5, 2.5]


def test_mini_series_shift_diff_clip_abs():
    s = _MiniSeries([1.0, 2.0, 3.0, 4.0])
    assert list(s.shift(1)) == [None, 1.0, 2.0, 3.0]
    assert list(s.shift(0)) == [1.0, 2.0, 3.0, 4.0]
    df = s.diff()
    assert df[0] is None
    assert df[1] == 1.0
    cl = s.clip(lower=1.5, upper=3.5)
    assert cl == _MiniSeries([1.5, 2.0, 3.0, 3.5])
    assert list(s.abs()) == [1.0, 2.0, 3.0, 4.0]
    assert s.tail(2) == _MiniSeries([3.0, 4.0])
    assert s.max() == 4.0
    assert s.min() == 1.0
    assert s.mean() == 2.5


def test_mini_series_rolling_and_ewm():
    s = _MiniSeries([1.0, 2.0, 3.0, 4.0])
    rmean = s.rolling(2).mean()
    assert rmean[0] == pytest.approx(1.0)
    assert rmean[1] == pytest.approx(1.5)
    rmax = s.rolling(2).max()
    assert rmax[1] == 2.0
    rmin = s.rolling(2).min()
    assert rmin[1] == 1.0
    em = s.ewm(alpha=0.5).mean()
    assert em[-1] is not None


def test_mini_frame():
    fr = _MiniFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    assert len(fr) == 2
    assert list(fr["a"]) == [1, 3]
    assert fr.iloc[0]["a"] == 1
    assert fr.tail(1)._rows == [{"a": 3, "b": 4}]
    assert fr.copy()._rows == fr._rows
    assert fr.drop_duplicates()._rows == fr._rows
    assert fr.sort_values()._rows == fr._rows
    assert fr.set_index()._rows == fr._rows
    with pytest.raises(TypeError):
        fr[0]


def test_cache_key_and_invalidate():
    invalidate_cache()
    k = _cache_key("BTC-USD", "ONE_HOUR", 1)
    assert k == ("BTC-USD", "ONE_HOUR", 1)
    invalidate_cache("BTC-USD")
    invalidate_cache(None)


def _fake_client(candles):
    c = MagicMock()
    c.public_candles.return_value = {"candles": candles}
    return c


def test_fetch_candles_df_basic(monkeypatch):
    monkeypatch.setattr(d, "_CACHE", {})
    candles = [
        {"start": 1, "open": 10, "high": 12, "low": 9, "close": 11, "volume": 100},
        {"start": 2, "open": 11, "high": 13, "low": 10, "close": 12, "volume": 110},
    ]
    client = _fake_client(candles)
    df = fetch_candles_df(client, "BTC-USD", lookback_days=1, granularity="ONE_HOUR", cache_ttl_s=1)
    rows = len(df)
    assert rows == 2
    # cache hit on second call
    df2 = fetch_candles_df(client, "BTC-USD", lookback_days=1, granularity="ONE_HOUR", cache_ttl_s=1)
    assert len(df2) == 2
    assert client.public_candles.call_count == 1


def test_fetch_candles_df_tuple_form(monkeypatch):
    monkeypatch.setattr(d, "_CACHE", {})
    candles = [
        [1, 9.0, 12.0, 10.0, 11.0, 100.0],
        [2, 10.0, 13.0, 11.0, 12.0, 110.0],
    ]
    client = _fake_client(candles)
    df = fetch_candles_df(client, "ETH-USD", lookback_days=1, granularity="ONE_HOUR", cache_ttl_s=1)
    assert len(df) == 2


def test_fetch_candles_df_retry_then_fail(monkeypatch):
    monkeypatch.setattr(d, "_CACHE", {})
    monkeypatch.setattr(time, "sleep", lambda x: None)
    client = MagicMock()
    client.public_candles.side_effect = Exception("boom")
    df = fetch_candles_df(client, "BTC-USD", lookback_days=1, granularity="ONE_HOUR",
                          max_retries=2, cache_ttl_s=1)
    assert len(df) == 0


def test_compute_atr_rolling_rsi(monkeypatch):
    monkeypatch.setattr(d, "_CACHE", {})
    candles = [
        {"start": i, "open": 10 + i, "high": 12 + i, "low": 9 + i, "close": 11 + i, "volume": 100}
        for i in range(30)
    ]
    client = _fake_client(candles)
    df = fetch_candles_df(client, "BTC-USD", lookback_days=1, granularity="ONE_HOUR", cache_ttl_s=1)
    atr = compute_atr(df, period=14)
    assert atr is not None
    rh = rolling_high(df, 20)
    rl = rolling_low(df, 20)
    assert rh is not None and rl is not None
    r = rsi(df["close"], 14)
    assert r is not None


def test_pandas_absent_path(monkeypatch):
    # Force pandas-unavailable code paths
    monkeypatch.setattr(d, "pd", None)
    s = d._series([1.0, 2.0, None, 4.0])
    assert isinstance(s, _MiniSeries)
    fr = d._frame([{"a": 1}], columns=["a"])
    assert isinstance(fr, _MiniFrame)
    monkeypatch.undo()
