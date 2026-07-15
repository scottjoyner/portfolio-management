"""Coverage tests for coinbase/src/data.py"""
from __future__ import annotations

import time

import pytest

from coinbase.src import data


@pytest.fixture(autouse=True)
def clear_cache():
    data._CACHE.clear()
    yield
    data._CACHE.clear()


# ---------------------------------------------------------------------------
# Mini pandas shims
# ---------------------------------------------------------------------------
def test_mini_series_ops():
    s = data._MiniSeries([1.0, 2.0, 3.0])
    assert s.iloc is s
    assert s.copy() == [1.0, 2.0, 3.0]
    assert s.shift(1) == [None, 1.0, 2.0]
    assert s.shift(0) == [1.0, 2.0, 3.0]
    assert s.diff() == [None, 1.0, 1.0]
    assert s.clip(lower=1.5) == [1.5, 2.0, 3.0]
    assert s.clip(upper=2.5) == [1.0, 2.0, 2.5]
    assert s.abs() == [1.0, 2.0, 3.0]
    assert s.tail(2) == [2.0, 3.0]
    assert s.max() == 3.0
    assert s.min() == 1.0
    assert s.mean() == pytest.approx(2.0)
    r = s.rolling(2)
    assert r.max()[-1] == 3.0
    assert r.min()[-1] == 2.0
    assert r.mean()[-1] == pytest.approx(2.5)
    e = s.ewm(alpha=0.5)
    assert len(e.mean()) == 3
    assert (s / 2) == [0.5, 1.0, 1.5]
    assert (2 / s) == [2.0, 1.0, 2 / 3]
    assert (s * 2) == [2.0, 4.0, 6.0]
    assert (s + 1) == [2.0, 3.0, 4.0]
    assert (s - 1) == [0.0, 1.0, 2.0]
    assert (s - data._MiniSeries([1.0, 1.0, 1.0])) == [0.0, 1.0, 2.0]


def test_mini_frame():
    f = data._MiniFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    assert len(f) == 2
    assert f["a"] == data._MiniSeries([1, 3])
    assert f.iloc[0] == {"a": 1, "b": 2}
    assert f.iloc[0:1]._rows == [{"a": 1, "b": 2}]
    assert f.tail(1)._rows == [{"a": 3, "b": 4}]
    assert f.copy()._rows == f._rows
    assert f.drop_duplicates()._rows == f._rows
    assert f.sort_values()._rows == f._rows
    assert f.set_index()._rows == f._rows
    with pytest.raises(TypeError):
        f[0]


def test_frame_series_shims(monkeypatch):
    monkeypatch.setattr(data, "pd", None)
    assert data._frame([{"a": 1}])._rows == [{"a": 1}]
    assert list(data._series([1, 2])) == [1, 2]


# ---------------------------------------------------------------------------
# invalidate_cache
# ---------------------------------------------------------------------------
def test_invalidate_cache():
    data._CACHE[("BTC-USD", "ONE_DAY", 1)] = (time.time(), [])
    data._CACHE[("ETH-USD", "ONE_DAY", 1)] = (time.time(), [])
    data.invalidate_cache("BTC-USD")
    assert ("BTC-USD", "ONE_DAY", 1) not in data._CACHE
    assert ("ETH-USD", "ONE_DAY", 1) in data._CACHE
    data.invalidate_cache()
    assert data._CACHE == {}


# ---------------------------------------------------------------------------
# fetch_candles_df
# ---------------------------------------------------------------------------
class FakeClient:
    def __init__(self, candles, raise_times=0):
        self.candles = candles
        self.raise_times = raise_times
        self.n = 0

    def public_candles(self, *a, **k):
        self.n += 1
        if self.raise_times > 0:
            self.raise_times -= 1
            raise RuntimeError("net")
        return self.candles


def test_fetch_candles_cache_hit():
    import pandas as pd
    key = data._cache_key("BTC-USD", "ONE_DAY", 1)
    cached = pd.DataFrame([{"ts": 1, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 1}])
    data._CACHE[key] = (time.time(), cached)
    client = FakeClient({"candles": []})
    out = data.fetch_candles_df(client, "BTC-USD", lookback_days=1, granularity="ONE_DAY")
    assert len(out) == 1
    assert client.n == 0


def test_fetch_candles_dict_rows():
    candles = {"candles": [
        {"start": 1, "open": 10, "high": 12, "low": 9, "close": 11, "volume": 1},
        {"start": 2, "open": 11, "high": 13, "low": 10, "close": 12, "volume": 2},
    ]}
    client = FakeClient(candles)
    df = data.fetch_candles_df(client, "BTC-USD", lookback_days=1, granularity="ONE_DAY")
    assert len(df) == 2


def test_fetch_candles_tuple_rows():
    candles = {"candles": [
        (1, 9, 12, 10, 11, 1),
        (2, 10, 13, 11, 12, 2),
    ]}
    client = FakeClient(candles)
    df = data.fetch_candles_df(client, "BTC-USD", lookback_days=1, granularity="ONE_DAY")
    assert len(df) == 2


def test_fetch_candles_retry_then_success(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)
    candles = {"candles": [{"start": 1, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 1}]}
    client = FakeClient(candles, raise_times=2)
    df = data.fetch_candles_df(client, "BTC-USD", lookback_days=1, granularity="ONE_DAY",
                               max_retries=6)
    assert len(df) == 1


def test_fetch_candles_all_fail(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)
    client = FakeClient({"candles": []}, raise_times=10)
    df = data.fetch_candles_df(client, "BTC-USD", lookback_days=1, granularity="ONE_DAY",
                               max_retries=3)
    assert len(df) == 0


def test_fetch_candles_mini_frame(monkeypatch):
    monkeypatch.setattr(data, "pd", None)
    candles = {"candles": [{"start": 1, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 1}]}
    client = FakeClient(candles)
    df = data.fetch_candles_df(client, "BTC-USD", lookback_days=1, granularity="ONE_DAY")
    assert df._rows == [{"ts": 1, "open": 1.0, "high": 2.0, "low": 1.0, "close": 2.0, "volume": 1.0}]


# ---------------------------------------------------------------------------
# indicators
# ---------------------------------------------------------------------------
def _df(rows):
    return data._frame(rows, columns=["open", "high", "low", "close", "volume"])


def test_compute_atr():
    rows = [
        {"open": 10, "high": 12, "low": 9, "close": 11, "volume": 1},
        {"open": 11, "high": 13, "low": 10, "close": 12, "volume": 1},
        {"open": 12, "high": 14, "low": 11, "close": 13, "volume": 1},
    ]
    atr = data.compute_atr(_df(rows), period=2)
    assert atr is not None


def test_rolling_high_low(monkeypatch):
    monkeypatch.setattr(data, "pd", None)
    rows = [
        {"open": 10, "high": 12, "low": 9, "close": 11, "volume": 1},
        {"open": 11, "high": 20, "low": 5, "close": 12, "volume": 1},
    ]
    assert data.rolling_high(_df(rows), lookback=2)[-1] == 20
    assert data.rolling_low(_df(rows), lookback=2)[-1] == 5


def test_rsi():
    rows = [{"open": i, "high": i + 1, "low": i - 1, "close": i, "volume": 1} for i in range(1, 20)]
    r = data.rsi(data._series([float(i) for i in range(1, 20)]), period=5)
    assert r is not None
