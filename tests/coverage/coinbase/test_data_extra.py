"""Extra coverage for coinbase/src/data.py — MiniSeries methods + no-pandas path."""
from __future__ import annotations

import importlib
import sys
import time

import pytest

import coinbase.src.data as d
from coinbase.src.data import (
    _MiniSeries, _MiniFrame, compute_atr, rolling_high, rolling_low, rsi,
    fetch_candles_df, invalidate_cache, _cache_key, _frame,
)


def test_mini_series_misc_methods():
    s = _MiniSeries([3.0, 1.0, 2.0, 4.0])
    assert s.copy() == [3.0, 1.0, 2.0, 4.0]
    assert list(s.tail(2)) == [2.0, 4.0]
    assert s.mean() == 2.5
    assert s.max() == 4.0
    assert s.min() == 1.0
    assert list(s.abs()) == [3.0, 1.0, 2.0, 4.0]
    r = _MiniSeries([1.0, 2.0, 3.0, 4.0])
    assert list(r.rolling(2).max())[-1] == 4.0
    assert list(r.rolling(2).min())[-1] == 3.0
    assert list(r.rolling(2).mean())[-1] == 3.5
    e = _MiniSeries([1.0, 2.0, 3.0])
    assert len(list(e.ewm(alpha=0.5).mean())) == 3
    fr = _MiniFrame([{"a": 1}, {"a": 2}], columns=["a"])
    assert list(fr["a"]) == [1, 2]
    assert fr.iloc[0]["a"] == 1
    assert len(fr.tail(1)) == 1
    assert fr.copy() is not fr


def test_mini_series_arith_with_list():
    s = _MiniSeries([1.0, 2.0, 3.0])
    assert list(s - [0.5, 0.5, 0.5]) == [0.5, 1.5, 2.5]


def test_mini_frame_passthroughs():
    fr = _MiniFrame([{"a": 1}])
    assert fr.drop_duplicates() is fr
    assert fr.sort_values() is fr
    assert fr.set_index() is fr


def test_invalidate_cache_variants():
    invalidate_cache("BTC-USD")
    invalidate_cache(None)


def test_fetch_candles_df_cache_hit(monkeypatch):
    class FakeClient:
        def public_candles(self, *a, **k):
            return {"candles": [{"start": 1, "open": 100, "high": 110, "low": 90,
                                 "close": 105, "volume": 10}]}

    key = _cache_key("BTC-USD", "ONE_DAY", 240)
    d._CACHE[key] = (time.time(), _MiniFrame([], columns=["open"]))
    res = fetch_candles_df(FakeClient(), "BTC-USD", lookback_days=240, granularity="ONE_DAY")
    assert res is not None
    invalidate_cache("BTC-USD")


def test_fetch_candles_df_no_pandas(monkeypatch):
    """Exercise the pandas-absent fallback path (patch _frame to build rows)."""
    with pytest.MonkeyPatch().context() as mp:
        mp.setitem(sys.modules, "pandas", None)
        importlib.reload(d)
        assert d.pd is None

        def fake_frame(rows, columns=None):
            if rows and isinstance(rows[0], (list, tuple)):
                dicts = [dict(zip(columns, r)) for r in rows]
                return _MiniFrame(dicts, columns=columns)
            return _MiniFrame(rows, columns=columns)

        mp.setattr(d, "_frame", fake_frame)

        class FakeClient:
            def public_candles(self, product_id, start_unix, end_unix, granularity, limit):
                return {"candles": [
                    {"start": 1, "open": 100, "high": 110, "low": 90, "close": 105, "volume": 10},
                    {"start": 2, "open": 105, "high": 115, "low": 95, "close": 110, "volume": 12},
                ]}

        res = fetch_candles_df(FakeClient(), "BTC-USD", lookback_days=1, granularity="ONE_HOUR")
        assert len(res) == 2
        atr = compute_atr(res, period=14)
        assert atr is not None
        assert rolling_high(res, lookback=20) is not None
        assert rolling_low(res, lookback=20) is not None
        assert rsi(res["close"], period=14) is not None

        # empty-frames branch (263->267)
        class EmptyClient:
            def public_candles(self, *a, **k):
                return {"candles": []}

        res2 = fetch_candles_df(EmptyClient(), "ETH-USD", lookback_days=1, granularity="ONE_HOUR")
        assert len(res2) == 0
    importlib.reload(d)
    assert d.pd is not None


def test_fetch_candles_df_no_pandas_tuple(monkeypatch):
    with pytest.MonkeyPatch().context() as mp:
        mp.setitem(sys.modules, "pandas", None)
        importlib.reload(d)

        def fake_frame(rows, columns=None):
            if rows and isinstance(rows[0], (list, tuple)):
                dicts = [dict(zip(columns, r)) for r in rows]
                return _MiniFrame(dicts, columns=columns)
            return _MiniFrame(rows, columns=columns)

        mp.setattr(d, "_frame", fake_frame)

        class FakeClient:
            def public_candles(self, *a, **k):
                return {"candles": [[1, 90, 110, 100, 105, 10]]}

        res = fetch_candles_df(FakeClient(), "BTC-USD", lookback_days=1, granularity="ONE_HOUR")
        assert len(res) == 1
    importlib.reload(d)
