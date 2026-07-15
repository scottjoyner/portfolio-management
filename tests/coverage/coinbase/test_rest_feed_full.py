"""Coverage tests for coinbase/src/rest_feed.py"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from coinbase.src import rest_feed as rf


def _resp(status=200, json_data=None, headers=None, raise_on_json=False):
    resp = AsyncMock()
    resp.status = status
    resp.headers = headers or {}
    if raise_on_json:
        resp.json.side_effect = ValueError("bad")
    else:
        resp.json.return_value = json_data
    cm = AsyncMock()
    cm.__aenter__.return_value = resp
    cm.__aexit__.return_value = False
    return cm, resp


def _session(cm_or_list):
    session = MagicMock()
    if isinstance(cm_or_list, list):
        session.get.side_effect = cm_or_list
    else:
        session.get.return_value = cm_or_list

    async def _get():
        return session
    return _get


def _wrap(session):
    async def g():
        return session
    return g


@pytest.fixture(autouse=True)
def reset_state():
    rf._CANDLE_CACHE.clear()
    rf._CIRCUIT_BREAKERS.clear()
    yield
    rf._CANDLE_CACHE.clear()
    rf._CIRCUIT_BREAKERS.clear()


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------
def test_circuit_breaker():
    cb = rf.CircuitBreaker()
    assert cb.can_attempt() is True
    cb.record_success()
    cb.record_failure(is_timeout=True)
    assert cb.consecutive_timeouts == 1
    # open after 3 timeouts
    cb.record_failure(is_timeout=True)
    cb.record_failure(is_timeout=True)
    assert cb.is_open is True
    assert cb.can_attempt() is False
    # half-open after 60s
    cb.last_failure = 0.0
    assert cb.can_attempt() is True


def test_circuit_breaker_failures():
    cb = rf.CircuitBreaker()
    for _ in range(5):
        cb.record_failure()
    assert cb.is_open is True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def test_cache_ttl_and_gran():
    assert rf._cache_ttl_s(60) == 15.0
    assert rf._cache_ttl_s(999) == 60.0
    assert rf._granularity_to_cb_str(3600) == "3600"
    assert rf._granularity_to_cb_str(123) == "123"


def test_normalize_candles():
    data = [
        [1, 9, 12, 10, 11, 5],
        {"start": 2, "open": 1, "high": 2, "low": 0, "close": 1.5, "volume": 3},
    ]
    out = rf._normalize_candles(data, "X")
    # _normalize_candles reverses input order
    assert (1, 10.0, 12.0, 9.0, 11.0, 5.0) in out
    assert (2, 1.0, 2.0, 0.0, 1.5, 3.0) in out


# ---------------------------------------------------------------------------
# fetch_candles_rest
# ---------------------------------------------------------------------------
def test_fetch_rest_cache_hit():
    key = ("BTC-USD", 3600, 100)
    rf._CANDLE_CACHE[key] = (asyncio.get_event_loop().time() if False else __import__("time").time(),
                             [(1, 1, 2, 0, 1, 1)])
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out == [(1, 1, 2, 0, 1, 1)]


def test_fetch_rest_success(monkeypatch):
    monkeypatch.setattr(rf, "_get_session", _session(_resp(200, [[1, 9, 12, 10, 11, 1]])[0]))
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out and out[0][0] == 1


def test_fetch_rest_429_retry(monkeypatch):
    monkeypatch.setattr(rf.asyncio, "sleep", AsyncMock())
    c429, _ = _resp(429, headers={"Retry-After": "1"})
    c200, _ = _resp(200, [[1, 9, 12, 10, 11, 1]])
    monkeypatch.setattr(rf, "_get_session", _session([c429, c200]))
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out


def test_fetch_rest_non200(monkeypatch):
    monkeypatch.setattr(rf, "_get_session", _session(_resp(500)[0]))
    cb = rf.CircuitBreaker()
    rf._CIRCUIT_BREAKERS["BTC-USD"] = cb
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out == []
    assert cb.failures >= 1


def test_fetch_rest_timeout(monkeypatch):
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.side_effect = asyncio.TimeoutError()
    session.get.return_value = cm
    monkeypatch.setattr(rf, "_get_session", _wrap(session))
    cb = rf.CircuitBreaker()
    rf._CIRCUIT_BREAKERS["BTC-USD"] = cb
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out == []
    assert cb.consecutive_timeouts >= 1


def test_fetch_rest_generic_exception(monkeypatch):
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.side_effect = RuntimeError("boom")
    session.get.return_value = cm
    monkeypatch.setattr(rf, "_get_session", _wrap(session))
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out == []


def test_fetch_rest_circuit_open_returns_cached(monkeypatch):
    rf._CANDLE_CACHE[("ETH-USD", 3600, 100)] = (__import__("time").time(), [(2, 1, 2, 0, 1, 1)])
    cb = rf.CircuitBreaker()
    cb.is_open = True
    rf._CIRCUIT_BREAKERS["ETH-USD"] = cb
    out = asyncio.run(rf.fetch_candles_rest("ETH-USD", 3600, 100))
    assert out == [(2, 1, 2, 0, 1, 1)]


def test_fetch_rest_circuit_open_no_cache(monkeypatch):
    cb = rf.CircuitBreaker()
    cb.is_open = True
    rf._CIRCUIT_BREAKERS["ZZZ-USD"] = cb
    out = asyncio.run(rf.fetch_candles_rest("ZZZ-USD", 3600, 100))
    assert out == []


# ---------------------------------------------------------------------------
# fetch_candles_batch / incremental
# ---------------------------------------------------------------------------
def test_fetch_batch(monkeypatch):
    monkeypatch.setattr(rf, "_get_session", _session(_resp(200, [[1, 9, 12, 10, 11, 1]])[0]))
    out = asyncio.run(rf.fetch_candles_batch(["BTC-USD", "ETH-USD"], 3600, 100))
    assert "BTC-USD" in out and "ETH-USD" in out


def test_fetch_batch_with_exception(monkeypatch):
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.side_effect = RuntimeError("x")
    session.get.return_value = cm
    monkeypatch.setattr(rf, "_get_session", _wrap(session))
    out = asyncio.run(rf.fetch_candles_batch(["BTC-USD"], 3600, 100))
    assert out == {}


def test_fetch_incremental_batch(monkeypatch):
    monkeypatch.setattr(rf, "_get_session", _session(_resp(200, [[5, 9, 12, 10, 11, 1]])[0]))
    new, ts = asyncio.run(rf.fetch_incremental_batch(
        ["BTC-USD"], 3600, {"BTC-USD": 1}, limit=100, max_concurrent=5))
    assert "BTC-USD" in new
    assert ts["BTC-USD"] == 5


# ---------------------------------------------------------------------------
# cache invalidation / arrays / sync wrappers
# ---------------------------------------------------------------------------
def test_invalidate_cache():
    rf._CANDLE_CACHE[("BTC-USD", 3600, 100)] = (0.0, [])
    rf._CANDLE_CACHE[("ETH-USD", 3600, 100)] = (0.0, [])
    asyncio.run(rf.invalidate_candle_cache("BTC-USD"))
    assert ("BTC-USD", 3600, 100) not in rf._CANDLE_CACHE
    asyncio.run(rf.invalidate_candle_cache())
    assert rf._CANDLE_CACHE == {}


def test_candle_arrays():
    candles = [(1, 10.0, 12.0, 9.0, 11.0, 5.0)]
    arr = rf.candle_arrays(candles)
    assert arr["closes"] == [11.0]
    assert arr["highs"] == [12.0]
    assert arr["lows"] == [9.0]
    assert arr["volumes"] == [5.0]


def test_sync_wrappers(monkeypatch):
    monkeypatch.setattr(rf, "_get_session", _session(_resp(200, [[1, 9, 12, 10, 11, 1]])[0]))
    out = rf.fetch_candles_rest_sync("BTC-USD", 3600, 100)
    assert out
    batch = rf.fetch_candles_batch_sync(["BTC-USD"], 3600, 100)
    assert "BTC-USD" in batch
