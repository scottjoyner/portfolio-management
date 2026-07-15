"""Extra branch-coverage tests for coinbase/src/rest_feed.py (target >=90%)."""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from coinbase.src import rest_feed as rf


def _resp(status=200, json_data=None, headers=None):
    resp = AsyncMock()
    resp.status = status
    resp.headers = headers or {}
    resp.json.return_value = json_data
    cm = AsyncMock()
    cm.__aenter__.return_value = resp
    cm.__aexit__.return_value = False
    return cm


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
def test_get_session_reuse_and_close():
    # Close with no session -> early return (covers the falsy branch).
    rf._tls.http_session = None
    asyncio.run(rf.close_session())
    # First call creates, second reuses (covers both branches of the guard).
    s1 = asyncio.run(rf._get_session())
    s2 = asyncio.run(rf._get_session())
    assert s1 is s2
    asyncio.run(rf.close_session())
    # After close, a new session is created again.
    s3 = asyncio.run(rf._get_session())
    assert s3 is not s1


def test_normalize_malformed_element():
    data = [123, [1, 9, 12, 10, 11, 5]]  # int is neither list/tuple nor dict
    out = rf._normalize_candles(data, "X")
    # only the valid list survives
    assert out == [(1, 10.0, 12.0, 9.0, 11.0, 5.0)]


def test_fetch_rest_circuit_open_after_ts_cached():
    rf._CANDLE_CACHE[("ETH-USD", 3600, 100)] = (time.time(), [(2, 1, 2, 0, 1, 1)])
    cb = rf.CircuitBreaker()
    cb.is_open = True
    cb.last_failure = time.time()  # keep it OPEN (avoid half-open reset)
    rf._CIRCUIT_BREAKERS["ETH-USD"] = cb
    out = asyncio.run(rf.fetch_candles_rest("ETH-USD", 3600, 100, after_ts=5))
    assert out == [(2, 1, 2, 0, 1, 1)]


def test_fetch_rest_limit_zero(monkeypatch):
    session = MagicMock()
    cm = _resp(200, [[1, 9, 12, 10, 11, 1]])
    session.get.return_value = cm
    monkeypatch.setattr(rf, "_get_session", _wrap(session))
    asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, limit=0))
    params = session.get.call_args.kwargs["params"]
    assert "limit" not in params


def test_fetch_rest_429_with_breaker_and_bad_retry(monkeypatch):
    monkeypatch.setattr(rf.asyncio, "sleep", AsyncMock())
    c429 = _resp(429, headers={"Retry-After": "soon"})
    c200 = _resp(200, [[1, 9, 12, 10, 11, 1]])
    monkeypatch.setattr(rf, "_get_session", _session([c429, c200]))
    cb = rf.CircuitBreaker()
    rf._CIRCUIT_BREAKERS["BTC-USD"] = cb
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out  # success after retry; breaker recorded failure


def test_fetch_rest_non200_no_breaker(monkeypatch):
    monkeypatch.setattr(rf, "_get_session", _session(_resp(500)))
    # No circuit breaker registered -> cb is None -> record_failure skipped.
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out == []


def test_fetch_rest_success_with_breaker(monkeypatch):
    monkeypatch.setattr(rf, "_get_session", _session(_resp(200, [[1, 9, 12, 10, 11, 1]])))
    cb = rf.CircuitBreaker()
    rf._CIRCUIT_BREAKERS["BTC-USD"] = cb
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out
    assert cb.failures == 0


def test_fetch_rest_timeout_no_breaker(monkeypatch):
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.side_effect = asyncio.TimeoutError()
    session.get.return_value = cm
    monkeypatch.setattr(rf, "_get_session", _wrap(session))
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out == []


def test_fetch_rest_circuit_open_no_cache(monkeypatch):
    cb = rf.CircuitBreaker()
    cb.is_open = True
    cb.last_failure = time.time()
    rf._CIRCUIT_BREAKERS["ZZZ-USD"] = cb
    out = asyncio.run(rf.fetch_candles_rest("ZZZ-USD", 3600, 100))
    assert out == []  # open breaker, no cache -> empty


def test_fetch_rest_generic_exception_no_breaker(monkeypatch):
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.side_effect = RuntimeError("boom")
    session.get.return_value = cm
    monkeypatch.setattr(rf, "_get_session", _wrap(session))
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out == []


def test_fetch_rest_generic_exception_with_breaker(monkeypatch):
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.side_effect = RuntimeError("boom")
    session.get.return_value = cm
    monkeypatch.setattr(rf, "_get_session", _wrap(session))
    cb = rf.CircuitBreaker()
    rf._CIRCUIT_BREAKERS["BTC-USD"] = cb
    out = asyncio.run(rf.fetch_candles_rest("BTC-USD", 3600, 100))
    assert out == []
    assert cb.failures >= 1
def test_fetch_incremental_empty(monkeypatch):
    monkeypatch.setattr(rf, "_get_session", _session(_resp(200, [])))
    new, ts = asyncio.run(rf.fetch_incremental_batch(
        ["BTC-USD"], 3600, {"BTC-USD": 1}, limit=100, max_concurrent=5))
    assert new == {}
    assert ts == {"BTC-USD": 1}


def test_sync_wrappers_outside_loop(monkeypatch):
    monkeypatch.setattr(rf, "_get_session", _session(_resp(200, [[1, 9, 12, 10, 11, 1]])))
    # Force get_event_loop to raise so the RuntimeError fallback path runs.
    real_get = rf.asyncio.get_event_loop
    rf.asyncio.get_event_loop = lambda: (_ for _ in ()).throw(RuntimeError("no loop"))
    try:
        out = rf.fetch_candles_rest_sync("BTC-USD", 3600, 100)
        batch = rf.fetch_candles_batch_sync(["BTC-USD"], 3600, 100)
    finally:
        rf.asyncio.get_event_loop = real_get
    assert out
    assert "BTC-USD" in batch
