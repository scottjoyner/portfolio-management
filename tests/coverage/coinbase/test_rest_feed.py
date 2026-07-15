import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import coinbase.src.rest_feed as rf
from coinbase.src.rest_feed import (
    CircuitBreaker,
    _granularity_to_cb_str,
    _cache_ttl_s,
    _normalize_candles,
    candle_arrays,
    fetch_candles_rest,
    fetch_candles_batch,
    fetch_incremental_batch,
    invalidate_candle_cache,
)


def test_granularity_map():
    assert _granularity_to_cb_str(60) == "60"
    assert _granularity_to_cb_str(3600) == "3600"
    assert _granularity_to_cb_str(123) == "123"


def test_cache_ttl():
    assert _cache_ttl_s(60) == 15.0
    assert _cache_ttl_s(999999) == 60.0


def test_normalize_candles_tuple():
    data = [[1, 9.0, 12.0, 10.0, 11.0, 100.0], [2, 10.0, 13.0, 11.0, 12.0, 110.0]]
    out = _normalize_candles(data, "BTC-USD")
    # _normalize_candles reverses so output is oldest-first
    assert out[0] == (2, 11.0, 13.0, 10.0, 12.0, 110.0)
    assert out[1] == (1, 10.0, 12.0, 9.0, 11.0, 100.0)


def test_normalize_candles_dict():
    data = [{"start": 1, "open": 10, "high": 12, "low": 9, "close": 11, "volume": 100}]
    out = _normalize_candles(data, "BTC-USD")
    assert out[0] == (1, 10.0, 12.0, 9.0, 11.0, 100.0)


def test_normalize_candles_short_and_empty():
    out = _normalize_candles([], "BTC-USD")
    assert out == []
    # tuple with fewer than 6 elements is skipped
    out2 = _normalize_candles([(1, 2, 3)], "BTC-USD")
    assert out2 == []


def test_candle_arrays():
    candles = [(1, 10.0, 12.0, 9.0, 11.0, 100.0)]
    arr = candle_arrays(candles)
    assert arr["closes"] == [11.0]
    assert arr["volumes"] == [100.0]


def test_circuit_breaker_lifecycle():
    cb = CircuitBreaker()
    cb.record_success()
    assert cb.is_open is False
    cb.record_failure(is_timeout=True)
    cb.record_failure(is_timeout=True)
    cb.record_failure(is_timeout=True)
    assert cb.is_open is True
    assert cb.can_attempt() is False
    # force open time in past -> half-open
    cb.last_failure = 0.0
    assert cb.can_attempt() is True
    assert cb.is_open is False


def test_circuit_breaker_five_failures():
    cb = CircuitBreaker()
    for _ in range(5):
        cb.record_failure()
    assert cb.is_open is True


def test_circuit_breaker_consecutive_timeouts():
    cb = CircuitBreaker()
    cb.record_failure(is_timeout=True)
    cb.record_failure(is_timeout=True)
    cb.record_failure(is_timeout=True)
    assert cb.is_open is True


async def _run(coro):
    return await coro


def _make_session(resp):
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.return_value = resp
    cm.__aexit__.return_value = False
    session.get = MagicMock(return_value=cm)
    return session


def test_fetch_candles_rest_success():
    resp = AsyncMock()
    resp.status = 200
    resp.json = AsyncMock(return_value=[[1, 9.0, 12.0, 10.0, 11.0, 100.0]])
    session = _make_session(resp)

    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CANDLE_CACHE, {}), \
         patch.dict(rf._CIRCUIT_BREAKERS, {}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600, limit=10))
    assert out and out[0][4] == 11.0


def test_fetch_candles_rest_429():
    resp = AsyncMock()
    resp.status = 429
    resp.headers = {"Retry-After": "1"}
    resp2 = AsyncMock()
    resp2.status = 200
    resp2.json = AsyncMock(return_value=[[1, 9.0, 12.0, 10.0, 11.0, 100.0]])
    cm = AsyncMock(); cm.__aenter__.return_value = resp; cm.__aexit__.return_value = False
    cm2 = AsyncMock(); cm2.__aenter__.return_value = resp2; cm2.__aexit__.return_value = False
    session = MagicMock()
    session.get = MagicMock(side_effect=[cm, cm2])

    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CANDLE_CACHE, {}), \
         patch.dict(rf._CIRCUIT_BREAKERS, {}), \
         patch("coinbase.src.rest_feed.asyncio.sleep", new=AsyncMock()):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600, limit=10))
    assert out[0][4] == 11.0


def test_fetch_candles_rest_non200():
    resp = AsyncMock()
    resp.status = 404
    session = _make_session(resp)

    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CANDLE_CACHE, {}), \
         patch.dict(rf._CIRCUIT_BREAKERS, {}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600))
    assert out == []


def test_fetch_candles_rest_timeout():
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.side_effect = asyncio.TimeoutError()
    cm.__aexit__.return_value = False
    session.get = MagicMock(return_value=cm)

    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CANDLE_CACHE, {}), \
         patch.dict(rf._CIRCUIT_BREAKERS, {}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600))
    assert out == []


def test_fetch_candles_rest_circuit_open():
    cb = CircuitBreaker()
    cb.is_open = True
    cb.last_failure = time.time()
    with patch.dict(rf._CIRCUIT_BREAKERS, {"BTC-USD": cb}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600))
    assert out == []


def test_fetch_batch_and_incremental():
    with patch.object(rf, "fetch_candles_rest", new=AsyncMock(
            side_effect=lambda pid, *a, **k: [(1, 10.0, 12.0, 9.0, 11.0, 100.0)])):
        out = asyncio.run(fetch_candles_batch(["BTC-USD", "ETH-USD"], granularity=3600))
    assert "BTC-USD" in out and "ETH-USD" in out

    with patch.object(rf, "fetch_candles_batch", new=AsyncMock(
            return_value={"BTC-USD": [(5, 10.0, 12.0, 9.0, 11.0, 100.0)]})):
        new_c, ts = asyncio.run(fetch_incremental_batch(
            ["BTC-USD"], 3600, {"BTC-USD": 1}))
    assert "BTC-USD" in new_c


def test_invalidate_cache():
    asyncio.run(invalidate_candle_cache("BTC-USD"))
    asyncio.run(invalidate_candle_cache(None))


def test_fetch_candles_rest_cache_hit():
    resp = AsyncMock()
    resp.status = 200
    resp.json = AsyncMock(return_value=[[1, 9.0, 12.0, 10.0, 11.0, 100.0]])
    session = _make_session(resp)

    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CIRCUIT_BREAKERS, {}), \
         patch.dict(rf._CANDLE_CACHE, {}):
        out1 = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600, limit=10))
        out2 = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600, limit=10))
    # second call served from cache -> no second network request
    assert out1 == out2
    assert session.get.call_count == 1


def test_fetch_candles_rest_circuit_open_returns_cache():
    cb = CircuitBreaker()
    cb.is_open = True
    cb.last_failure = time.time()
    cached = [(1, 10.0, 12.0, 9.0, 11.0, 100.0)]
    with patch.dict(rf._CIRCUIT_BREAKERS, {"BTC-USD": cb}), \
         patch.dict(rf._CANDLE_CACHE, {("BTC-USD", 3600, 10): (time.time(), list(cached))}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600, limit=10))
    assert out == cached


def test_fetch_candles_rest_after_ts():
    resp = AsyncMock()
    resp.status = 200
    resp.json = AsyncMock(return_value=[[1, 9.0, 12.0, 10.0, 11.0, 100.0]])
    session = _make_session(resp)
    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CIRCUIT_BREAKERS, {}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600, limit=10, after_ts=12345))
    assert out and out[0][4] == 11.0
    # after_ts requests are not cached
    assert ("BTC-USD", 3600, 10) not in rf._CANDLE_CACHE


def test_get_session_recreates_closed():
    rf._tls.http_session = MagicMock(closed=True)
    try:
        s = asyncio.run(rf._get_session())
        assert s is not None
    finally:
        rf._tls.http_session = None


def test_fetch_candles_rest_429_bad_retry_after():
    resp = AsyncMock()
    resp.status = 429
    resp.headers = {"Retry-After": "notanint"}
    resp2 = AsyncMock()
    resp2.status = 200
    resp2.json = AsyncMock(return_value=[[1, 9.0, 12.0, 10.0, 11.0, 100.0]])
    cm = AsyncMock(); cm.__aenter__.return_value = resp; cm.__aexit__.return_value = False
    cm2 = AsyncMock(); cm2.__aenter__.return_value = resp2; cm2.__aexit__.return_value = False
    session = MagicMock()
    session.get = MagicMock(side_effect=[cm, cm2])
    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CIRCUIT_BREAKERS, {}), \
         patch("coinbase.src.rest_feed.asyncio.sleep", new=AsyncMock()):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600, limit=10))
    assert out[0][4] == 11.0


def test_fetch_candles_rest_exception():
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.side_effect = RuntimeError("boom")
    cm.__aexit__.return_value = False
    session.get = MagicMock(return_value=cm)
    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CIRCUIT_BREAKERS, {}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600))
    assert out == []


async def _boom(*a, **k):
    raise RuntimeError("boom")


def test_fetch_batch_exception_and_empty():
    with patch.object(rf, "fetch_candles_rest", new=_boom), \
         patch("coinbase.src.rest_feed.asyncio.sleep", new=AsyncMock()):
        out = asyncio.run(fetch_candles_batch(["BTC-USD", "ETH-USD"], granularity=3600))
    assert out == {}

    async def fake(pid, *a, **k):
        return [(1, 10.0, 12.0, 9.0, 11.0, 100.0)] if pid == "BTC-USD" else []
    with patch.object(rf, "fetch_candles_rest", new=fake):
        out = asyncio.run(fetch_candles_batch(["BTC-USD", "ETH-USD"], granularity=3600))
    assert "BTC-USD" in out
    assert "ETH-USD" not in out


def test_sync_wrappers():
    resp = AsyncMock()
    resp.status = 200
    resp.json = AsyncMock(return_value=[[1, 9.0, 12.0, 10.0, 11.0, 100.0]])
    session = _make_session(resp)
    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CIRCUIT_BREAKERS, {}):
        out = rf.fetch_candles_rest_sync("BTC-USD", granularity=3600, limit=10)
        assert out and out[0][4] == 11.0
        out2 = rf.fetch_candles_batch_sync(["BTC-USD"], granularity=3600)
        assert "BTC-USD" in out2


def test_close_session():
    rf._tls.http_session = None
    asyncio.run(rf.close_session())
    s = asyncio.run(rf._get_session())
    asyncio.run(rf.close_session())
    assert rf._tls.http_session is None
