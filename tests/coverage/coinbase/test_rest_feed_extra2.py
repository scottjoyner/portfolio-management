"""More branch coverage for coinbase/src/rest_feed.py (circuit breaker + session reuse)."""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import coinbase.src.rest_feed as rf
from coinbase.src.rest_feed import CircuitBreaker, fetch_candles_rest, invalidate_candle_cache


def _make_session(resp):
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.return_value = resp
    cm.__aexit__.return_value = False
    session.get = MagicMock(return_value=cm)
    return session


def test_get_session_reuse():
    rf._tls.http_session = None
    s1 = asyncio.run(rf._get_session())
    s2 = asyncio.run(rf._get_session())
    assert s1 is s2
    rf._tls.http_session = None


def test_circuit_open_expired_cache():
    cb = CircuitBreaker()
    cb.is_open = True
    cb.last_failure = time.time()
    cached = [(1, 10.0, 12.0, 9.0, 11.0, 100.0)]
    with patch.dict(rf._CIRCUIT_BREAKERS, {"BTC-USD": cb}), \
         patch.dict(rf._CANDLE_CACHE, {("BTC-USD", 3600, 10): (time.time() - 1000, list(cached))}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600, limit=10))
    assert out == cached


def test_429_with_cb():
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
    cb = CircuitBreaker()
    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CIRCUIT_BREAKERS, {"BTC-USD": cb}), \
         patch.dict(rf._CANDLE_CACHE, {}), \
         patch("coinbase.src.rest_feed.asyncio.sleep", new=AsyncMock()):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600, limit=10))
    assert out[0][4] == 11.0


def test_non200_with_cb():
    resp = AsyncMock()
    resp.status = 500
    session = _make_session(resp)
    cb = CircuitBreaker()
    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CIRCUIT_BREAKERS, {"BTC-USD": cb}), \
         patch.dict(rf._CANDLE_CACHE, {}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600))
    assert out == []
    assert cb.failures >= 1


def test_timeout_with_cb():
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.side_effect = asyncio.TimeoutError()
    cm.__aexit__.return_value = False
    session.get = MagicMock(return_value=cm)
    cb = CircuitBreaker()
    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CIRCUIT_BREAKERS, {"BTC-USD": cb}), \
         patch.dict(rf._CANDLE_CACHE, {}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600))
    assert out == []
    assert cb.consecutive_timeouts >= 1


def test_exception_with_cb():
    session = MagicMock()
    cm = AsyncMock()
    cm.__aenter__.side_effect = RuntimeError("boom")
    cm.__aexit__.return_value = False
    session.get = MagicMock(return_value=cm)
    cb = CircuitBreaker()
    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CIRCUIT_BREAKERS, {"BTC-USD": cb}), \
         patch.dict(rf._CANDLE_CACHE, {}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600))
    assert out == []
    assert cb.failures >= 1


def test_success_with_cb():
    resp = AsyncMock()
    resp.status = 200
    resp.json = AsyncMock(return_value=[[1, 9.0, 12.0, 10.0, 11.0, 100.0]])
    session = _make_session(resp)
    cb = CircuitBreaker()
    with patch.object(rf, "_get_session", return_value=session), \
         patch.dict(rf._CIRCUIT_BREAKERS, {"BTC-USD": cb}), \
         patch.dict(rf._CANDLE_CACHE, {}):
        out = asyncio.run(fetch_candles_rest("BTC-USD", granularity=3600, limit=10))
    assert out[0][4] == 11.0
    assert cb.failures == 0


def test_invalidate_specific_product():
    rf._CANDLE_CACHE[("BTC-USD", 3600, 10)] = (time.time(), [(1, 10.0, 12.0, 9.0, 11.0, 100.0)])
    rf._CANDLE_CACHE[("ETH-USD", 3600, 10)] = (time.time(), [(2, 10.0, 12.0, 9.0, 11.0, 100.0)])
    asyncio.run(invalidate_candle_cache("BTC-USD"))
    assert ("BTC-USD", 3600, 10) not in rf._CANDLE_CACHE
    assert ("ETH-USD", 3600, 10) in rf._CANDLE_CACHE
    asyncio.run(invalidate_candle_cache(None))
