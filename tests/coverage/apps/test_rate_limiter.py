import asyncio
from unittest.mock import AsyncMock, MagicMock

from trading_system.apps.api.middleware import rate_limiter as rl


def run(coro):
    return asyncio.run(coro)


def test_token_bucket_check_and_record():
    lim = rl.TokenBucketRateLimiter(default_requests_per_second=10.0, burst_size=2)
    assert run(lim.check_rate_limit("e")) is True
    run(lim.record_request("e"))
    assert run(lim.check_rate_limit("e")) is True
    run(lim.record_request("e"))
    # Saturated the burst window -> denied.
    assert run(lim.check_rate_limit("e")) is False


def test_record_request_cleans_old():
    lim = rl.TokenBucketRateLimiter(default_requests_per_second=10.0, burst_size=5)
    run(lim.record_request("e"))
    assert "e" in lim._request_times


def test_middleware_skip_path():
    mw = rl.RateLimitMiddleware(skip_paths=["/healthz"])
    request = MagicMock()
    request.url.path = "/healthz/foo"
    call_next = AsyncMock(return_value="RESP")
    assert run(mw(request, call_next)) == "RESP"
    call_next.assert_awaited_once()


def test_middleware_allowed():
    mw = rl.RateLimitMiddleware()
    mw.limiter.check_rate_limit = AsyncMock(return_value=True)
    request = MagicMock()
    request.url.path = "/api/foo"
    call_next = AsyncMock(return_value="RESP")
    assert run(mw(request, call_next)) == "RESP"


def test_middleware_not_allowed_logs():
    mw = rl.RateLimitMiddleware()
    mw.limiter.check_rate_limit = AsyncMock(return_value=False)
    request = MagicMock()
    request.url.path = "/api/foo"
    call_next = AsyncMock(return_value="RESP")
    assert run(mw(request, call_next)) == "RESP"


def test_middleware_exception_passes_through():
    mw = rl.RateLimitMiddleware()
    mw.limiter.check_rate_limit = AsyncMock(side_effect=RuntimeError("boom"))
    request = MagicMock()
    request.url.path = "/api/foo"
    call_next = AsyncMock(return_value="RESP")
    assert run(mw(request, call_next)) == "RESP"


def test_health_check_endpoint():
    assert run(rl.health_check()) == {"status": "ok"}
