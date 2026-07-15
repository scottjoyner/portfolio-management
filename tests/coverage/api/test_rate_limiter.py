import asyncio

from unittest.mock import AsyncMock, MagicMock

from trading_system.apps.api.middleware import rate_limiter as rl


def run(coro):
    return asyncio.run(coro)


def test_token_bucket_check_allow_when_empty():
    lim = rl.TokenBucketRateLimiter(default_requests_per_second=10.0, burst_size=3)
    assert run(lim.check_rate_limit("e")) is True


def test_token_bucket_denies_when_saturated():
    lim = rl.TokenBucketRateLimiter(default_requests_per_second=10.0, burst_size=2)
    # Pre-fill the recent window with two requests (within the 1s window).
    now = __import__("time").time()
    lim._request_times["e"] = [now, now]
    assert run(lim.check_rate_limit("e")) is False


def test_token_bucket_old_entries_pruned():
    lim = rl.TokenBucketRateLimiter(default_requests_per_second=10.0, burst_size=2)
    old = __import__("time").time() - 100.0  # well outside the 1s window
    lim._request_times["e"] = [old, old]
    # Old entries are pruned so the request is allowed again.
    assert run(lim.check_rate_limit("e")) is True
    # Window should have been trimmed.
    assert len(lim._request_times["e"]) <= 4


def test_record_request_appends_and_cleans():
    lim = rl.TokenBucketRateLimiter(default_requests_per_second=10.0, burst_size=5)
    run(lim.record_request("e"))
    assert "e" in lim._request_times
    assert len(lim._request_times["e"]) == 1
    run(lim.record_request("e"))
    assert len(lim._request_times["e"]) == 2


def test_record_request_uses_lock():
    lim = rl.TokenBucketRateLimiter(default_requests_per_second=10.0, burst_size=5)
    # Ensure the asyncio.Lock acquisition path runs without error.
    run(lim.record_request("locked"))
    assert "locked" in lim._request_times


def test_middleware_skip_path():
    mw = rl.RateLimitMiddleware(skip_paths=["/healthz"])
    request = MagicMock()
    request.url.path = "/healthz/foo"
    call_next = AsyncMock(return_value="RESP")
    assert run(mw(request, call_next)) == "RESP"
    call_next.assert_awaited_once()


def test_middleware_non_skip_path_not_starting_with():
    mw = rl.RateLimitMiddleware(skip_paths=["/healthz"])
    request = MagicMock()
    request.url.path = "/api/foo"
    call_next = AsyncMock(return_value="RESP")
    mw.limiter.check_rate_limit = AsyncMock(return_value=True)
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
    call_next.assert_awaited_once()


def test_middleware_exception_passes_through():
    mw = rl.RateLimitMiddleware()
    mw.limiter.check_rate_limit = AsyncMock(side_effect=RuntimeError("boom"))
    request = MagicMock()
    request.url.path = "/api/foo"
    call_next = AsyncMock(return_value="RESP")
    assert run(mw(request, call_next)) == "RESP"
    call_next.assert_awaited_once()


def test_health_check_endpoint():
    assert run(rl.health_check()) == {"status": "ok"}
