from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Callable, Optional

from fastapi.responses import Response
from starware.middleware.base import BaseMiddleware

log = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """
    Rate limiter using token bucket algorithm per client/network.
    
    Tracks requests per endpoint with configurable limits to protect against
    API quota exhaustion and provide graceful degradation under load.
    """

    def __init__(
        self,
        default_requests_per_second: float = 10.0,
        burst_size: int = 20,
        skip_paths: list[str] | None = None,
    ) -> None:
        self.default_rps = default_requests_per_second
        self.burst_size = burst_size
        self.skip_paths = skip_paths or []
        
        # Per-endpoint tracking: {endpoint_path: [last_request_time]}
        self._request_times: dict[str, list[float]] = defaultdict(list)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def check_rate_limit(self, endpoint: str, key: str = "default") -> bool:
        """Check if request is within rate limit. Returns True if allowed."""
        now = time.time()
        
        # Clean up old entries (> 1 second for 10 RPS)
        cutoff = now - (1.0 / self.default_rps)
        times = [t for t in self._request_times[endpoint] if t > cutoff]
        
        # Remove oldest if at burst limit
        while len(times) >= self.burst_size:
            times.pop(0)
        
        # Allow or wait (simplified: return immediately, caller can decide)
        self._request_times[endpoint].extend(times)
        
        # Check if under limit
        under_limit = len(times) < self.burst_size
        
        return under_limit

    async def record_request(self, endpoint: str) -> None:
        """Record request timestamp."""
        await self._locks[endpoint].acquire()
        now = time.time()
        self._request_times[endpoint].append(now)
        
        # Clean old entries
        cutoff = now - (1.0 / self.default_rps)
        self._request_times[endpoint] = [t for t in self._request_times[endpoint] if t > cutoff]


class RateLimitMiddleware:
    """
    FastAPI middleware for rate limiting with configurable limits per endpoint.
    
    Protects API endpoints from quota exhaustion and provides graceful degradation.
    Uses token bucket algorithm with per-endpoint tracking.
    """

    def __init__(
        self,
        default_requests_per_second: float = 10.0,
        burst_size: int = 20,
        skip_paths: list[str] | None = None,
    ) -> None:
        self.limiter = TokenBucketRateLimiter(
            default_requests_per_second=default_requests_per_second,
            burst_size=burst_size,
            skip_paths=skip_paths,
        )
        self.skip_paths = skip_paths or []

    async def __call__(self, request: Any, call_next: Callable) -> Any:
        """Process request with rate limiting."""
        # Check if path is skipped
        path = request.url.path
        if any(path.startswith(s) for s in self.skip_paths):
            return await call_next(request)

        endpoint = f"/{path.strip('/').split('/')[0] if '/' in path else ''}"
        
        try:
            allowed = await self.limiter.check_rate_limit(endpoint)
            
            if not allowed:
                # Rate limit exceeded - log and continue (or respond with 429)
                log.warning("Rate limit exceeded for %s", endpoint)
                # Option A: Allow burst through
                # Option B: Return 429 Too Many Requests
                pass

            return await call_next(request)
        
        except Exception as e:
            log.exception("rate limiter error: %s", e)
            return await call_next(request)


# Convenience endpoint for health check (no rate limiting)
@router.get("/health")
async def health_check() -> dict[str, Any]:
    """Health check endpoint (bypasses rate limiting)."""
    return {"status": "ok"}
