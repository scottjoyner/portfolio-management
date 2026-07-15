"""
Global API rate limiter for Coinbase Exchange HTTP endpoints.

Coordinates concurrent outbound requests across all consumers
(rest_feed, pair_discovery, accumulator, news) so the host never
exceeds Coinbase's per-IP connection ceiling. Components acquire a
shared slot before making a request; the limiter also enforces a soft
minimum inter-request spacing to avoid burst throttling (HTTP 429).
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Iterator

# Maximum concurrent connections to api.exchange.coinbase.com
MAX_CONCURRENT = 5

# Soft minimum spacing between successive requests (seconds) — caps burst
# throughput at ~1 / MIN_INTERVAL_S requests per second per caller stream.
_MIN_INTERVAL_S = 0.05  # ~20 req/s soft cap

_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT)
_active = 0
_lock = threading.Lock()
_last_req_ts = 0.0


@contextmanager
def api_slot() -> Iterator[None]:
    """Acquire a shared slot before making a Coinbase API call.

    Usage::

        with api_slot():
            r = http.request("GET", url, timeout=10)
    """
    _semaphore.acquire()
    with _lock:
        global _active, _last_req_ts
        _active += 1
        now = time.time()
        wait = _MIN_INTERVAL_S - (now - _last_req_ts)
        if wait > 0:
            time.sleep(wait)
        _last_req_ts = time.time()
    try:
        yield
    finally:
        with _lock:
            _active -= 1
        _semaphore.release()


def active_count() -> int:
    """Number of requests currently holding a slot."""
    with _lock:
        return _active


def available() -> int:
    """Number of free slots."""
    return max(0, MAX_CONCURRENT - active_count())
