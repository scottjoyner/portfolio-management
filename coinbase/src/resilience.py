from __future__ import annotations

import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


def is_transient_error(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, ConnectionError):
        return True
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in {408, 425, 429, 500, 502, 503, 504}
    if isinstance(exc, urllib.error.URLError):
        return True
    msg = str(exc).lower()
    return any(token in msg for token in ("timeout", "temporarily", "rate limit", "reset by peer", "unavailable"))


def retry_call(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
    jitter: float = 0.2,
    retry_if: Callable[[Exception], bool] = is_transient_error,
) -> T:
    last_exc: Optional[Exception] = None
    for i in range(max(1, attempts)):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if i >= attempts - 1 or not retry_if(exc):
                raise
            delay = min(max_delay, base_delay * (2 ** i))
            delay *= 1.0 + (random.random() * 2.0 - 1.0) * jitter
            time.sleep(max(0.0, delay))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("retry_call failed without exception")


@dataclass
class SourceCircuitBreaker:
    name: str
    failure_threshold: int = 3
    reset_timeout_s: float = 300.0
    half_open_success_threshold: int = 1
    failure_count: int = 0
    opened_at: float = 0.0
    state: str = "closed"  # closed | open | half_open
    last_error: str = ""
    success_count: int = 0

    def allow(self) -> bool:
        now = time.time()
        if self.state == "open":
            if now - self.opened_at >= self.reset_timeout_s:
                self.state = "half_open"
                self.success_count = 0
                return True
            return False
        return True

    def on_success(self) -> None:
        self.failure_count = 0
        self.last_error = ""
        if self.state == "half_open":
            self.success_count += 1
            if self.success_count >= self.half_open_success_threshold:
                self.state = "closed"
                self.success_count = 0
        else:
            self.state = "closed"

    def on_failure(self, exc: Exception | str) -> None:
        self.failure_count += 1
        self.last_error = str(exc)
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            self.opened_at = time.time()
            self.success_count = 0

    def snapshot(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state,
            "failure_count": self.failure_count,
            "opened_at": self.opened_at,
            "last_error": self.last_error,
            "success_count": self.success_count,
        }
