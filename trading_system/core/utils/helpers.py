from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import TypeVar

T = TypeVar("T")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def timestamp_ms() -> int:
    return int(time.time() * 1000)


def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0) -> Callable[[Callable[..., T]], Callable[..., T]]:
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: object, **kwargs: object) -> T:
            last_exc: Exception | None = None
            wait = delay
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if attempt < max_attempts - 1:
                        time.sleep(wait)
                        wait *= backoff
            msg = f"failed after {max_attempts} attempts"
            raise RuntimeError(msg) from last_exc
        return wrapper
    return decorator


def quantize_decimal(value: Decimal, precision: Decimal = Decimal("0.00000001")) -> Decimal:
    return value.quantize(precision)


def bps_to_decimal(bps: int) -> Decimal:
    return Decimal(bps) / Decimal(10000)
