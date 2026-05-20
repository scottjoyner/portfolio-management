from __future__ import annotations


def quote_stale(quote_age_ms: int, max_age_ms: int = 3_000) -> bool:
    return quote_age_ms > max_age_ms
