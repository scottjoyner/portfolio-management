"""
Async REST API candle fetcher with incremental updates and circuit breakers.

Uses Coinbase Exchange public API with aiohttp connection pooling.
Supports incremental fetches (only new candles) and per-product circuit breakers.
"""

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from collections import deque

import aiohttp

log = logging.getLogger(__name__)

_EXCHANGE_API = "https://api.exchange.coinbase.com"
_USER_AGENT = "PortfolioOptimizer/1.0"

# Granularity mapping (seconds -> Coinbase API string - seconds as string)
_GRANULARITY_MAP = {
    60: "60",
    300: "300",
    900: "900",
    3600: "3600",
    21600: "21600",
    86400: "86400",
}

# Cache TTL by granularity (seconds)
_CACHE_TTL = {
    60: 15.0,
    300: 45.0,
    900: 90.0,
    3600: 180.0,
    21600: 300.0,
    86400: 600.0,
}

# Circuit breaker state
@dataclass
class CircuitBreaker:
    failures: int = 0
    last_failure: float = 0.0
    is_open: bool = False
    consecutive_timeouts: int = 0
    
    def record_success(self):
        self.failures = 0
        self.consecutive_timeouts = 0
        self.is_open = False
    
    def record_failure(self, is_timeout: bool = False):
        self.failures += 1
        self.last_failure = time.time()
        if is_timeout:
            self.consecutive_timeouts += 1
        if self.failures >= 5 or self.consecutive_timeouts >= 3:
            self.is_open = True
            log.warning(f"Circuit breaker OPEN for product")
    
    def can_attempt(self) -> bool:
        if not self.is_open:
            return True
        # Half-open after 60s
        if time.time() - self.last_failure > 60:
            self.is_open = False
            self.failures = 0
            return True
        return False


# Global state
_CANDLE_CACHE: Dict[Tuple[str, int, int], Tuple[float, List[Tuple[int, float, float, float, float, float]]]] = {}
_CANDLE_CACHE_LOCK = asyncio.Lock()
_CIRCUIT_BREAKERS: Dict[str, CircuitBreaker] = {}
_SESSION_LOCK = asyncio.Lock()
_tls = threading.local()


async def _get_session() -> aiohttp.ClientSession:
    session = getattr(_tls, 'http_session', None)
    if session is None or session.closed:
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=5,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
        )
        _tls.http_session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={"User-Agent": _USER_AGENT},
        )
    return _tls.http_session


async def close_session():
    session = getattr(_tls, 'http_session', None)
    if session and not session.closed:
        await session.close()
        _tls.http_session = None


def _cache_ttl_s(granularity: int) -> float:
    return _CACHE_TTL.get(granularity, 60.0)


def _granularity_to_cb_str(granularity: int) -> str:
    return str(_GRANULARITY_MAP.get(granularity, granularity))


def _persist_nas(product_id: str, granularity: int,
                 candles: List[Tuple[int, float, float, float, float, float]]) -> None:
    """Best-effort durable write of fetched candles to the NAS feed cache."""
    if not candles:
        return
    try:
        from data.feed_cache import save_candles
        save_candles("coinbase_candles", product_id, granularity, candles)
    except Exception as e:  # pragma: no cover - durability is best-effort
        log.debug("NAS persist skipped for %s: %s", product_id, e)


def _normalize_candles(data: List, product_id: str) -> List[Tuple[int, float, float, float, float, float]]:
    """Normalize Coinbase API response to (ts, open, high, low, close, volume)."""
    result = []
    for c in reversed(data):
        if isinstance(c, (list, tuple)) and len(c) >= 6:
            ts, lo, hi, op, cl, vol = c[:6]
            result.append((int(ts), float(op), float(hi), float(lo), float(cl), float(vol)))
        elif isinstance(c, dict):
            ts = int(c.get("start", c.get("time", 0)))
            op = float(c.get("open", 0))
            hi = float(c.get("high", 0))
            lo = float(c.get("low", 0))
            cl = float(c.get("close", 0))
            vol = float(c.get("volume", 0))
            result.append((ts, op, hi, lo, cl, vol))
    return result


async def fetch_candles_rest(
    product_id: str,
    granularity: int = 3600,
    limit: int = 100,
    after_ts: Optional[int] = None,
    timeout: float = 10.0,
) -> List[Tuple[int, float, float, float, float, float]]:
    """
    Fetch candles via REST API with incremental support.
    
    Args:
        product_id: e.g., "BTC-USD"
        granularity: seconds (60, 300, 900, 3600, 21600, 86400)
        limit: max candles (max 300 per Coinbase)
        after_ts: only fetch candles AFTER this unix timestamp (incremental)
    
    Returns:
        List of (ts, open, high, low, close, volume) sorted oldest-first
    """
    cb_granularity = _granularity_to_cb_str(granularity)
    cache_key = (product_id, granularity, min(limit, 300) if limit else 0)
    now = time.time()
    ttl_s = _cache_ttl_s(granularity)
    
    # Check cache first (only for non-incremental fetches)
    if after_ts is None:
        async with _CANDLE_CACHE_LOCK:
            cached = _CANDLE_CACHE.get(cache_key)
            if cached and (now - cached[0]) < ttl_s:
                return list(cached[1])
    
    # Check circuit breaker
    cb = _CIRCUIT_BREAKERS.get(product_id)
    if cb and not cb.can_attempt():
        log.debug(f"Circuit breaker open for {product_id}, returning cached data")
        async with _CANDLE_CACHE_LOCK:
            cached = _CANDLE_CACHE.get(cache_key)
            if cached:
                return list(cached[1])
        return []
    
    url = f"{_EXCHANGE_API}/products/{product_id}/candles"
    params = {"granularity": cb_granularity}
    if limit:
        params["limit"] = str(min(limit, 300))
    if after_ts:
        params["start"] = datetime.fromtimestamp(after_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    
    session = await _get_session()
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status == 429:
                # Rate limited
                if cb:
                    cb.record_failure(is_timeout=False)
                retry_after_str = resp.headers.get("Retry-After", "5")
                try:
                    retry_after = int(retry_after_str)
                except (ValueError, TypeError):
                    retry_after = 5
                await asyncio.sleep(min(retry_after, 30))
                return await fetch_candles_rest(product_id, granularity, limit, after_ts, timeout)
            
            if resp.status != 200:
                log.debug(f"REST candle fetch {product_id}: HTTP {resp.status}")
                if cb:
                    cb.record_failure()
                return []
            
            data = await resp.json()
            result = _normalize_candles(data, product_id)

            if cb:
                cb.record_success()

            # Durable write (every fetch, full or incremental) for backtesting
            _persist_nas(product_id, granularity, result)

            # Update cache (only for full fetches)
            if after_ts is None and result:
                async with _CANDLE_CACHE_LOCK:
                    _CANDLE_CACHE[cache_key] = (time.time(), list(result))
            
            return result
            
    except asyncio.TimeoutError:
        log.debug(f"REST candle fetch {product_id}: timeout")
        if cb:
            cb.record_failure(is_timeout=True)
        return []
    except Exception as e:
        log.info(f"REST candle fetch {product_id} failed: {e}")
        if cb:
            cb.record_failure()
        return []


async def fetch_candles_batch(
    products: List[str],
    granularity: int = 3600,
    limit: int = 100,
    max_concurrent: int = 5,
    after_ts_map: Optional[Dict[str, int]] = None,
) -> Dict[str, List[Tuple[int, float, float, float, float, float]]]:
    """
    Fetch candles for multiple products concurrently with semaphore limiting.
    
    Args:
        products: List of product_ids
        granularity: seconds
        limit: max candles per product
        max_concurrent: max concurrent requests (semaphore)
        after_ts_map: optional dict of product_id -> after_ts for incremental fetches
    
    Returns:
        Dict of product_id -> list of candles
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    after_ts_map = after_ts_map or {}
    
    async def fetch_one(pid: str):
        async with semaphore:
            after_ts = after_ts_map.get(pid)
            candles = await fetch_candles_rest(pid, granularity, limit, after_ts)
            return pid, candles
    
    tasks = [fetch_one(pid) for pid in products]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    output = {}
    for result in results:
        if isinstance(result, Exception):  # pragma: no cover - fetch_one never raises
            log.info(f"Batch fetch error: {result}")
            continue
        pid, candles = result
        if candles:
            output[pid] = candles
    
    return output


async def fetch_incremental_batch(
    products: List[str],
    granularity: int,
    last_cached_ts: Dict[str, int],
    limit: int = 100,
    max_concurrent: int = 20,
) -> Tuple[Dict[str, List[Tuple[int, float, float, float, float, float]]], Dict[str, int]]:
    """
    Fetch only new candles since last_cached_ts for each product.
    
    Returns:
        (new_candles_dict, updated_last_ts_dict)
    """
    after_ts_map = {pid: ts for pid, ts in last_cached_ts.items() if ts > 0}
    new_candles = await fetch_candles_batch(products, granularity, limit, max_concurrent, after_ts_map)
    
    updated_ts = dict(last_cached_ts)
    for pid, candles in new_candles.items():
        if candles:  # pragma: no cover - batch never inserts empty lists
            updated_ts[pid] = max(c[0] for c in candles)
    
    return new_candles, updated_ts


async def invalidate_candle_cache(product_id: Optional[str] = None) -> None:
    """Invalidate REST candle cache for one product or all products."""
    async with _CANDLE_CACHE_LOCK:
        if product_id is None:
            _CANDLE_CACHE.clear()
            return
        keys = [k for k in _CANDLE_CACHE.keys() if k[0] == product_id]
        for key in keys:
            _CANDLE_CACHE.pop(key, None)


def candle_arrays(
    candles: List[Tuple[int, float, float, float, float, float]],
) -> Dict[str, List[float]]:
    """Convert candle list to per-field arrays for strategy engine."""
    closes = [c[4] for c in candles]
    volumes = [c[5] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    return {"closes": closes, "volumes": volumes, "highs": highs, "lows": lows}


# Sync wrapper for backward compatibility
def fetch_candles_rest_sync(
    product_id: str,
    granularity: int = 3600,
    limit: int = 100,
    timeout: float = 10.0,
) -> List[Tuple[int, float, float, float, float, float]]:
    """Synchronous wrapper for backward compatibility."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(
            fetch_candles_rest(product_id, granularity, limit, timeout=timeout)
        )
    finally:
        # Don't try to close session here - it causes segfaults in thread pools
        # The session is thread-local and will be cleaned up when thread exits
        pass


def fetch_candles_batch_sync(
    products: List[str],
    granularity: int = 3600,
    limit: int = 100,
    max_workers: int = 12,
) -> Dict[str, List[Tuple[int, float, float, float, float, float]]]:
    """Synchronous wrapper for backward compatibility."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(
            fetch_candles_batch(products, granularity, limit, max_concurrent=max_workers)
        )
    finally:
        # Don't try to close session here - it causes segfaults in thread pools
        # The session is thread-local and will be cleaned up when thread exits
        pass