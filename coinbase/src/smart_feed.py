"""
Tiered smart feed refresh manager.

Eliminates redundant candle fetches across the optimizer and trader by
providing a shared cache with tier-aware refresh schedules:

  Tier 0 (CRITICAL)  — core BTC-USD/ETH-USD/SOL-USD + any open position
  Tier 1 (HOT)       — high-volume growth pairs, active scan candidates
  Tier 2 (WARM)      — remaining universe pairs
  Tier 3 (COLD)      — everything else, fetched on demand

Each consumer calls get_candles() / get_candles_batch() instead of raw I/O.
The manager serves cached data if within tier TTL, fetches fresh if stale.
A background daemon thread cycles through tiers proportionally so that
critical feeds are refreshed every loop whereas cold feeds are infrequent.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tiers
# ---------------------------------------------------------------------------

TIER_0 = 0  # CRITICAL  — core positions, refreshed every loop
TIER_1 = 1  # HOT       — top volume, refreshed every N loops
TIER_2 = 2  # WARM      — rest of universe
TIER_3 = 3  # COLD      — on demand

TIER_NAMES = {0: "CRITICAL", 1: "HOT", 2: "WARM", 3: "COLD"}

# Default TTL per tier (seconds) — tier-0 is refreshed every refresh cycle
# tier-1 gets refreshed every other cycle, tier-2 every few cycles, tier-3 never proactively
DEFAULT_TIER_TTL = {
    TIER_0: 15.0,
    TIER_1: 60.0,
    TIER_2: 300.0,
    TIER_3: 900.0,
}

# How many background-loop iterations between proactive refresh of each tier
# Tier 0: every loop, Tier 1: every 4th loop, Tier 2: every 10th, Tier 3: off
DEFAULT_TIER_SKIP = {
    TIER_0: 1,
    TIER_1: 4,
    TIER_2: 10,
    TIER_3: 0,  # never proactively refreshed
}

# How many products per tier to touch per loop (cap to avoid thundering herd)
DEFAULT_TIER_BATCH = {
    TIER_0: 5,
    TIER_1: 10,
    TIER_2: 20,
    TIER_3: 0,
}

# ---------------------------------------------------------------------------
# Feed state
# ---------------------------------------------------------------------------


@dataclass
class CandleCacheEntry:
    ts: float = 0.0
    candles: list = field(default_factory=list)


@dataclass
class ProductFeedState:
    product_id: str
    tier: int = TIER_3
    last_fetch: float = 0.0
    volume_24h: float = 0.0
    has_position: bool = False


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------


class SmartFeedRefreshManager:
    """Coordinate feed freshness across all consumers with tiered refresh.

    Usage::

        manager = SmartFeedRefreshManager(
            fetch_fn=fetch_candles_rest_sync,
            batch_fn=fetch_candles_batch_sync,
        )
        manager.set_critical(["BTC-USD", "ETH-USD", "SOL-USD"])
        manager.start()               # background daemon thread

        # Consumers — always go through the manager
        candles = manager.get_candles("BTC-USD", granularity=3600, limit=100)
        results = manager.get_candles_batch(["ETH-USD", "SOL-USD"], ...)

        # Promote products that become positions
        manager.add_position("XRP-USD")

        # On demand — force refresh of all critical feeds right now
        manager.refresh_critical_now()

        manager.stop()
    """

    def __init__(
        self,
        fetch_fn: Optional[Callable] = None,
        batch_fn: Optional[Callable] = None,
        tier_ttl: Optional[Dict[int, float]] = None,
        tier_skip: Optional[Dict[int, int]] = None,
        tier_batch: Optional[Dict[int, int]] = None,
        io_pool: Optional[ThreadPoolExecutor] = None,
        interval: float = 5.0,
        max_stale_s: float = 600.0,
    ):
        self._fetch_fn = fetch_fn
        self._batch_fn = batch_fn
        self._tier_ttl = dict(tier_ttl or DEFAULT_TIER_TTL)
        self._tier_skip = dict(tier_skip or DEFAULT_TIER_SKIP)
        self._tier_batch = dict(tier_batch or DEFAULT_TIER_BATCH)
        self._interval = interval
        self._max_stale_s = max_stale_s
        self._io_pool = io_pool or ThreadPoolExecutor(
            max_workers=3, thread_name_prefix="smart_feed"
        )
        # Stats for observability / health
        self._stale_served = 0
        self._fetch_failures = 0

        # Product state
        self._products: Dict[str, ProductFeedState] = {}
        self._lock = threading.Lock()

        # Shared candle cache: {(pid, granularity, limit) -> CandleCacheEntry}
        self._candle_cache: Dict[Tuple[str, int, int], CandleCacheEntry] = {}
        self._cache_lock = threading.Lock()

        # Critical product set (tier-0, always refreshed)
        self._critical: Set[str] = set()
        # Positions (promoted to tier-0)
        self._positions: Set[str] = set()

        # Background thread
        self._thread: Optional[threading.Thread] = None
        self._shutdown = threading.Event()

        # Loop iteration counter for tier skipping
        self._loop_count = 0

        # Callback for when critical data arrives fresh
        self.on_critical_refresh: Optional[Callable[[List[str]], None]] = None

    # ── Product management ────────────────────────────────────────────

    def set_critical(self, products: List[str]):
        """Set the always-critical product set (core BTC/ETH/SOL)."""
        with self._lock:
            self._critical = set(products)
            for pid in products:
                self._ensure_product(pid).tier = TIER_0

    def add_position(self, product_id: str):
        """Promote a product to tier-0 because we hold a position in it."""
        with self._lock:
            self._positions.add(product_id)
            state = self._ensure_product(product_id)
            state.tier = TIER_0
            state.has_position = True

    def remove_position(self, product_id: str):
        """Demote a product when a position is closed."""
        with self._lock:
            self._positions.discard(product_id)
            state = self._products.get(product_id)
            if state and product_id not in self._critical:
                state.tier = TIER_3
                state.has_position = False

    def set_volume(self, product_id: str, volume_24h: float):
        """Update volume ranking for tier assignment."""
        with self._lock:
            state = self._ensure_product(product_id)
            state.volume_24h = volume_24h
            if product_id in self._critical or product_id in self._positions:
                return
            if volume_24h >= 100_000_000:
                state.tier = TIER_1
            elif volume_24h >= 10_000_000:
                state.tier = TIER_2
            else:
                state.tier = TIER_3

    def _ensure_product(self, pid: str) -> ProductFeedState:
        if pid not in self._products:
            self._products[pid] = ProductFeedState(product_id=pid)
        return self._products[pid]

    def known_products(self) -> List[str]:
        with self._lock:
            return list(self._products.keys())

    def products_by_tier(self, tier: int) -> List[str]:
        with self._lock:
            return [pid for pid, s in self._products.items() if s.tier == tier]

    # ── Data access ────────────────────────────────────────────────────

    def _cache_get(self, pid: str, granularity: int, limit: int) -> Optional[list]:
        key = (pid, granularity, limit)
        with self._cache_lock:
            entry = self._candle_cache.get(key)
            if entry and (time.time() - entry.ts) < self._tier_ttl.get(
                self._product_tier(pid), 60.0
            ):
                return list(entry.candles)
        return None

    def _cache_get_stale(self, pid: str, granularity: int, limit: int,
                         max_stale_s: Optional[float] = None) -> Tuple[Optional[list], float]:
        """Return cached candles even if past TTL, if within max_stale_s.

        Used for graceful degradation when a fresh fetch fails. Returns
        (candles_or_None, age_seconds).
        """
        max_stale = max_stale_s if max_stale_s is not None else self._max_stale_s
        key = (pid, granularity, limit)
        with self._cache_lock:
            entry = self._candle_cache.get(key)
            if entry:
                age = time.time() - entry.ts
                if age <= max_stale:
                    return list(entry.candles), age
        return None, 0.0

    def _cache_set(self, pid: str, granularity: int, limit: int, candles: list):
        key = (pid, granularity, limit)
        with self._cache_lock:
            self._candle_cache[key] = CandleCacheEntry(
                ts=time.time(), candles=list(candles)
            )

    def _product_tier(self, pid: str) -> int:
        with self._lock:
            s = self._products.get(pid)
            return s.tier if s else TIER_3

    def get_candles(
        self,
        product_id: str,
        granularity: int = 3600,
        limit: int = 100,
        force: bool = False,
        allow_stale: bool = True,
    ) -> list:
        """Get candles — cached if fresh, fetched if stale/forced.

        On a fetch failure, falls back to the most recent cached candles
        (within ``max_stale_s``) so consumers keep working during API
        rate-limiting or outages instead of receiving empty data.
        """
        if not force:
            cached = self._cache_get(product_id, granularity, limit)
            if cached is not None:
                return cached

        if self._fetch_fn:
            candles = self._fetch_fn(product_id, granularity=granularity, limit=limit)
        else:
            candles = self._fetch_fallback(product_id, granularity, limit)

        if candles:
            self._cache_set(product_id, granularity, limit, candles)
            with self._lock:
                s = self._ensure_product(product_id)
                s.last_fetch = time.time()
            return candles

        # Fetch failed — degrade gracefully to stale cache if available
        if allow_stale:
            stale, age = self._cache_get_stale(product_id, granularity, limit)
            if stale is not None:
                self._stale_served += 1
                self._fetch_failures += 1
                log.warning(
                    "SmartFeed: serving stale cache for %s (age=%.0fs) after fetch failure",
                    product_id, age,
                )
                return stale
        self._fetch_failures += 1
        return []

    def get_candles_batch(
        self,
        products: List[str],
        granularity: int = 3600,
        limit: int = 100,
        force: bool = False,
        allow_stale: bool = True,
    ) -> Dict[str, list]:
        """Batch get — skips products with fresh cache by default.

        Products whose fresh fetch fails fall back to stale cached candles
        (within ``max_stale_s``) when ``allow_stale`` is True.
        """
        to_fetch: List[str] = []
        result: Dict[str, list] = {}

        for pid in products:
            if not force:
                cached = self._cache_get(pid, granularity, limit)
                if cached is not None:
                    result[pid] = cached
                    continue
            to_fetch.append(pid)

        if to_fetch and self._batch_fn:
            fetched = self._batch_fn(
                to_fetch, granularity=granularity, limit=limit
            )
        elif to_fetch:
            fetched = {}
            for pid in to_fetch:
                c = self._fetch_fallback(pid, granularity, limit)
                if c:
                    fetched[pid] = c
        else:
            fetched = {}

        now = time.time()
        failed: List[str] = []
        for pid, candles in fetched.items():
            if candles:
                self._cache_set(pid, granularity, limit, candles)
                result[pid] = candles
                with self._lock:
                    s = self._ensure_product(pid)
                    s.last_fetch = now
            else:
                failed.append(pid)
        # Products we tried to fetch but got nothing back at all
        failed.extend(pid for pid in to_fetch if pid not in fetched)

        if allow_stale and failed:
            for pid in failed:
                stale, age = self._cache_get_stale(pid, granularity, limit)
                if stale is not None:
                    result[pid] = stale
                    self._stale_served += 1
                    log.warning(
                        "SmartFeed: serving stale cache for %s (age=%.0fs) after fetch failure",
                        pid, age,
                    )
            self._fetch_failures += len(failed)

        return result

    def invalidate(self, product_id: Optional[str] = None):
        """Clear cache for one product or all products."""
        with self._cache_lock:
            if product_id is None:
                self._candle_cache.clear()
            else:
                keys = [k for k in self._candle_cache if k[0] == product_id]
                for k in keys:
                    del self._candle_cache[k]

    def stats(self) -> Dict[str, Any]:
        """Return observability stats (fetch failures, stale served, tiers)."""
        with self._lock:
            tiers = {t: 0 for t in (TIER_0, TIER_1, TIER_2, TIER_3)}
            for s in self._products.values():
                tiers[s.tier] = tiers.get(s.tier, 0) + 1
        return {
            "stale_served": self._stale_served,
            "fetch_failures": self._fetch_failures,
            "products": len(self._products),
            "tiers": tiers,
            "max_stale_s": self._max_stale_s,
            "running": self.running,
        }

    def refresh_critical_now(self) -> Dict[str, list]:
        """Force an immediate fetch of all tier-0 products. Returns fresh data."""
        with self._lock:
            critical = list(self._critical | self._positions)
        if not critical:
            return {}
        log.info("SmartFeed: instant-refreshing %d critical products", len(critical))
        result = self.get_candles_batch(critical, force=True)
        fresh = [pid for pid in critical if pid in result and result[pid]]
        if fresh and self.on_critical_refresh:
            try:
                self.on_critical_refresh(fresh)
            except Exception:
                log.warning("SmartFeed: on_critical_refresh callback failed", exc_info=True)
        return result

    def refresh_all_active(self) -> Dict[str, list]:
        """Fetch all tier-0 + tier-1 products (critical + hot)."""
        with self._lock:
            active = [
                pid
                for pid, s in self._products.items()
                if s.tier <= TIER_1
            ]
        if not active:
            return {}
        return self.get_candles_batch(active, force=True)

    # ── Background daemon ─────────────────────────────────────────────

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._shutdown.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="smart-feed"
        )
        self._thread.start()
        log.info("SmartFeed: background refresh thread started (interval=%.1fs)", self._interval)

    def stop(self):
        self._shutdown.set()

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def wait_ready(self, timeout: float = 5.0):
        """Block until at least one refresh cycle of critical products completes."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                critical = list(self._critical | self._positions)
                if critical:
                    all_fresh = True
                    for pid in critical:
                        s = self._products.get(pid)
                        if not s or (time.time() - s.last_fetch) > self._tier_ttl.get(TIER_0, 15) * 2:
                            all_fresh = False
                            break
                    if all_fresh:
                        return
            time.sleep(0.5)
        log.warning("SmartFeed: wait_ready timeout after %.1fs", timeout)

    def _loop(self):
        while not self._shutdown.is_set():
            cycle_start = time.time()
            try:
                self._tick()
            except Exception:
                log.error("SmartFeed: background tick failed", exc_info=True)

            elapsed = time.time() - cycle_start
            sleep_for = max(0.0, self._interval - elapsed)
            if sleep_for > 0:
                self._shutdown.wait(sleep_for)
        log.info("SmartFeed: background thread stopped")

    def _tick(self):
        self._loop_count += 1
        loop = self._loop_count

        # Gather products that need refresh this cycle by tier
        to_fetch: List[str] = []
        with self._lock:
            for pid, state in list(self._products.items()):
                tier = state.tier
                skip = self._tier_skip.get(tier, 0)
                # tier-0 every cycle; higher tiers skip cycles
                if skip == 0:
                    continue
                if (loop % skip) != 0:
                    continue
                # Check if data is actually stale
                ttl = self._tier_ttl.get(tier, 60.0)
                if (time.time() - state.last_fetch) >= ttl * 0.8:
                    to_fetch.append(pid)

        # Apply per-tier batch cap
        capped: Dict[int, List[str]] = {t: [] for t in (TIER_0, TIER_1, TIER_2)}
        for pid in to_fetch:
            with self._lock:
                state = self._products.get(pid)
                t = state.tier if state else TIER_3
            if t in capped:
                capped[t].append(pid)

        # Execute fetches: tier-0 first (all), then tier-1, then tier-2 (capped)
        critical_pids = capped.get(TIER_0, [])
        hot_pids = capped.get(TIER_1, [])[: self._tier_batch.get(TIER_1, 50)]
        warm_pids = capped.get(TIER_2, [])[: self._tier_batch.get(TIER_2, 100)]

        if critical_pids:
            self.get_candles_batch(
                critical_pids, granularity=3600, limit=10, force=True
            )

        if hot_pids:
            self.get_candles_batch(
                hot_pids, granularity=3600, limit=10, force=True
            )

        if warm_pids:
            self.get_candles_batch(
                warm_pids, granularity=3600, limit=10, force=True
            )

    # ── Fallback ──────────────────────────────────────────────────────

    def _fetch_fallback(self, product_id: str, granularity: int, limit: int) -> list:
        """Direct HTTP fetch when no batch function is configured."""
        try:
            import urllib3
            import json
            http = urllib3.PoolManager()
            url = (
                f"https://api.exchange.coinbase.com/products/"
                f"{product_id}/candles?granularity={granularity}&limit={limit}"
            )
            r = http.request("GET", url, timeout=15)
            if r.status == 200:
                data = json.loads(r.data)
                return data if isinstance(data, list) else []
        except Exception as e:
            log.debug("SmartFeed fallback fetch failed for %s: %s", product_id, e)
        return []
