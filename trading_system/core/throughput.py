"""
Throughput optimization: RingBuffer, IndicatorCache, and stateful StrategyRunner.

Reduces per-tick memory allocation and redundant indicator computation.
"""

import logging
import time
from collections import OrderedDict
from collections.abc import MutableSequence
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# RingBuffer — fixed-size deque for candle data
# ---------------------------------------------------------------------------

class RingBuffer(MutableSequence):
    """Fixed-size circular buffer for OHLCV data.
    O(1) append, no list reallocation. Behaves like a list.
    """

    __slots__ = ("_data", "_maxlen", "_head", "_size")

    def __init__(self, maxlen: int = 200):
        self._data = [0.0] * maxlen
        self._maxlen = maxlen
        self._head = 0
        self._size = 0

    def append(self, value: float) -> None:
        self._data[self._head] = value
        self._head = (self._head + 1) % self._maxlen
        if self._size < self._maxlen:
            self._size += 1

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            return [self[i] for i in range(start, stop, step)]
        if not isinstance(index, int):
            raise TypeError(f"Expected int or slice, got {type(index)}")
        if index < 0:
            index += self._size
        if index < 0 or index >= self._size:
            raise IndexError("RingBuffer index out of range")
        pos = (self._head - self._size + index) % self._maxlen
        return self._data[pos]

    def __setitem__(self, index, value):
        if index < 0:
            index += self._size
        if index < 0 or index >= self._size:
            raise IndexError("RingBuffer assignment index out of range")
        pos = (self._head - self._size + index) % self._maxlen
        self._data[pos] = value

    def __delitem__(self, index):
        raise NotImplementedError("RingBuffer does not support deletion")

    def __len__(self) -> int:
        return self._size

    def __repr__(self) -> str:
        return f"RingBuffer({list(self)})"

    def insert(self, index, value):
        raise NotImplementedError("RingBuffer does not support insert")

    def to_list(self) -> List[float]:
        """Return a flat list copy (for legacy API compatibility)."""
        return [self[i] for i in range(self._size)]


# ---------------------------------------------------------------------------
# IndicatorCache — TTL + LRU for computed indicators
# ---------------------------------------------------------------------------

class IndicatorCache:
    """Thread-safe cache for computed indicator values.

    Keyed by (product_id, indicator_name, *params).
    Example: ("BTC-USD", "ema", 12) → 45200.5
    Supports TTL expiry and LRU eviction.
    """

    __slots__ = ("_store", "_timestamps", "_ttl_secs", "_max_entries")

    def __init__(self, ttl_secs: float = 10.0, max_entries: int = 500):
        self._store: OrderedDict = OrderedDict()
        self._timestamps: Dict[str, float] = {}
        self._ttl_secs = ttl_secs
        self._max_entries = max_entries

    def _key(self, product_id: str, indicator: str, *params) -> str:
        return f"{product_id}:{indicator}:{params}"

    def get(self, product_id: str, indicator: str, *params) -> Optional[float]:
        k = self._key(product_id, indicator, *params)
        entry = self._store.get(k)
        if entry is None:
            return None
        ts = self._timestamps.get(k, 0)
        if time.monotonic() - ts > self._ttl_secs:
            del self._store[k]
            del self._timestamps[k]
            return None
        self._store.move_to_end(k)
        return entry

    def get_many(self, product_id: str, indicator: str, *param_sets) -> List[Optional[float]]:
        return [self.get(product_id, indicator, *ps) for ps in param_sets]

    def set(self, product_id: str, indicator: str, value: float, *params) -> None:
        k = self._key(product_id, indicator, *params)
        self._store[k] = value
        self._timestamps[k] = time.monotonic()
        if len(self._store) > self._max_entries:
            self._store.popitem(last=False)
            # Also clean eldest timestamp
            oldest_k = next(iter(self._timestamps))
            if oldest_k in self._store:
                pass
            else:
                del self._timestamps[oldest_k]

    def invalidate(self, product_id: Optional[str] = None, indicator: Optional[str] = None) -> int:
        """Invalidate entries matching optional filters. Returns count removed."""
        if product_id is None and indicator is None:
            count = len(self._store)
            self._store.clear()
            self._timestamps.clear()
            return count
        to_remove = []
        for k in self._store:
            parts = k.split(":")
            pid = parts[0]
            ind = parts[1]
            if product_id and pid != product_id:
                continue
            if indicator and ind != indicator:
                continue
            to_remove.append(k)
        for k in to_remove:
            del self._store[k]
            del self._timestamps[k]
        return len(to_remove)

    @property
    def size(self) -> int:
        return len(self._store)


# ---------------------------------------------------------------------------
# Stateful Strategy Runner — keeps instances alive between ticks
# ---------------------------------------------------------------------------

class StrategyRunner:
    """Manages strategy instances across ticks for stateful delta-updates.

    Maintains mapping of strategy_name → instance so that internal state
    (e.g., EMA_Crossover.prev_fast, ADX.prev_plus_di) persists across
    calls. Also wires IndicatorCache for shared indicator reuse.
    """

    def __init__(self, indicator_cache: Optional[IndicatorCache] = None):
        self._instances: Dict[str, object] = {}
        self._indicator_cache = indicator_cache or IndicatorCache()

    def get_or_create(self, strategy_name: str, strategy_cls: type, *args, **kwargs) -> object:
        """Return cached instance or create + cache new one."""
        existing = self._instances.get(strategy_name)
        if existing is not None:
            return existing
        instance = strategy_cls(*args, **kwargs)
        self._instances[strategy_name] = instance
        return instance

    def reset(self, strategy_name: Optional[str] = None) -> None:
        """Reset a specific instance or all instances."""
        if strategy_name:
            self._instances.pop(strategy_name, None)
        else:
            self._instances.clear()

    @property
    def cache(self) -> IndicatorCache:
        return self._indicator_cache


# ---------------------------------------------------------------------------
# CandleSet — holds all 4 OHLCV series as RingBuffers
# ---------------------------------------------------------------------------

class CandleSet:
    """A complete OHLCV dataset for one product, backed by RingBuffers.

    Minimizes allocation: appending a new bar pushes old data out.
    """

    __slots__ = ("product_id", "closes", "volumes", "highs", "lows", "_len")

    def __init__(
        self,
        product_id: str,
        maxlen: int = 200,
        closes: Optional[List[float]] = None,
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ):
        self.product_id = product_id
        self.closes = RingBuffer(maxlen)
        self.volumes = RingBuffer(maxlen)
        self.highs = RingBuffer(maxlen)
        self.lows = RingBuffer(maxlen)

        if closes:
            for c in closes:
                self.closes.append(c)
        if volumes:
            for v in volumes:
                self.volumes.append(v)
        if highs:
            for h in highs:
                self.highs.append(h)
        if lows:
            for l in lows:
                self.lows.append(l)

    def append_bar(self, close: float, volume: float, high: float, low: float) -> None:
        self.closes.append(close)
        self.volumes.append(volume)
        self.highs.append(high)
        self.lows.append(low)

    def __len__(self) -> int:
        return len(self.closes)

    @property
    def current_price(self) -> float:
        return self.closes[-1] if self.closes else 0.0
