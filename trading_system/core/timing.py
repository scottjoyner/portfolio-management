import asyncio
import functools
import json
import logging
import subprocess
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from typing import Any, Callable, Dict, List, Optional

_logger = logging.getLogger(__name__)


def timed(logger: logging.Logger = _logger, level: int = logging.DEBUG) -> Callable:
    """Decorator that logs the wall-clock duration of the decorated function.

    Usage::

        @timed()
        def my_func():
            ...

        @timed(logger=my_logger, level=logging.INFO)
        def other_func():
            ...
    """
    def decorator(fn: Callable) -> Callable:
        if asyncio.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                start = time.perf_counter()
                try:
                    return await fn(*args, **kwargs)
                finally:
                    elapsed = time.perf_counter() - start
                    logger.log(level, "%s took %.4fs", fn.__name__, elapsed)

            return async_wrapper
        else:

            @functools.wraps(fn)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                start = time.perf_counter()
                try:
                    return fn(*args, **kwargs)
                finally:
                    elapsed = time.perf_counter() - start
                    logger.log(level, "%s took %.4fs", fn.__name__, elapsed)

            return sync_wrapper

    return decorator


# ---------------------------------------------------------------------------
# LatencyProfiler — stage-by-stage tick timing with history
# ---------------------------------------------------------------------------


@dataclass
class StageStats:
    """Aggregate stats for a named stage."""
    name: str
    count: int
    last_ms: float
    min_ms: float
    max_ms: float
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float


class LatencyProfiler:
    """Per-tick, per-stage latency measurement with rolling history.

    Usage::

        profiler = LatencyProfiler()

        with profiler.measure("fetch_candles"):
            data = fetch_data()

        profiler.record("compute", 1.5)  # ms

        summary = profiler.summary()  # per-stage stats
    """

    def __init__(self, max_history: int = 500):
        self._stages: Dict[str, List[float]] = defaultdict(list)
        self._max_history = max_history
        self._lock = threading.Lock()
        self._tick_timestamps: List[float] = []
        self._tick_durations: List[float] = []

    @contextmanager
    def measure(self, stage_name: str):
        """Context manager: times the block and records duration."""
        start = time.perf_counter()
        try:
            yield
        finally:
            ms = (time.perf_counter() - start) * 1000.0
            self.record(stage_name, ms)

    def record(self, stage_name: str, duration_ms: float) -> None:
        """Record a single stage duration."""
        with self._lock:
            buf = self._stages[stage_name]
            buf.append(duration_ms)
            if len(buf) > self._max_history:
                buf.pop(0)

    def record_tick(self) -> None:
        """Mark a tick boundary for tick-duration tracking."""
        now = time.perf_counter()
        with self._lock:
            if self._tick_timestamps:
                self._tick_durations.append((now - self._tick_timestamps[-1]) * 1000.0)
                if len(self._tick_durations) > self._max_history:
                    self._tick_durations.pop(0)
            self._tick_timestamps.append(now)
            if len(self._tick_timestamps) > self._max_history:
                self._tick_timestamps.pop(0)

    @staticmethod
    def _stats(values: List[float]) -> Dict[str, float]:
        if not values:
            return {"count": 0, "min": 0, "max": 0, "avg": 0, "p50": 0, "p95": 0, "p99": 0}
        s = sorted(values)
        n = len(s)
        return {
            "count": n,
            "min": s[0],
            "max": s[-1],
            "avg": sum(s) / n,
            "p50": s[n // 2],
            "p95": s[int(n * 0.95)],
            "p99": s[int(n * 0.99)],
        }

    def summary(self) -> Dict[str, Dict[str, float]]:
        """Return per-stage stats dict."""
        with self._lock:
            result = {}
            for stage, vals in self._stages.items():
                result[stage] = self._stats(vals)
            if self._tick_durations:
                result["_tick"] = self._stats(self._tick_durations)
            return result

    def as_json(self) -> str:
        return json.dumps(self.summary(), indent=2, default=str)


# ---------------------------------------------------------------------------
# Ping / HTTP Round-Trip Checks
# ---------------------------------------------------------------------------

_COINBASE_HOSTS = [
    ("api.coinbase.com", "Coinbase API (auth)"),
    ("api.exchange.coinbase.com", "Coinbase Exchange API (public)"),
    ("ws-feed.exchange.coinbase.com", "Coinbase WebSocket feed"),
]


def ping_host(host: str, count: int = 3, timeout: int = 5) -> Dict[str, float]:
    """ICMP ping to a host. Returns min/avg/max/mdev in ms."""
    try:
        out = subprocess.run(
            ["ping", "-c", str(count), "-W", str(timeout), host],
            capture_output=True, text=True, timeout=timeout + 2,
        )
        for line in out.stdout.splitlines():
            if "rtt min/avg/max" in line or "round-trip" in line or "min/avg/max" in line:
                # Handle variable formats:
                #   Standard:  rtt min/avg/max/mdev = 1.2/2.3/3.4/0.5 ms
                #   BusyBox:   rtt min/avg/max = 1.2/2.3/3.4 ms
                #   macOS:     round-trip min/avg/max/stddev = 1.2/2.3/3.4/0.5 ms
                after_eq = line.split("=")[-1].strip()
                parts = [p for p in after_eq.replace("ms", "").replace("=", "").split("/") if p.strip()]
                if len(parts) < 3:
                    continue
                return {
                    "host": host,
                    "min_ms": float(parts[0]),
                    "avg_ms": float(parts[1]),
                    "max_ms": float(parts[2]),
                    "mdev_ms": float(parts[3]) if len(parts) > 3 else float(parts[2]),
                }
        return {"host": host, "error": "no ping stats found", "raw": out.stdout[:200]}
    except Exception as e:
        return {"host": host, "error": str(e)}


def measure_coinbase_latency() -> List[Dict]:
    """Measure ping to all Coinbase hosts."""
    return [ping_host(host) for host, _ in _COINBASE_HOSTS]


def http_roundtrip(url: str, timeout: int = 5) -> Dict[str, float]:
    """Measure HTTPS GET round-trip time to a URL."""
    import urllib.request
    start = time.perf_counter()
    try:
        req = urllib.request.Request(url, method="GET")
        resp = urllib.request.urlopen(req, timeout=timeout)
        body = resp.read()
        ms = (time.perf_counter() - start) * 1000.0
        return {
            "url": url,
            "status": resp.status,
            "bytes": len(body),
            "rtt_ms": round(ms, 2),
        }
    except Exception as e:
        ms = (time.perf_counter() - start) * 1000.0
        return {"url": url, "error": str(e), "rtt_ms": round(ms, 2)}


__all__ = ["timed", "LatencyProfiler", "ping_host", "measure_coinbase_latency", "http_roundtrip"]
