"""NAS-backed feed cache for durable market-data persistence.

Every external data feed the system consumes (Coinbase candles, on-chain
metrics, prediction-market snapshots, news) is cached here on the NAS so it
survives process restarts and can be replayed for backtesting.

Storage layout (root from ``NAS_FEED_ROOT`` env, default
``/media/scott/NAS3/feed_cache``)::

    <root>/coinbase_candles/<SYMBOL>/<GRANULARITY>.parquet
    <root>/onchain/<SOURCE>/<SYMBOL>.parquet
    <root>/prediction_markets/<PLATFORM>.parquet
    <root>/news/<SOURCE>.jsonl

Candles are stored as ``[t, o, h, l, c, v]`` (timestamp + OHLCV). Writes are
append + de-duplicate by timestamp so re-fetching the same window is safe.

The default root is ``/media/scott/NAS3/feed_cache`` (env ``NAS_FEED_ROOT``),
but if the NAS mount is not writable by the running user (e.g. dev runs as a
non-root account), it transparently falls back to ``<repo>/data/feed_cache`` so
durability still works locally. Under the production systemd unit (root) the
NAS path is used.

A small in-process TTL layer (``mem_cache``) keeps hot windows off disk for
sub-millisecond reads during a live tick.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

NAS_FEED_ROOT = os.environ.get("NAS_FEED_ROOT", "/media/scott/NAS3/feed_cache").rstrip("/")

_HAVE_PARQUET = True
try:
    import pandas as pd  # noqa: F401
except Exception:  # pragma: no cover - exercised only when pandas missing
    _HAVE_PARQUET = False

# candle columns
COLUMNS = ["t", "o", "h", "l", "c", "v"]

_lock = threading.RLock()
_mem: Dict[str, Tuple[float, List[List[float]]]] = {}
_MEM_TTL = 30.0  # seconds

_RESOLVED_ROOT: Optional[str] = None


def _root() -> str:
    """Return a writable cache root, resolving NAS -> local fallback once."""
    global _RESOLVED_ROOT
    if _RESOLVED_ROOT is not None:
        return _RESOLVED_ROOT
    repo_local = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "feed_cache"
    )
    for cand in (NAS_FEED_ROOT, repo_local):
        try:
            os.makedirs(cand, exist_ok=True)
            # existence is not enough — prove we can actually write a subdir
            probe = os.path.join(cand, ".write_test_" + str(os.getpid()))
            os.mkdir(probe)
            os.rmdir(probe)
            _RESOLVED_ROOT = cand
            if cand != NAS_FEED_ROOT:
                logger.warning("NAS feed root not writable; using local fallback %s", cand)
            return cand
        except Exception as e:  # pragma: no cover - defensive
            logger.debug("feed root %s unavailable: %s", cand, e)
    _RESOLVED_ROOT = repo_local
    return _RESOLVED_ROOT


def ensure_root() -> str:
    return _root()


def _path(kind: str, *parts: str) -> str:
    return os.path.join(_root(), kind, *[str(p) for p in parts])


def _norm(candles: Sequence[Sequence[float]]) -> List[List[float]]:
    out = []
    for c in candles:
        row = [float(x) for x in c[:6]]
        out.append(row)
    return out


def _dedup(candles: List[List[float]]) -> List[List[float]]:
    seen = set()
    out: List[List[float]] = []
    for c in sorted(candles, key=lambda x: x[0]):
        t = c[0]
        if t in seen:
            continue
        seen.add(t)
        out.append(c)
    return out


def _mem_key(kind: str, symbol: str, granularity: object) -> str:
    return f"{kind}:{symbol}:{granularity}"


# ── public API ────────────────────────────────────────────────────────────

def save_candles(kind: str, symbol: str, granularity: object,
                 candles: Sequence[Sequence[float]]) -> int:
    """Append + de-duplicate candles to durable storage. Returns bars written."""
    if not candles:
        return 0
    candles = _dedup(_norm(candles))
    path = _path(kind, symbol, f"{granularity}.parquet")
    merged_rows: List[List[float]] = candles
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if _HAVE_PARQUET:
            import pandas as pd

            new = pd.DataFrame(candles, columns=COLUMNS)
            if os.path.exists(path):
                old = pd.read_parquet(path)
                merged = (
                    pd.concat([old, new])
                    .drop_duplicates(subset=["t"])
                    .sort_values("t")
                    .reset_index(drop=True)
                )
            else:
                merged = new
            merged.to_parquet(path, index=False)
            merged_rows = [[float(x) for x in r] for r in merged.values.tolist()]
        else:  # pragma: no cover - pandas path is the default
            existing = _load_jsonl(path)
            seen = {r["t"] for r in existing}
            for c in candles:
                if c[0] not in seen:
                    existing.append({"t": c[0], "o": c[1], "h": c[2], "l": c[3], "c": c[4], "v": c[5]})
            existing.sort(key=lambda r: r["t"])
            _write_jsonl(path, existing)
            merged_rows = [[r["t"], r["o"], r["h"], r["l"], r["c"], r["v"]] for r in existing]
        with _lock:
            _mem[_mem_key(kind, symbol, granularity)] = (time.time(), merged_rows)
        return len(candles)
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("feed_cache save failed %s: %s", path, e)
        return 0


def load_candles(kind: str, symbol: str, granularity: object,
                 limit: Optional[int] = None) -> List[List[float]]:
    """Load candles from durable storage (or hot mem cache). Oldest-first."""
    key = _mem_key(kind, symbol, granularity)
    with _lock:
        entry = _mem.get(key)
        if entry and (time.time() - entry[0]) < _MEM_TTL:
            rows = entry[1]
        else:
            rows = []
    if not rows:
        path = _path(kind, symbol, f"{granularity}.parquet")
        if not os.path.exists(path):
            return []
        try:
            if _HAVE_PARQUET:
                import pandas as pd

                df = pd.read_parquet(path)
                rows = [[float(x) for x in r] for r in df.values.tolist()]
            else:  # pragma: no cover - pandas path is the default
                rows = [[r["t"], r["o"], r["h"], r["l"], r["c"], r["v"]] for r in _load_jsonl(path)]
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("feed_cache load failed %s: %s", path, e)
            return []
    if limit:
        rows = rows[-limit:]
    return rows


def new_since(kind: str, symbol: str, granularity: object, last_ts: float) -> List[List[float]]:
    """Return only stored candles with timestamp > last_ts (oldest-first)."""
    return [r for r in load_candles(kind, symbol, granularity) if r[0] > last_ts]


def record(kind: str, symbol: str, granularity: object,
           candles: Sequence[Sequence[float]]) -> int:
    """Convenience: persist candles and return count written (alias of save)."""
    return save_candles(kind, symbol, granularity, candles)


# ── non-OHLCV record helpers (on-chain / prediction markets / news) ────────

def save_records(kind: str, name: str, records: Sequence[dict]) -> int:
    """Append JSON records for non-candle feeds (on-chain, PM, news)."""
    if not records:
        return 0
    path = _path(kind, f"{name}.jsonl")
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        _write_jsonl(path, list(records), append=True)
        return len(records)
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("feed_cache save_records failed %s: %s", path, e)
        return 0


def load_records(kind: str, name: str, limit: Optional[int] = None) -> List[dict]:
    path = _path(kind, f"{name}.jsonl")
    if not os.path.exists(path):
        return []
    try:
        rows = _load_jsonl(path)
        if limit:
            rows = rows[-limit:]
        return rows
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("feed_cache load_records failed %s: %s", path, e)
        return []


# ── internal jsonl helpers (pandas-free fallback / non-OHLCV) ──────────────

def _load_jsonl(path: str) -> List[dict]:
    rows: List[dict] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _write_jsonl(path: str, rows: List[dict], append: bool = False) -> None:
    mode = "a" if append else "w"
    with open(path, mode) as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
