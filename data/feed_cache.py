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

# ── cache-hit / efficiency metrics (E5) ────────────────────────────────────
_metrics = {
    "candle_hits": 0,       # served from hot in-process mem cache
    "candle_misses": 0,     # read from durable storage (or empty)
    "record_hits": 0,
    "record_misses": 0,
    "candle_saves": 0,      # durable writes of candles
    "record_saves": 0,      # durable writes of non-OHLCV records
}


def get_metrics() -> dict:
    """Return a copy of the running cache-efficiency counters."""
    with _lock:
        return dict(_metrics)


def reset_metrics() -> None:
    with _lock:
        for k in _metrics:
            _metrics[k] = 0


# ── retention policy (E3) ─────────────────────────────────────────────────
# Max bars retained per candle granularity (seconds -> max rows). Keeps enough
# history for backtests without unbounded growth.
CANDLE_RETENTION = {
    60: 60 * 24 * 7,        # 1m  -> 7 days
    300: 300 * 288 * 30,    # 5m  -> 30 days
    900: 900 * 96 * 60,     # 15m -> 60 days
    3600: 3600 * 24 * 180,  # 1h  -> 180 days
    21600: 21600 * 4 * 365, # 6h  -> ~1 year
    86400: 86400 * 365 * 5, # 1d  -> 5 years
}
RECORD_RETENTION = 200_000  # max rows per non-OHLCV feed file


def _retention_rows(granularity: object) -> int:
    try:
        return CANDLE_RETENTION.get(int(granularity), 50_000)
    except (TypeError, ValueError):
        return 50_000


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


def inspect_cache(max_files: int = 10_000, freshness_seconds: float = 900.0,
                  now: Optional[float] = None, max_entries: Optional[int] = None) -> dict:
    """Return an observational cache report without resolving or modifying roots.

    Unlike :func:`ensure_root`, this function never creates directories and never
    performs a write probe.  It reports the root selected by the current process
    when one has already been resolved; otherwise it only examines the configured
    NAS path. Traversal is deliberately bounded for dashboard requests: both
    directory entries and regular files have independent limits. ``max_entries``
    defaults to ``max_files`` for backward-compatible bounded calls.
    """
    max_files = max(0, int(max_files))
    max_entries = max_files if max_entries is None else max(0, int(max_entries))
    configured = NAS_FEED_ROOT
    resolved = _RESOLVED_ROOT
    selected = resolved or configured
    report = {
        "source": "feed_cache",
        "configured_root": configured,
        "resolved_root": resolved,
        "selected_root": selected,
        "fallback": bool(resolved and os.path.abspath(resolved) != os.path.abspath(configured)),
        "readable": False,
        "status": "unknown",
        "truncation_reasons": [],
        "totals": {"files": 0, "bytes": 0, "truncated": False, "unreadable_entries": 0},
        "kinds": {},
    }
    try:
        os.stat(selected)
        if not os.path.isdir(selected):
            return report
        # Opening the directory is the useful read-access test.  Do not use a
        # write probe or os.makedirs here.
        with os.scandir(selected):
            pass
        report["readable"] = True
    except OSError:
        return report

    now = time.time() if now is None else now
    stack = [(selected, "", 0)]
    files_seen = 0
    entries_seen = 0
    max_depth = 12
    stop_traversal = False

    def truncate(reason: str) -> None:
        report["totals"]["truncated"] = True
        if reason not in report["truncation_reasons"]:
            report["truncation_reasons"].append(reason)

    while stack and not stop_traversal:
        directory, kind, depth = stack.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if entries_seen >= max_entries:
                        truncate("directory_entries_limit")
                        stop_traversal = True
                        break
                    entries_seen += 1
                    item_kind = kind or entry.name
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if depth < max_depth:
                                stack.append((entry.path, item_kind, depth + 1))
                            else:
                                # A bounded inspection must disclose that it
                                # deliberately omitted descendants.
                                truncate("max_depth")
                            continue
                        if not entry.is_file(follow_symlinks=False):
                            continue
                        # Enforce the file limit before a file contributes to
                        # totals. In particular, max_files=0 records none.
                        if files_seen >= max_files:
                            truncate("regular_files_limit")
                            stop_traversal = True
                            break
                        stat = entry.stat(follow_symlinks=False)
                    except OSError:
                        report["totals"]["unreadable_entries"] += 1
                        continue
                    files_seen += 1
                    detail = report["kinds"].setdefault(item_kind, {
                        "files": 0, "bytes": 0, "newest_mtime": None,
                        "age_sec": None, "freshness": "unknown",
                    })
                    detail["files"] += 1
                    detail["bytes"] += max(int(stat.st_size), 0)
                    newest = detail["newest_mtime"]
                    detail["newest_mtime"] = stat.st_mtime if newest is None else max(newest, stat.st_mtime)
                    report["totals"]["files"] += 1
                    report["totals"]["bytes"] += max(int(stat.st_size), 0)
                    if files_seen >= max_files:
                        truncate("regular_files_limit")
                        stop_traversal = True
                        break
        except OSError:
            continue

    for detail in report["kinds"].values():
        if detail["newest_mtime"] is not None:
            detail["age_sec"] = round(max(0.0, now - detail["newest_mtime"]), 1)
            detail["freshness"] = "fresh" if detail["age_sec"] <= freshness_seconds else "stale"
            detail["newest_mtime"] = round(detail["newest_mtime"], 3)
    if (report["fallback"] or report["totals"]["truncated"] or report["totals"]["unreadable_entries"]
            or any(detail["freshness"] == "stale" for detail in report["kinds"].values())):
        report["status"] = "warn"
    elif report["totals"]["files"]:
        report["status"] = "ok"
    return report


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
            _metrics["candle_saves"] += len(candles)
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
            _metrics["candle_hits"] += 1
        else:
            rows = []
    if not rows:
        path = _path(kind, symbol, f"{granularity}.parquet")
        with _lock:
            _metrics["candle_misses"] += 1
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
        with _lock:
            _metrics["record_saves"] += len(records)
        return len(records)
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("feed_cache save_records failed %s: %s", path, e)
        return 0


def save_records_durable(kind: str, name: str, records: Sequence[dict]) -> int:
    """Append records and fsync both the JSONL file and its directory.

    Unlike :func:`save_records`, this function deliberately propagates errors:
    callers use it for trade events that must not be silently dropped.
    """
    if not records:
        return 0
    path = _path(kind, f"{name}.jsonl")
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    payload = b"".join(
        (json.dumps(record, allow_nan=False, separators=(",", ":")) + "\n").encode("utf-8")
        for record in records
    )
    with _lock:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            view = memoryview(payload)
            while view:
                written = os.write(fd, view)
                if written <= 0:  # pragma: no cover - defensive OS contract guard
                    raise OSError("short durable journal write")
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        dir_fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        _metrics["record_saves"] += len(records)
    return len(records)


def load_records(kind: str, name: str, limit: Optional[int] = None) -> List[dict]:
    path = _path(kind, f"{name}.jsonl")
    if not os.path.exists(path):
        with _lock:
            _metrics["record_misses"] += 1
        return []
    try:
        rows = _load_jsonl(path)
        with _lock:
            if rows:
                _metrics["record_hits"] += 1
            else:
                _metrics["record_misses"] += 1
        if limit:
            rows = rows[-limit:]
        return rows
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("feed_cache load_records failed %s: %s", path, e)
        return []


# ── compaction / retention (E3) ─────────────────────────────────────────────

def compact(kind: Optional[str] = None, dry_run: bool = False) -> Dict[str, int]:
    """Enforce retention policy across feed-cache files.

    For candle parquet files, trims to the per-granularity max row count
    (``CANDLE_RETENTION``). For non-OHLCV ``.jsonl`` files, trims to the last
    ``RECORD_RETENTION`` rows. Returns a summary of rows removed per file.

    Run periodically (e.g. from the optimizer daemon) to bound disk growth.
    """
    root = _root()
    summary: Dict[str, int] = {}
    kinds = [kind] if kind else ["coinbase_candles", "onchain", "prediction_markets", "news"]
    for k in kinds:
        base = os.path.join(root, k)
        if not os.path.isdir(base):
            continue
        for dirpath, _, files in os.walk(base):
            for fn in files:
                full = os.path.join(dirpath, fn)
                try:
                    if fn.endswith(".parquet"):
                        if not _HAVE_PARQUET:
                            continue
                        import pandas as pd

                        df = pd.read_parquet(full)
                        keep = _retention_rows(os.path.basename(fn).split(".")[0])
                        if len(df) > keep:
                            trimmed = df.tail(keep).reset_index(drop=True)
                            removed = len(df) - len(trimmed)
                            if not dry_run:
                                trimmed.to_parquet(full, index=False)
                                # keep in-process cache consistent with disk
                                try:
                                    sym = os.path.basename(dirpath)
                                    gran = int(os.path.basename(fn).split(".")[0])
                                    _mem[_mem_key("coinbase_candles", sym, gran)] = (
                                        time.time(),
                                        [[float(x) for x in r] for r in trimmed.values.tolist()],
                                    )
                                except Exception:
                                    pass
                            summary[full] = removed
                    elif fn.endswith(".jsonl"):
                        rows = _load_jsonl(full)
                        if len(rows) > RECORD_RETENTION:
                            trimmed = rows[-RECORD_RETENTION:]
                            removed = len(rows) - len(trimmed)
                            if not dry_run:
                                _write_jsonl(full, trimmed)
                            summary[full] = removed
                except Exception as e:  # pragma: no cover - defensive
                    logger.warning("compact skipped %s: %s", full, e)
    return summary


def compact_all(dry_run: bool = False) -> Dict[str, int]:
    """Compact every feed kind."""
    return compact(kind=None, dry_run=dry_run)


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
