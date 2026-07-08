from __future__ import annotations
import time
import math
import logging
from .cb_client import CBClient

try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None


class _MiniSeries(list):
    @property
    def iloc(self):
        return self

    def copy(self):
        return _MiniSeries(self)

    def shift(self, periods: int = 1):
        if periods <= 0:
            return _MiniSeries(self)
        return _MiniSeries([None] * periods + list(self[:-periods] if periods < len(self) else []))

    def diff(self):
        if not self:
            return _MiniSeries([])
        out = [None]
        for i in range(1, len(self)):
            a = self[i]
            b = self[i - 1]
            out.append((a - b) if a is not None and b is not None else None)
        return _MiniSeries(out)

    def clip(self, lower=None, upper=None):
        out = []
        for v in self:
            if v is None:
                out.append(None)
                continue
            if lower is not None and v < lower:
                v = lower
            if upper is not None and v > upper:
                v = upper
            out.append(v)
        return _MiniSeries(out)

    def abs(self):
        return _MiniSeries([abs(v) if v is not None else None for v in self])

    def tail(self, n: int):
        return _MiniSeries(self[-n:])

    def max(self):
        vals = [v for v in self if v is not None]
        return max(vals) if vals else 0.0

    def min(self):
        vals = [v for v in self if v is not None]
        return min(vals) if vals else 0.0

    def mean(self):
        vals = [v for v in self if v is not None]
        return sum(vals) / len(vals) if vals else 0.0

    def rolling(self, window: int):
        return _MiniRolling(self, window)

    def ewm(self, alpha=0.5, adjust=False):
        return _MiniEWM(self, alpha=alpha)

    def __truediv__(self, other):
        return _MiniSeries([(v / other) if v is not None and other not in (0, None) else None for v in self])

    def __rtruediv__(self, other):
        return _MiniSeries([(other / v) if v not in (0, None) else None for v in self])

    def __mul__(self, other):
        return _MiniSeries([(v * other) if v is not None else None for v in self])

    def __add__(self, other):
        return _MiniSeries([(v + other) if v is not None else None for v in self])

    def __sub__(self, other):
        if isinstance(other, (list, _MiniSeries)):
            return _MiniSeries([(a - b) if a is not None and b is not None else None for a, b in zip(self, other)])
        return _MiniSeries([(v - other) if v is not None else None for v in self])


class _MiniRolling:
    def __init__(self, series, window: int):
        self.series = list(series)
        self.window = max(int(window), 1)

    def _window_vals(self, idx: int):
        start = max(0, idx - self.window + 1)
        return [v for v in self.series[start:idx + 1] if v is not None]

    def max(self):
        return _MiniSeries([max(self._window_vals(i)) if self._window_vals(i) else None for i in range(len(self.series))])

    def min(self):
        return _MiniSeries([min(self._window_vals(i)) if self._window_vals(i) else None for i in range(len(self.series))])

    def mean(self):
        vals = []
        for i in range(len(self.series)):
            w = self._window_vals(i)
            vals.append(sum(w) / len(w) if w else None)
        return _MiniSeries(vals)


class _MiniEWM:
    def __init__(self, series, alpha=0.5):
        self.series = list(series)
        self.alpha = alpha

    def mean(self):
        out = []
        ema = None
        for v in self.series:
            if v is None:
                out.append(ema)  # Keep last valid EMA instead of None
                continue
            ema = v if ema is None else (self.alpha * v) + ((1 - self.alpha) * ema)
            out.append(ema)
        return _MiniSeries(out)


class _MiniFrame:
    def __init__(self, rows=None, columns=None):
        self._rows = [dict(r) for r in (rows or [])]
        self._columns = list(columns or (self._rows[0].keys() if self._rows else []))

    def __len__(self):
        return len(self._rows)

    def __getitem__(self, key):
        if isinstance(key, str):
            return _MiniSeries([row.get(key) for row in self._rows])
        raise TypeError("MiniFrame only supports column access")

    @property
    def iloc(self):
        class _ILoc:
            def __init__(self, rows): self.rows = rows
            def __getitem__(self, idx):
                if isinstance(idx, slice):
                    return _MiniFrame(self.rows[idx])
                return self.rows[idx]
        return _ILoc(self._rows)

    def tail(self, n: int):
        return _MiniFrame(self._rows[-n:])

    def copy(self):
        return _MiniFrame(self._rows[:])

    def drop_duplicates(self, *args, **kwargs):
        return self

    def sort_values(self, *args, **kwargs):
        return self

    def set_index(self, *args, **kwargs):
        return self


def _frame(rows=None, columns=None):
    if pd is not None:
        return pd.DataFrame(rows, columns=columns)
    return _MiniFrame(rows, columns=columns)


def _series(values):
    if pd is not None:
        return pd.Series(values)
    return _MiniSeries(values)

log = logging.getLogger(__name__)

_SEC_PER = {
    "ONE_MINUTE":60, "FIVE_MINUTE":300, "FIFTEEN_MINUTE":900, "THIRTY_MINUTE":1800,
    "ONE_HOUR":3600, "TWO_HOUR":7200, "FOUR_HOUR":14400, "SIX_HOUR":21600, "ONE_DAY":86400
}

# In-memory cache: {(product_id, granularity, lookback_days): (expiry_ts, DataFrame)}
_CACHE: dict = {}
_CACHE_TTL: int = 300  # 5 minutes default

def _cache_key(product_id: str, granularity: str, lookback_days: int) -> tuple:
    return (product_id, granularity, lookback_days)


def invalidate_cache(product_id: str | None = None) -> None:
    """Invalidate cached candles for a product, or all products."""
    global _CACHE
    if product_id is None:
        _CACHE.clear()
    else:
        _CACHE = {k: v for k, v in _CACHE.items() if k[0] != product_id}


def fetch_candles_df(
    client: CBClient,
    product_id: str,
    lookback_days: int = 240,
    granularity: str = "ONE_DAY",
    *,
    chunk_bars: int = 200,        # smaller than 300 to reduce payload
    max_retries: int = 6,
    backoff_base_s: float = 1.5,
    backoff_cap_s: float = 30.0,
    cache_ttl_s: int = _CACHE_TTL,
) -> object:
    # Check cache first
    key = _cache_key(product_id, granularity, lookback_days)
    now = time.time()
    cached = _CACHE.get(key)
    if cached and (now - cached[0]) < cache_ttl_s:
        log.debug(f"[candles] cache hit {product_id} {granularity} {lookback_days}d")
        return cached[1].copy()

    end = int(now)
    start = end - int(lookback_days) * 86400
    spb = _SEC_PER[granularity]

    frames = []
    cursor = start
    while cursor < end:
        chunk_end = min(end, cursor + chunk_bars * spb)

        raw = None
        for att in range(max_retries):
            try:
                raw = client.public_candles(
                    product_id,
                    start_unix=cursor,
                    end_unix=chunk_end,
                    granularity=granularity,
                    limit=chunk_bars
                )
                break  # success
            except Exception as e:
                # exponential backoff with cap
                wait = min(backoff_cap_s, backoff_base_s * (2 ** att))
                time.sleep(wait)
                if att == max_retries - 1:
                    log.warning(f"[candles] skip {product_id} {cursor}->{chunk_end} after retries: {e}")
        if raw:
            rows = []
            payload = raw.get("candles", raw if isinstance(raw, list) else [])
            for c in payload:
                if isinstance(c, dict):
                    ts = int(c.get("start", c.get("start_time", 0)))
                    rows.append([ts, float(c.get("open", 0)), float(c.get("high", 0)),
                                 float(c.get("low", 0)), float(c.get("close", 0)), float(c.get("volume", 0))])
                else:
                    # tuple/list form: [ts, low, high, open, close, volume] → normalize to o,h,l,c
                    ts, lo, hi, op, cl, vol = c
                    rows.append([int(ts), float(op), float(hi), float(lo), float(cl), float(vol)])
            if rows:
                df = _frame(rows, columns=["ts","open","high","low","close","volume"])
                frames.append(df)

        cursor = chunk_end

    if not frames:
        empty = _frame([], columns=["open","high","low","close","volume"])
        _CACHE[key] = (now, empty)
        return empty

    if pd is not None:
        out = pd.concat(frames, ignore_index=True).drop_duplicates("ts").sort_values("ts")
        out["datetime"] = pd.to_datetime(out["ts"], unit="s", utc=True)
        out.set_index("datetime", inplace=True)
        result = out[["open","high","low","close","volume"]]
    else:
        rows2 = []
        for fr in frames:
            rows2.extend(getattr(fr, "_rows", []))
        result = _frame(rows2, columns=["ts","open","high","low","close","volume"])

    # Store in cache
    _CACHE[key] = (now, result)
    log.debug(f"[candles] cached {product_id} {granularity} {lookback_days}d ({len(result)} rows)")

    return result.copy()

def compute_atr(df, period: int = 14):
    hi, lo, cl = df["high"], df["low"], df["close"]
    prev_close = cl.shift(1)
    tr_vals = []
    for h, l, pc in zip(hi, lo, prev_close):
        if h is None or l is None:
            tr_vals.append(None)
            continue
        a = h - l
        b = abs(h - pc) if pc is not None else a
        c = abs(l - pc) if pc is not None else a
        tr_vals.append(max(a, b, c))
    return _series(tr_vals).ewm(alpha=1 / period, adjust=False).mean()

def rolling_high(df, lookback: int = 20):
    return df["high"].rolling(lookback).max()

def rolling_low(df, lookback: int = 20):
    return df["low"].rolling(lookback).min()

def rsi(series, period: int = 14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = _series([(-v if v is not None and v < 0 else 0.0) if v is not None else None for v in delta])
    roll_up = up.ewm(alpha=1 / period, adjust=False).mean()
    roll_down = down.ewm(alpha=1 / period, adjust=False).mean()
    rs = []
    for u, d in zip(roll_up, roll_down):
        if u is None or d is None:
            rs.append(None)
        else:
            rs.append(u / (d + 1e-12))
    return _series([100 - (100 / (1 + v)) if v is not None else None for v in rs])
