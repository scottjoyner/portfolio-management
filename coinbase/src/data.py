from __future__ import annotations
import time
import math
import pandas as pd
from .cb_client import CBClient

_SEC_PER = {
    "ONE_MINUTE":60, "FIVE_MINUTE":300, "FIFTEEN_MINUTE":900, "THIRTY_MINUTE":1800,
    "ONE_HOUR":3600, "TWO_HOUR":7200, "FOUR_HOUR":14400, "SIX_HOUR":21600, "ONE_DAY":86400
}

def fetch_candles_df(
    client: CBClient,
    product_id: str,
    lookback_days: int = 240,
    granularity: str = "ONE_DAY",
    *,
    chunk_bars: int = 200,        # smaller than 300 to reduce payload
    max_retries: int = 6,
    backoff_base_s: float = 1.5,
    backoff_cap_s: float = 30.0
) -> pd.DataFrame:
    end = int(time.time())
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
                    # give up on this window, move on so the run can continue
                    print(f"[candles] skip {product_id} {cursor}->{chunk_end} after retries: {e}")
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
                df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
                frames.append(df)

        cursor = chunk_end

    if not frames:
        return pd.DataFrame(columns=["open","high","low","close","volume"])

    out = pd.concat(frames, ignore_index=True).drop_duplicates("ts").sort_values("ts")
    out["datetime"] = pd.to_datetime(out["ts"], unit="s", utc=True)
    out.set_index("datetime", inplace=True)
    return out[["open","high","low","close","volume"]]

def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    hi, lo, cl = df["high"], df["low"], df["close"]
    prev_close = cl.shift(1)
    tr = pd.concat([hi - lo, (hi - prev_close).abs(), (lo - prev_close).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    return atr

def rolling_high(df: pd.DataFrame, lookback: int = 20) -> pd.Series:
    return df["high"].rolling(lookback).max()

def rolling_low(df: pd.DataFrame, lookback: int = 20) -> pd.Series:
    return df["low"].rolling(lookback).min()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-12)
    return 100 - (100 / (1 + rs))
