from __future__ import annotations
import time, math
import pandas as pd
from .cb_client import CBClient

def fetch_candles_df(client: CBClient, product_id: str, lookback_days: int = 240, granularity: str = "ONE_DAY") -> pd.DataFrame:
    """
    Pulls candles in chunks (max ~300 per call). Returns DataFrame with timestamp, open, high, low, close, volume.
    Note: Advanced Trade public candles require UNIX start/end and granularity enums.
    """
    end = int(time.time())
    start = end - lookback_days * 86400
    step = 300  # max candles per request
    # choose seconds per bar for stepping purposes
    sec_per_bar = {"ONE_MINUTE":60,"FIVE_MINUTE":300,"FIFTEEN_MINUTE":900,"THIRTY_MINUTE":1800,
                   "ONE_HOUR":3600,"TWO_HOUR":7200,"FOUR_HOUR":14400,"SIX_HOUR":21600,"ONE_DAY":86400}[granularity]
    frames = []
    cursor = start
    while cursor < end:
        chunk_end = min(end, cursor + step*sec_per_bar)
        raw = client.public_candles(product_id, start_unix=cursor, end_unix=chunk_end, granularity=granularity, limit=300)
        # Expect list of [start, low, high, open, close, volume] or dict; normalize
        rows = []
        for c in raw.get("candles", raw if isinstance(raw, list) else []):
            # Candle may be dict or list
            if isinstance(c, dict):
                ts = int(c.get("start", c.get("start_time", 0)))
                rows.append([ts, float(c.get("open", 0)), float(c.get("high", 0)), float(c.get("low", 0)), float(c.get("close", 0)), float(c.get("volume", 0))])
            else:
                # best-effort: [start, low, high, open, close, volume]
                ts, lo, hi, op, cl, vol = c
                rows.append([int(ts), float(op), float(hi), float(lo), float(cl), float(vol)])
        if rows:
            import pandas as pd
            df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
            frames.append(df)
        cursor = chunk_end
    if not frames:
        return pd.DataFrame(columns=["ts","open","high","low","close","volume"])
    out = pd.concat(frames, ignore_index=True).drop_duplicates("ts").sort_values("ts")
    out["datetime"] = pd.to_datetime(out["ts"], unit="s", utc=True)
    out.set_index("datetime", inplace=True)
    return out[["open","high","low","close","volume"]]
