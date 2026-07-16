#!/usr/bin/env python3
"""
hermes_mtf.py — Phase 6: MULTI-TIMEFRAME + VOL-REGIME confirmation.

The single 4h BTC regime in hermes_regime can fake out on one timeframe. This
module adds:
  * multi_timeframe_regime(product, client): agree BTC (or the asset) across
    1h + 4h + 1d. Returns the dominant regime only if >=2 of 3 agree, else
    "MIXED" (stand down — fakeout guard).
  * vol_regime(product, client, tf): realized-vol bucket -> "LOW"/"NORMAL"/
    "HIGH"/"EXTREME". In HIGH/EXTREME vol we widen thresholds (trends still
    tradable but dips are noisier) and in EXTREME we stand down new entries.
  * conviction(mom, vol_bucket): scale 0..1 combining signal strength + vol
    context. Feeds size_for later.

Read-only (candles only). No external calls.
"""
from __future__ import annotations
import datetime as dt
from typing import Optional


# Map seconds -> Coinbase granularity string + window hours for enough bars.
_TF_MAP = {3600: ("1h", 72), 14400: ("4h", 120), 86400: ("1d", 360)}


def _candles(client, product: str, granularity: int) -> list:
    from scripts.hermes_agent_loop import _candles as _c
    gstr, hours = _TF_MAP.get(granularity, ("1h", 72))
    return _c(client, product, hours, granularity=gstr)


def _regime_on_tf(product: str, client, granularity: int) -> str:
    """Classify regime for `product` on a specific candle granularity (seconds)."""
    try:
        candles = _candles(client, product, granularity)
        from scripts.hermes_regime import classify_candles
        return classify_candles(candles)["regime"]
    except Exception:
        return "UNKNOWN"


def _realized_vol(candles: list, n: int = 20) -> float:
    """Annualized-ish realized vol from close-to-close log returns (last n)."""
    closes = [float(c.get("close", 0)) for c in candles[-(n + 1):] if c.get("close")]
    if len(closes) < 3:
        return 0.0
    rets = [abs((closes[i] - closes[i - 1]) / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1]]
    if not rets:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / len(rets)
    sd = var ** 0.5
    return sd  # per-candle stdev of returns (~proxy for vol regime)


def vol_regime(candles: list, n: int = 20) -> dict:
    """Bucket realized vol into LOW/NORMAL/HIGH/EXTREME by per-candle return stdev."""
    sd = _realized_vol(candles, n)
    if sd <= 0.0:
        return {"bucket": "UNKNOWN", "sd": 0.0, "stand_down": False}
    if sd < 0.006:
        b = "LOW"
    elif sd < 0.012:
        b = "NORMAL"
    elif sd < 0.022:
        b = "HIGH"
    else:
        b = "EXTREME"
    return {"bucket": b, "sd": round(sd, 5), "stand_down": b == "EXTREME"}


def multi_timeframe_regime(product: str, client,
                           timeframes: Optional[list] = None) -> dict:
    """Agree regime across timeframes. Default 1h(3600)+4h(14400)+1d(86400).
    Returns dominant regime only if >=2/3 agree, else MIXED (fakeout guard)."""
    if timeframes is None:
        timeframes = [3600, 14400, 86400]
    votes = {}
    detail = {}
    for tf in timeframes:
        r = _regime_on_tf(product, client, tf)
        detail[tf] = r
        votes[r] = votes.get(r, 0) + 1
    best = max(votes, key=lambda k: votes[k])
    best_n = votes[best]
    agree = best_n >= 2  # majority (or unanimous)
    return {
        "regime": best if agree else "MIXED",
        "agree": agree,
        "votes": votes,
        "detail": detail,
        "n_tf": len(timeframes),
    }


def conviction(mom: float, vol_bucket: str) -> float:
    """0..1 signal quality. Strong momentum + NORMAL/LOW vol = high conviction.
    HIGH vol discounts (noisier); EXTREME forces ~0 (stand down)."""
    base = min(1.0, abs(mom) / 0.02)
    vol_mult = {
        "LOW": 1.05, "NORMAL": 1.0, "HIGH": 0.7, "EXTREME": 0.0, "UNKNOWN": 0.85,
    }.get(vol_bucket, 0.85)
    return round(min(1.0, base * vol_mult), 3)


if __name__ == "__main__":
    import sys, json
    sys.path.insert(0, ".")
    from coinbase.src.cb_client import CBClient
    from scripts.hermes_regime import classify_btc
    c = CBClient(dry_run_cli=True)
    mtf = multi_timeframe_regime("BTC-USD", c)
    print("MTF BTC:", json.dumps(mtf, indent=1))
    candles = _candles(c, "BTC-USD", 14400)
    print("VOL BTC 4h:", json.dumps(vol_regime(candles), indent=1))
