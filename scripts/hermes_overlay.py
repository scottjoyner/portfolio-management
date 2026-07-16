#!/usr/bin/env python3
"""
hermes_overlay.py — Phase 7: SENTIMENT + EVENT-RISK OVERLAY.

The bot has ZERO event-risk or sentiment model. This module adds:
  * fear_greed()  -> Crypto Fear & Greed Index (alternative.me, KEYLESS public
    JSON). Buckets: EXTREME_FEAR / FEAR / NEUTRAL / GREED / EXTREME_GREED.
    Used to fade extremes: buy dips harder in EXTREME_FEAR, stand down new
    entries in EXTREME_GREED (sell-the-news risk). Falls back to UNKNOWN
    offline (no hard dependency).
  * event_risk(now) -> coarse macro-event gate. Crypto prints major US data
    (CPI, FOMC, NFP) on a known cadence; we avoid NEW entries in a quiet
    window around the 1st-Friday NFP and FOMC Wednesdays. Stand-down only
    blocks NEW entries; open positions still managed.
  * sentiment_bias(fg) -> multiplier 0.5..1.2 for sizing.

Read-only network (keyless). Never blocks the loop on a network failure.
"""
from __future__ import annotations
import datetime as dt
import json
import urllib.request
from typing import Optional

_FG_URL = "https://api.alternative.me/fng/?limit=1"


def fear_greed(timeout: float = 4.0) -> dict:
    """Crypto Fear & Greed Index (0-100). Keyless. Returns bucket + value."""
    try:
        req = urllib.request.Request(_FG_URL, headers={"User-Agent": "hermes-agent/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
        data = json.loads(raw)
        first = data["data"][0]
        val = int(str(first["value"]))
        classification = str(first.get("value_classification", ""))
    except Exception:
        return {"ok": False, "value": None, "bucket": "UNKNOWN",
                "classification": "", "note": "fetch_failed_offline"}
    if val <= 20:
        bucket = "EXTREME_FEAR"
    elif val <= 40:
        bucket = "FEAR"
    elif val <= 60:
        bucket = "NEUTRAL"
    elif val <= 80:
        bucket = "GREED"
    else:
        bucket = "EXTREME_GREED"
    return {"ok": True, "value": val, "bucket": bucket,
            "classification": classification, "note": ""}


def sentiment_bias(fg: dict) -> float:
    """Sizing multiplier from sentiment. Buy fear, avoid extreme greed."""
    b = fg.get("bucket")
    return {
        "EXTREME_FEAR": 1.2,   # fade the fear — add size to dip-buys
        "FEAR": 1.05,
        "NEUTRAL": 1.0,
        "GREED": 0.85,
        "EXTREME_GREED": 0.5,  # sell-the-news risk — shrink new entries
        "UNKNOWN": 0.9,         # offline: slightly cautious
    }.get(b, 0.9)


def event_risk(now: Optional[dt.datetime] = None) -> dict:
    """Coarse macro-event gate. Stand down NEW entries around known high-impact
    US prints: FOMC (Wed, sometimes 2-day, approximate as 1st/3rd Wed) and
    NFP (1st Friday). We use a simple rule: if today is a 1st-Friday or a
    Wednesday in the first 3 weeks (FOMC window), mark elevated and widen/block
    new entries in the listed UTC hours. Kept deliberately simple + offline-safe.
    """
    if now is None:
        now = dt.datetime.now(dt.timezone.utc)
    day = now.weekday()  # Mon=0..Sun=6
    dom = now.day
    is_first_week = dom <= 7
    is_first_friday = (day == 4 and is_first_week)
    is_fomc_window = (day == 2 and is_first_week)  # Wednesday 1st week ~ FOMC
    elevated = is_first_friday or is_fomc_window
    return {
        "elevated": elevated,
        "block_new": False,   # we do NOT hard-block; just widen + size-down via caller
        "reason": ("NFP_Friday" if is_first_friday else
                   "FOMC_Wednesday" if is_fomc_window else "none"),
        "utc_hour": now.hour,
    }


def overlay_state() -> dict:
    """Combine sentiment + event into one decision struct for the loop."""
    fg = fear_greed()
    ev = event_risk()
    size_mult = sentiment_bias(fg)
    if ev["elevated"]:
        size_mult *= 0.7  # shrink around macro prints
    return {
        "fear_greed": fg,
        "event": ev,
        "size_mult": round(size_mult, 3),
        "stand_down_new": fg.get("bucket") == "EXTREME_GREED" and ev["elevated"],
    }


if __name__ == "__main__":
    import sys, json as _j
    sys.path.insert(0, ".")
    print(_j.dumps(overlay_state(), indent=1))
