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
    """Bucket realized vol into LOW/NORMAL/HIGH/EXTREME by per-candle return stdev.

    Phase 6c re-tune: the OLD EXTREME cutoff was 2.2% stdev, which in normal BTC
    conditions (2-4% per-candle on 4h/daily) stood the agent down almost always.
    Raised so only genuine vol expansion (>=4.0%) forces a stand-down; HIGH covers
    the normal elevated-crypto band (2.2-4.0%) where we still trade but size down.
    """
    sd = _realized_vol(candles, n)
    if sd <= 0.0:
        return {"bucket": "UNKNOWN", "sd": 0.0, "stand_down": False}
    if sd < 0.010:
        b = "LOW"
    elif sd < 0.022:
        b = "NORMAL"
    elif sd < 0.040:   # Phase 6c: raised from 0.022 -> 0.040
        b = "HIGH"
    else:
        b = "EXTREME"
    return {"bucket": b, "sd": round(sd, 5), "stand_down": b == "EXTREME"}


# Phase 6b: reliability weights per timeframe. The daily trend is the most
# trustworthy regime signal; the 1h is the noisiest. Used to break a split vote
# instead of hard-standing-down on MIXED (which kept the agent at 0 trades in
# normal crypto conditions where 1h/4h/1d routinely disagree).
_TF_WEIGHT = {86400: 3.0, 14400: 2.0, 3600: 1.0}

# "Hard" regimes that must be respected even on a split vote — if ANY reliable
# timeframe (4h/1d) screams CRISIS, we still stand down (real event/vol risk).
_HARD_REGIMES = {"CRISIS"}


def _weighted_regime(votes_detail: dict, timeframes: list) -> tuple[str, float]:
    """Return (regime, agreement 0..1) from a per-tf vote map, weighting by
    timeframe reliability. agreement = weighted-share of the winning regime, so
    callers can SCALE SIZE DOWN on disagreement rather than fully standing down."""
    wsum = {r: 0.0 for r in set(list(votes_detail.values()) + ["TREND_UP",
              "TREND_DOWN", "RANGE", "CRISIS", "UNKNOWN"])}
    total = 0.0
    for tf, r in votes_detail.items():
        w = _TF_WEIGHT.get(tf, 1.0)
        wsum[r] = wsum.get(r, 0.0) + w
        total += w
    # winning regime = highest weighted vote, ignoring UNKNOWN unless it dominates
    ranked = sorted(((r, w) for r, w in wsum.items() if r != "UNKNOWN"),
                    key=lambda x: -x[1])
    if not ranked:
        return "MIXED", 0.0
    best, best_w = ranked[0]
    agreement = (best_w / total) if total else 0.0
    return best, round(agreement, 3)


def multi_timeframe_regime(product: str, client,
                           timeframes: Optional[list] = None) -> dict:
    """Phase 6b: agree regime across timeframes with a WEIGHTED FALLBACK.

    OLD behavior: required >=2/3 raw agreement, else hard MIXED -> entry gate
    fully closed (agent never traded in normal split conditions).
    NEW behavior: weight votes by reliability (1d>4h>1h). Returns the weighted
    winning regime PLUS an `agreement` score (0..1). Callers scale size DOWN on
    low agreement (fakeout protection preserved) instead of standing down
    entirely. Hard regimes (CRISIS on any 4h/1d tf) still force a stand-down.
    """
    if timeframes is None:
        timeframes = [3600, 14400, 86400]
    votes = {}
    detail = {}
    for tf in timeframes:
        r = _regime_on_tf(product, client, tf)
        detail[tf] = r
        votes[r] = votes.get(r, 0) + 1
    # Hard stand-down: a reliable timeframe (>=4h) flagging CRISIS is real risk.
    hard_crisis = any(detail.get(tf) == "CRISIS" for tf in timeframes
                      if _TF_WEIGHT.get(tf, 1.0) >= 2.0)
    if hard_crisis:
        return {
            "regime": "CRISIS", "agree": False, "agreement": 0.0,
            "votes": votes, "detail": detail, "n_tf": len(timeframes),
            "weighted_fallback": False, "reason": "hard_crisis_on_reliable_tf",
        }
    best, agreement = _weighted_regime(detail, timeframes)
    # Treat UNKNOWN-dominated or truly empty as MIXED (stand down).
    if best in ("UNKNOWN",) or agreement <= 0.0:
        best = "MIXED"
    return {
        "regime": best,
        "agree": agreement >= 0.66,   # 2/3 weighted = "clean" agreement
        "agreement": agreement,        # 0..1, used by loop to scale size
        "votes": votes,
        "detail": detail,
        "n_tf": len(timeframes),
        "weighted_fallback": agreement < 0.66,
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
