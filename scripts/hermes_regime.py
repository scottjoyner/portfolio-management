#!/usr/bin/env python3
"""
hermes_regime.py — BTC-as-oracle market-regime classifier for the Hermes agent.

The bot trades every pair in isolation with no cross-asset context. BTC-USD drives
~all alts, so classifying the BTC regime and GATING alt signals on it is the agent's
primary edge. This is a transparent, rule-based classifier (no LLM dependency) so it
is cheap, debuggable, and paper-safe.

Regime output drives hermes_agent_loop.py:
  TREND_UP   -> take LONG/BUY momentum on alts
  TREND_DOWN  -> take SHORT/SELL momentum on alts
  RANGE       -> fade extremes (mean-reversion), either side
  CRISIS      -> STAND DOWN (vol spike / event risk) — no trades

Read-only: pulls candles via CBClient, returns a dict. No state, no side effects.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _candles(product_id: str, granularity: str, hours: int) -> list[dict]:
    from coinbase.src.cb_client import CBClient
    c = CBClient(dry_run_cli=True)
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(hours=hours)
    r = c._cli_json(
        "products", "candles", product_id, f"granularity={granularity}",
        f"start={start.isoformat()}", f"end={end.isoformat()}",
    )
    if isinstance(r, dict) and "candles" in r:
        return r["candles"]
    return []


def _returns(candles: list[dict]) -> list[float]:
    closes = [float(x["close"]) for x in candles if x.get("close")]
    out = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            out.append((closes[i] - closes[i - 1]) / closes[i - 1])
    return out


def classify_btc(hours: int = 48) -> dict:
    """Return BTC regime + supporting stats. Paper-safe, read-only."""
    candles = _candles("BTC-USD", "4h", hours)
    if len(candles) < 4:
        return {"regime": "UNKNOWN", "reason": "insufficient candles",
                "trend": 0.0, "vol": 0.0, "n": len(candles)}
    closes = [float(x["close"]) for x in candles if x.get("close")]
    rets = _returns(candles)
    n = len(closes)
    first, last = closes[0], closes[-1]
    trend = (last - first) / first if first > 0 else 0.0
    # realized vol = stdev of returns (per-bar), annualized-ish not needed
    if rets:
        mean = sum(rets) / len(rets)
        var = sum((x - mean) ** 2 for x in rets) / len(rets)
        vol = var ** 0.5
    else:
        vol = 0.0
    # range-boundness: how close last close is to the window midpoint
    mid = (min(closes) + max(closes)) / 2.0
    spread = (max(closes) - min(closes))
    midpoint_dist = abs(last - mid) / spread if spread > 0 else 1.0

    TREND_THRESH = 0.015   # >1.5% over 48h = trending
    VOL_THRESH = 0.012      # per-4h-bar vol above this = crisis/elevated
    RANGE_MID = 0.25        # last within 25% of midpoint = range-bound

    if vol > VOL_THRESH:
        regime = "CRISIS"
        reason = f"elevated vol {vol:.4f} > {VOL_THRESH}"
    elif abs(trend) > TREND_THRESH:
        regime = "TREND_UP" if trend > 0 else "TREND_DOWN"
        reason = f"trend {trend:+.4f} beyond {TREND_THRESH}"
    elif midpoint_dist < RANGE_MID:
        regime = "RANGE"
        reason = f"midpoint_dist {midpoint_dist:.3f} < {RANGE_MID}"
    else:
        regime = "RANGE"  # default to range (conservative) if ambiguous
        reason = f"ambiguous (trend={trend:+.4f}, mid={midpoint_dist:.3f}) -> RANGE"

    return {"regime": regime, "reason": reason, "trend": round(trend, 5),
            "vol": round(vol, 5), "midpoint_dist": round(midpoint_dist, 4),
            "n": n, "last": round(last, 2)}


def compatible_side(regime: str, momentum_side: str) -> bool:
    """Is an alt's momentum signal allowed under the current BTC regime?"""
    if regime == "CRISIS":
        return False
    if regime == "TREND_UP":
        return momentum_side == "BUY"
    if regime == "TREND_DOWN":
        return momentum_side == "SELL"
    # RANGE: either side allowed (we'll fade extremes in the loop)
    return True


if __name__ == "__main__":
    import json
    print(json.dumps(classify_btc(), indent=2))
