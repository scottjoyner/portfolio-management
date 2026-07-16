#!/usr/bin/env python3
"""
hermes_news.py — Phase 14: NEWS SENTIMENT ANALYSIS (the bot has ZERO news model).

Pulls crypto headlines from a keyless Google News RSS feed, scores each with a
compact finance lexicon (bullish/bearish term weights), and returns an aggregate
sentiment in [-1, +1] plus a size_mult + stand_down flag (panic/ euphoria = stand
down new entries, like the Fear&Greed overlay in Phase 7 but from NEWS flow).

Offline-safe: if the fetch fails, returns UNKNOWN and does NOT block trading
(news is a soft overlay, never a hard kill — the drawdown circuit is the hard stop).

Read-only network (RSS GET). No state writes.
"""
from __future__ import annotations

import datetime as dt
import re
import urllib.parse
import urllib.request
from typing import Optional

_RSS = ("https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en")
_QUERIES = ["cryptocurrency bitcoin", "ethereum crypto market", "crypto regulation sec"]

# Compact finance sentiment lexicon (term -> weight). Negative = bearish.
_BULL = {
    "surge": 1.0, "rally": 1.0, "soar": 1.2, "gain": 0.6, "gains": 0.6,
    "jump": 0.7, "rise": 0.5, "rises": 0.5, "high": 0.4, "record": 0.8,
    "boost": 0.6, "optimism": 0.8, "bullish": 1.0, "approval": 0.7,
    "etf": 0.4, "inflow": 0.6, "adoption": 0.7, "breakout": 0.7,
    "recovery": 0.6, "up": 0.3, "ticks up": 0.5, "climbs": 0.5,
}
_BEAR = {
    "crash": -1.2, "plunge": -1.1, "slump": -0.9, "drop": -0.6, "drops": -0.6,
    "fall": -0.6, "falls": -0.6, "tumble": -1.0, "sink": -0.8, "sinks": -0.8,
    "loss": -0.5, "losses": -0.5, "fear": -0.7, "selloff": -1.0, "sell-off": -1.0,
    "bearish": -1.0, "ban": -0.8, "banning": -0.9, "lawsuit": -0.6, "probe": -0.6,
    "crackdown": -0.9, "warning": -0.5, "risk": -0.3, "default": -1.0,
    "fraud": -1.0, "hack": -0.9, "collapse": -1.2, "low": -0.4, "weak": -0.5,
    "down": -0.3, "tanks": -1.1, "slashes": -0.7, "tension": -0.4,
    "inflation": -0.2, "rate hike": -0.6, "recession": -0.9,
}


def _score_text(text: str) -> tuple[float, int]:
    t = text.lower()
    score = 0.0
    hits = 0
    for term, w in _BULL.items():
        if term in t:
            score += w
            hits += 1
    for term, w in _BEAR.items():
        if term in t:
            score -= abs(w)
            hits += 1
    return score, hits


def fetch_headlines(queries: Optional[list] = None, timeout: int = 12,
                    max_each: int = 12) -> list[str]:
    """Return de-duplicated recent crypto headlines (keyless RSS)."""
    qs = queries or _QUERIES
    out: list[str] = []
    seen: set[str] = set()
    for q in qs:
        url = _RSS.format(q=urllib.parse.quote_plus(q))
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            raw = urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", "ignore")
            titles = re.findall(r"<title>(.*?)</title>", raw)
            for t in titles[1:max_each + 1]:  # skip the "Google News" channel title
                t = t.strip()
                if t and t not in seen:
                    seen.add(t)
                    out.append(t)
        except Exception:
            continue
    return out


def news_sentiment(queries: Optional[list] = None, timeout: int = 12) -> dict:
    """Aggregate crypto news sentiment. Returns a ready-to-use overlay dict
    compatible with the regime gate: {ok, score, bucket, size_mult,
    stand_down_new, n_headlines, samples[]}."""
    try:
        headlines = fetch_headlines(queries, timeout)
    except Exception:
        headlines = []
    if not headlines:
        return {"ok": False, "score": 0.0, "bucket": "UNKNOWN",
                "size_mult": 1.0, "stand_down_new": False,
                "n_headlines": 0, "samples": [],
                "reason": "fetch_unavailable"}
    total = 0.0
    scored = 0
    samples = []
    for h in headlines:
        s, hits = _score_text(h)
        if hits:
            total += s
            scored += 1
            if abs(s) >= 0.8:  # only surface strong ones
                samples.append((round(s, 2), h[:80]))
    # normalize to [-1, 1]: divide by a soft ceiling, clamp
    norm = (total / max(scored, 1)) / 2.0 if scored else 0.0
    norm = max(-1.0, min(1.0, norm))
    # buckets
    if norm >= 0.25:
        bucket, size_mult, stand = "BULLISH", 1.05, False
    elif norm <= -0.25:
        bucket, size_mult, stand = "BEARISH", 0.95, False
    elif norm <= -0.55:
        # extreme panic: stand down new entries (soft, not a hard kill)
        bucket, size_mult, stand = "PANIC", 0.8, True
    else:
        bucket, size_mult, stand = "NEUTRAL", 1.0, False
    return {"ok": True, "score": round(norm, 3), "bucket": bucket,
            "size_mult": size_mult, "stand_down_new": stand,
            "n_headlines": len(headlines), "scored": scored,
            "samples": samples[:5]}


if __name__ == "__main__":
    import sys as _sys
    from pathlib import Path as _P
    _sys.path.insert(0, str(_P(__file__).resolve().parent.parent))
    ns = news_sentiment()
    print("=== Phase 14 news sentiment ===")
    print(f"  bucket={ns['bucket']} score={ns['score']} "
          f"size_mult={ns['size_mult']} stand_down={ns['stand_down_new']}")
    print(f"  headlines={ns['n_headlines']} scored={ns.get('scored')}")
    for s, h in ns.get("samples", []):
        print(f"   {s:+.1f}  {h}")
