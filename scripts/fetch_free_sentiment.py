#!/usr/bin/env python3
"""Collect free, keyless market sentiment aligned to the cleaned candle series.

WHY ALTERNATIVE.ME. The brief was free endpoints only, and this is the one that
actually satisfies it:

  * no API key, no account, no quota;
  * `limit=0` returns the full index history -- 3167 days from 2018-02-01, far
    overlapping the candle window;
  * it is a published composite (volatility, momentum/volume, social media,
    search trends, dominance) rather than a price return, so it carries
    information the deterministic ensemble does not already model.

The other candidates were checked and rejected for the record:

  * Reddit public JSON returns 403 from this host, and has no history regardless.
  * Google News RSS 302s and offers no history.
  * CryptoPanic and LunarCrush need keys and have free tiers too small to
    backtest against.
  * The 12,674 cached headlines in data/feed_cache/news are real but cover only
    four days -- 2550 of them land on a single date -- so they cannot score a
    2026-07-02..2026-10-06 series. That cache is useful for forward collection,
    not for history.

HONEST CAVEAT. The index is not independent of price: momentum and volume are two
of its six inputs. It is less price-derived than asking a language model to read
candles -- which is the failure mode observed live, where a local model given ten
hourly closes returned "steady uptrend, sustained buying momentum", i.e. the
momentum term restated as sentiment. But a positive result here would not prove
the signal is exogenous, and the harness's random control exists to catch exactly
that kind of self-deception.

DAILY, NOT INTRADAY. The index publishes once per day at 00:00 UTC. A reading
cannot honestly claim a 60-minute horizon, so it is emitted with a 1440-minute
horizon and the harness must be run with --horizon 24. Aligning a daily index to
an hourly claim would be the single easiest way to manufacture a false result.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

FEAR_GREED_URL = "https://api.alternative.me/fng/?limit={limit}&format=json"
# The index is a daily composite, so it is not evidence about a single hour.
# Confidence is held well below 1: it is one published number per day, and the
# blend treats confidence as an attenuation factor, so an inflated value here
# would silently weight the signal more than its evidence supports.
DEFAULT_CONFIDENCE = 0.35
# Below this the reading is "Extreme Fear"/"Extreme Greed" territory, where the
# index historically mean-reverts; capping the magnitude stops a single extreme
# day from swinging the forecast. Applied to the mapped score, not the raw value.
SCORE_CAP = 0.8


def fetch_index(limit: int = 0, timeout: float = 20.0) -> list[dict]:
    request = urllib.request.Request(
        FEAR_GREED_URL.format(limit=limit),
        headers={"accept": "application/json", "user-agent": "sentiment-harness/0.1"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read())
    rows = payload.get("data") or []
    out = []
    for row in rows:
        try:
            out.append({
                "timestamp": int(row["timestamp"]),
                "value": int(row["value"]),
                "classification": str(row.get("value_classification") or ""),
            })
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda r: r["timestamp"])
    return out


def to_reading(row: dict, confidence: float) -> dict:
    # 0..100 -> -1..1, centred at neutral 50.
    score = max(-SCORE_CAP, min(SCORE_CAP, (row["value"] - 50) / 50.0))
    return {
        "timestamp": datetime.fromtimestamp(row["timestamp"], timezone.utc).isoformat().replace("+00:00", "Z"),
        "t": row["timestamp"],
        "score": round(score, 6),
        "confidence": confidence,
        "horizonMinutes": 1440,
        "rationale": f"alternative.me Fear & Greed {row['value']} ({row['classification']})",
        "drivers": [row["classification"]] if row["classification"] else [],
        "source": "alternative.me/fng",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="data/sentiment_harness/sentiment/fear-greed.jsonl")
    parser.add_argument("--confidence", type=float, default=DEFAULT_CONFIDENCE)
    parser.add_argument("--since", default=None, help="ISO date; default is to emit everything")
    args = parser.parse_args()

    try:
        rows = fetch_index()
    except Exception as error:  # network is a normal failure here, not a crash
        print(json.dumps({"error": "fear_greed_fetch_failed", "detail": str(error)[:200]}), file=sys.stderr)
        return 2
    if not rows:
        print(json.dumps({"error": "fear_greed_empty"}), file=sys.stderr)
        return 2

    if args.since:
        cutoff = int(datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc).timestamp())
        rows = [r for r in rows if r["timestamp"] >= cutoff]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(to_reading(row, args.confidence)) + "\n")

    audit = {
        "source": "alternative.me/fng",
        "readings": len(rows),
        "first": rows[0]["timestamp"],
        "last": rows[-1]["timestamp"],
        "confidence": args.confidence,
        "horizonMinutes": 1440,
        "score_cap": SCORE_CAP,
        "out": str(out_path),
    }
    print(json.dumps(audit), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())