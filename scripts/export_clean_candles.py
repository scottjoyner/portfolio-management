#!/usr/bin/env python3
"""Export feed-cache candles to JSONL for the sentiment decision harness.

WHY THIS EXISTS. The parquet feed cache is the only real OHLCV we have, and it is
not clean. Three distinct problems, all of which would silently poison a
calibration run:

  * Unit-test fixture rows at the head of the 1h files -- BTC-USD/3600.parquet
    opens with t=0, t=1, t=5 at o=10 h=12 l=9 c=11 v=100. These are the first
    rows any naive loader reads, and a momentum model fitted on them learns that
    price is ~11.
  * One-row stubs at coarse granularity -- ETH-USD/86400.parquet and
    SOL-USD/86400.parquet each hold exactly one row at t=0.
  * Constant-date CSVs elsewhere in data/historical (every row dated 2023-05-11)
    and BTC-USD_synthetic_5yrs.csv, whose open diverges to 3.9e110. Not touched
    here; this exporter only reads the parquet cache, because that is the one
    series with real timestamps and a plausible price range.

The harness scores a forecast against realized forward returns, so a single fake
row at t=0 does not merely add noise -- it changes the sign of "did sentiment
help" for whichever window it lands in. Rejecting the rows is therefore part of
the measurement, not hygiene.

Emits JSONL on stdout or to --out, plus a JSON audit on stderr so a run records
exactly how much it discarded.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

# 2000-01-01T00:00:00Z. Coinbase has not existed since 2012, so anything earlier is
# a fixture or a unit error, and a crypto feed with no bar before 2012 is a
# strong signal the file is synthetic.
MIN_VALID_TIMESTAMP = 946_684_800


def clean_candles(frame: pd.DataFrame, symbol: str, granularity: int) -> tuple[pd.DataFrame, dict]:
    audit = {
        "symbol": symbol,
        "granularity": granularity,
        "rows_in": int(len(frame)),
        "dropped_implausible_timestamp": 0,
        "dropped_duplicate_timestamp": 0,
        "dropped_ohlc_inconsistent": 0,
        "dropped_nonpositive_price": 0,
        "dropped_unsorted": 0,
    }
    out = frame.copy()
    out["t"] = pd.to_numeric(out["t"], errors="coerce")
    before = len(out)
    out = out[out["t"].notna() & (out["t"] >= MIN_VALID_TIMESTAMP)]
    audit["dropped_implausible_timestamp"] = before - len(out)

    out = out.sort_values("t", kind="stable")
    audit["dropped_unsorted"] = int((out["t"].diff() < 0).sum())

    before = len(out)
    out = out.drop_duplicates(subset=["t"], keep="last")
    audit["dropped_duplicate_timestamp"] = before - len(out)

    for column in ("o", "h", "l", "c", "v"):
        out[column] = pd.to_numeric(out[column], errors="coerce")

    before = len(out)
    out = out[out[["o", "h", "l", "c"]].notna().all(axis=1)]
    out = out[(out["o"] > 0) & (out["h"] > 0) & (out["l"] > 0) & (out["c"] > 0)]
    audit["dropped_nonpositive_price"] = before - len(out)

    # A bar where the high is below the close is arithmetically impossible. These
    # come from partially-written rows and from the synthetic CSVs.
    before = len(out)
    consistent = (
        (out["h"] >= out[["o", "c", "l"]].max(axis=1))
        & (out["l"] <= out[["o", "c", "h"]].min(axis=1))
        & (out["v"] >= 0)
    )
    out = out[consistent]
    audit["dropped_ohlc_inconsistent"] = before - len(out)

    audit["rows_out"] = int(len(out))
    return out, audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", default="BTC-USD")
    parser.add_argument("--granularity", type=int, default=3600)
    parser.add_argument("--cache-root", default=None, help="defaults to data/feed_cache")
    parser.add_argument("--min-rows", type=int, default=200, help="refuse to emit a series too short to score")
    parser.add_argument("--out", default="-")
    args = parser.parse_args()

    # parents[1] is the repo root: parents[0] is scripts/. parents[2] would be the
    # directory containing the checkout, and the cache would silently not be found.
    root = Path(args.cache_root) if args.cache_root else Path(__file__).resolve().parents[1] / "data" / "feed_cache"
    source = root / "coinbase_candles" / args.symbol / f"{args.granularity}.parquet"
    if not source.exists():
        print(json.dumps({"error": "candle_source_missing", "path": str(source)}), file=sys.stderr)
        return 2

    frame = pd.read_parquet(source)
    cleaned, audit = clean_candles(frame, args.symbol, args.granularity)

    # Continuity: a gap longer than 3 intervals means the forward-return target
    # spans a period we did not observe, which would be scored as a real move.
    if len(cleaned) > 1:
        deltas = cleaned["t"].diff().dropna()
        expected = args.granularity
        audit["largest_gap_intervals"] = float(deltas.max() / expected) if expected else None
        audit["gaps_over_3_intervals"] = int((deltas > expected * 3).sum())

    if audit["rows_out"] < args.min_rows:
        audit["error"] = "clean_series_too_short"
        print(json.dumps(audit), file=sys.stderr)
        return 3

    handle = sys.stdout if args.out == "-" else open(args.out, "w", encoding="utf-8")
    try:
        for row in cleaned.itertuples(index=False):
            handle.write(json.dumps({
                "t": int(row.t),
                "timestamp": pd.Timestamp(row.t, unit="s", tz="UTC").isoformat().replace("+00:00", "Z"),
                "open": float(row.o),
                "high": float(row.h),
                "low": float(row.l),
                "close": float(row.c),
                "volume": float(row.v),
            }) + "\n")
    finally:
        if handle is not sys.stdout:
            handle.close()

    print(json.dumps(audit), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())