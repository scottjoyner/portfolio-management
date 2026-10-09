#!/usr/bin/env python3
"""Export feed-cache candles to JSONL for the sentiment and decision harnesses.

USES THE CANONICAL LOADER. An earlier version of this file read the parquet files
directly with pandas. That worked, but it bypassed the provenance chain the repo
deliberately built: data/feed_cache.load_candles is what canonical_replay.py and
run_experiment.py both call, and it is the documented API for the
<root>/coinbase_candles/<SYMBOL>/<GRANULARITY>.parquet layout. Reading the files
by hand meant a replay could consume data the canonical path would have resolved
differently.

WHY SANITIZATION IS PART OF THE MEASUREMENT, NOT HYGIENE.
  * Unit-test fixture rows at the head of the 1h files -- BTC-USD/3600 opens with
    t=0, t=1, t=5 at o=10 h=12 l=9 c=11 v=100. A momentum model fitted on those
    learns that price is about 11.
  * Coarse-granularity stubs: ETH-USD/86400 and SOL-USD/86400 each hold one row.
  * Duplicate timestamps and impossible OHLC from partially written rows.
A single fake row does not add noise; it changes the sign of "did this help" for
whichever window it lands in. Note that canonical_replay.py's own
normalize_candle_rows enforces the OHLC invariants but has no timestamp floor, so
a replay over the dirty files will not raise -- it will quietly produce a forecast
built on fixture rows.

Not touched: data/historical. Every *-daily.csv there carries a constant date of
2023-05-11 and BTC-USD_synthetic_5yrs.csv diverges to 1e110.

Emits one JSONL file per symbol plus a manifest carrying the dataset identity and
a row hash per symbol, so a cross-sectional run attests what it actually read.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data.feed_cache import load_candles  # noqa: E402
from scripts.alpha_validation import stable_hash  # noqa: E402

# 2000-01-01T00:00:00Z. Coinbase did not exist until 2012.
MIN_VALID_TIMESTAMP = 946_684_800
COLUMNS = ["t", "o", "h", "l", "c", "v"]


def clean_rows(rows, symbol: str, granularity: int) -> tuple[list[list[float]], dict]:
    audit = {
        "symbol": symbol,
        "granularity": granularity,
        "rows_in": len(rows),
        "dropped_implausible_timestamp": 0,
        "dropped_duplicate_timestamp": 0,
        "dropped_ohlc_inconsistent": 0,
        "dropped_nonpositive_price": 0,
    }

    # Impose ordering and the floor in one pass.
    floor = 0
    for row in rows:
        try:
            t = float(row[0])
        except (TypeError, ValueError, IndexError):
            floor += 1
            continue
        if t < MIN_VALID_TIMESTAMP:
            floor += 1
    audit["dropped_implausible_timestamp"] = floor

    kept = []
    for row in rows:
        try:
            values = [float(value) for value in row[:6]]
        except (TypeError, ValueError, IndexError):
            continue
        if values[0] < MIN_VALID_TIMESTAMP:
            continue
        kept.append(values)
    kept.sort(key=lambda r: r[0])

    before = len(kept)
    deduped = []
    for row in kept:
        if deduped and row[0] == deduped[-1][0]:
            deduped[-1] = row  # last write wins, matching feed_cache.save_candles
        else:
            deduped.append(row)
    audit["dropped_duplicate_timestamp"] = before - len(deduped)
    kept = deduped

    before = len(kept)
    priced = [r for r in kept if r[1] > 0 and r[2] > 0 and r[3] > 0 and r[4] > 0]
    audit["dropped_nonpositive_price"] = before - len(priced)
    kept = priced

    before = len(kept)
    consistent = [
        r for r in kept
        if r[2] >= max(r[1], r[4], r[3]) and r[3] <= min(r[1], r[4], r[2]) and r[5] >= 0
    ]
    audit["dropped_ohlc_inconsistent"] = before - len(consistent)
    kept = consistent

    audit["rows_out"] = len(kept)
    return kept, audit


def dataset_id(symbol: str, granularity: int, rows: list[list[float]]) -> str:
    """Derived from the rows, not a caller-supplied label."""
    return f"coinbase_candles:{symbol}:{granularity}:{int(rows[0][0])}:{int(rows[-1][0])}:{len(rows)}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbols", default="", help="comma separated; default is every symbol found")
    parser.add_argument("--granularity", type=int, default=3600)
    parser.add_argument("--out-dir", default="data/sentiment_harness/candles")
    parser.add_argument("--min-rows", type=int, default=200)
    args = parser.parse_args()

    wanted = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not wanted:
        # Match the canonical layout rather than globbing the parquet files, so
        # discovery agrees with what load_candles will resolve.
        from data.feed_cache import _root
        base = Path(_root()) / "coinbase_candles"
        wanted = sorted(p.parent.name for p in base.glob(f"*/{args.granularity}.parquet")) if base.exists() else []

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / f"manifest-{args.granularity}.json"
    entries, audits = [], []

    for symbol in wanted:
        try:
            rows = load_candles("coinbase_candles", symbol, args.granularity)
        except Exception as error:  # a missing symbol must not kill the sweep
            audits.append({"symbol": symbol, "error": str(error)[:160]})
            continue
        cleaned, audit = clean_rows(rows or [], symbol, args.granularity)
        if len(cleaned) < args.min_rows:
            audit["error"] = "clean_series_too_short"
            audit["rows_out"] = len(cleaned)
            audits.append(audit)
            continue

        target = out_dir / f"{symbol}-{args.granularity // 60}m.jsonl"
        with target.open("w", encoding="utf-8") as handle:
            for row in cleaned:
                handle.write(json.dumps({
                    "t": int(row[0]),
                    "open": row[1], "high": row[2], "low": row[3], "close": row[4], "volume": row[5],
                }) + "\n")

        entries.append({
            "symbol": symbol,
            "granularity": args.granularity,
            "file": target.name,
            "row_count": len(cleaned),
            "start_ts": int(cleaned[0][0]),
            "end_ts": int(cleaned[-1][0]),
            "dataset_id": dataset_id(symbol, args.granularity, cleaned),
            "rows_hash": stable_hash([[int(r[0]), r[1], r[2], r[3], r[4], r[5]] for r in cleaned]),
        })
        audits.append(audit)

    manifest = {
        "kind": "coinbase_candles",
        "granularity": args.granularity,
        "min_valid_timestamp": MIN_VALID_TIMESTAMP,
        "symbols_found": len(wanted),
        "symbols_exported": len(entries),
        "datasets": entries,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    audit_summary = {
        "manifest": str(manifest_path),
        "symbols_exported": len(entries),
        "symbols_rejected": len([a for a in audits if a.get("error")]),
        "rows_exported": sum(e["row_count"] for e in entries),
        "rows_dropped": sum(a.get("rows_in", 0) - a.get("rows_out", 0) for a in audits),
        "reasons": sorted({a["error"] for a in audits if a.get("error")}),
    }
    print(json.dumps(audit_summary), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())