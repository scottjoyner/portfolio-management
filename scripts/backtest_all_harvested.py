#!/usr/bin/env python3
"""One-shot: backtest every harvested symbol at 1h and persist into the
optimizer bt_cache so live confidence scoring reflects measured edge.

Run this once the Coinbase 365d backfill has populated enough 1h candles
across the top-N pairs (see E13 in docs/SYSTEM_GAP_ANALYSIS.md). It is the
"close the loop" job: harvest -> backtest -> ConfidenceMatrix weights.

Usage:
    python3 scripts/backtest_all_harvested.py --bt-cache-db optimizer_state.db
    python3 scripts/backtest_all_harvested.py --bt-cache-db optimizer_state.db --include-external
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import scripts.backtest_from_cache as B  # noqa: E402

log = logging.getLogger("backtest_all")


def main():
    p = argparse.ArgumentParser(description="Backtest all harvested symbols @1h into optimizer bt_cache")
    p.add_argument("--bt-cache-db", default="optimizer_state.db",
                   help="optimizer StateStore db to write verdicts into")
    p.add_argument("--granularity", type=int, default=3600, help="candle granularity (seconds)")
    p.add_argument("--min-rows", type=int, default=100,
                   help="skip symbols with fewer than this many candles at the granularity")
    p.add_argument("--only-passed", action="store_true", help="only persist passing strategies")
    p.add_argument("--include-external", action="store_true",
                   help="also backtest the 9 Python-path external/order-flow strategies")
    p.add_argument("--top", type=int, default=0, help="0 = print no per-strategy list")
    args = p.parse_args()

    # Auto-discover every symbol present in the cache at this granularity.
    import os as _os
    from data.feed_cache import _root, load_candles
    base = _os.path.join(_root(), B.KIND)
    symbols = []
    if _os.path.isdir(base):
        for name in _os.listdir(base):
            fp = _os.path.join(base, name, f"{args.granularity}.parquet")
            if _os.path.exists(fp) and len(load_candles(B.KIND, name, args.granularity)) >= args.min_rows:
                symbols.append(name)
    symbols.sort()
    log.info("Backtesting %d harvested symbols @ %ds", len(symbols), args.granularity)

    a = B._args_for(granularity=args.granularity, symbols=",".join(symbols),
                    include_external=args.include_external, only_passed=args.only_passed,
                    bt_cache_db=args.bt_cache_db, top=args.top)
    summary = B.run(a)
    log.info("DONE: symbols=%d signals=%d passed=%d pass_rate=%.1f%%",
              summary["n_symbols"], summary["n_signals"], summary["n_passed"],
              summary["pass_rate"] * 100)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
