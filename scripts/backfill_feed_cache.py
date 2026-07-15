#!/usr/bin/env python3
"""Backfill the NAS feed cache with historical candles for backtesting.

Fetches Coinbase candles cursor-by-cursor (300 bars/page, walking backwards
from now) for the top-N pairs across one or more granularities and persists
them via ``data.feed_cache`` (parquet append + de-dup). This populates history
that live ticks would otherwise never have captured.

Usage:
    # top 50 pairs x {1m,5m,15m,1h,1d} for the last 90 days
    python3 scripts/backfill_feed_cache.py --top-n 50 --days 90 \\
        --granularities 60,300,900,3600,86400 --sleep 0.5

    # explicit symbols only
    python3 scripts/backfill_feed_cache.py --symbols BTC-USD,ETH-USD \\
        --granularities 3600 --days 30
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aiohttp

from coinbase.src.pair_discovery import top_coinbase_pairs
from coinbase.src.rest_feed import _EXCHANGE_API, _normalize_candles
from data.feed_cache import save_candles, ensure_root, get_metrics

MAX_PER_PAGE = 300
_KIND = "coinbase_candles"


async def _fetch_chunk(session: aiohttp.ClientSession, product_id: str,
                       granularity: int, start: int, end: int):
    url = (f"{_EXCHANGE_API}/products/{product_id}/candles"
           f"?start={start}&end={end}&granularity={granularity}")
    try:
        async with session.get(url, headers={"User-Agent": "PortfolioOptimizer/1.0"}) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            return _normalize_candles(data, product_id)
    except Exception:
        return []


async def backfill_symbol(session, product_id, granularity, days, sleep):
    now = int(time.time())
    oldest = now - int(days) * 86400
    end = now
    total = 0
    while end > oldest:
        start = end - MAX_PER_PAGE * granularity
        if start < oldest:
            start = oldest
        candles = await _fetch_chunk(session, product_id, granularity, start, end)
        if candles:
            save_candles(_KIND, product_id, granularity, candles)
            total += len(candles)
        end = start
        if sleep:
            await asyncio.sleep(sleep)
    return total


async def main(args):
    ensure_root()
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        pairs = top_coinbase_pairs(n=args.top_n)
        symbols = [p[0] for p in pairs]
    granularities = [int(g) for g in args.granularities.split(",")]
    print(f"Backfilling {len(symbols)} symbols x {granularities} granularities for {args.days}d")
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for product_id in symbols:
            for g in granularities:
                try:
                    n = await backfill_symbol(session, product_id, g, args.days, args.sleep)
                    print(f"  {product_id} {g}s: {n} bars")
                except Exception as e:  # pragma: no cover - defensive
                    print(f"  ERROR {product_id} {g}s: {e}")
    print("Backfill complete. feed_cache metrics:", get_metrics())


def cli():
    p = argparse.ArgumentParser(description="Backfill NAS feed cache with historical candles")
    p.add_argument("--top-n", type=int, default=10, help="number of top pairs by volume")
    p.add_argument("--symbols", default="", help="explicit comma-separated product ids")
    p.add_argument("--granularities", default="60,300,900,3600,86400",
                   help="comma-separated candle granularities (seconds)")
    p.add_argument("--days", type=int, default=90, help="history window in days")
    p.add_argument("--sleep", type=float, default=0.5, help="seconds between page fetches")
    args = p.parse_args()
    asyncio.run(main(args))


if __name__ == "__main__":
    cli()
