#!/usr/bin/env python3
"""Aggregate key-free market data into the durable feed cache for backtesting.

Collects from four sources that require NO API keys:

  * Coinbase public candles  (api.exchange.coinbase.com)        -> coinbase_candles/<SYM>/<GRAN>.parquet
  * Yahoo Finance v8 chart   (query1.finance.yahoo.com)         -> yahoo_candles/<SYM>/<GRAN>.parquet
  * CoinGecko market_chart   (api.coingecko.com)                -> coingecko_candles/<CG>/<GRAN>.parquet
  * Binance funding premiumIndex (fapi.binance.com)             -> binance_funding/<SYM>.jsonl  (records)

Everything is normalized to [t, o, h, l, c, v] (epoch seconds + OHLCV) and
written through ``data.feed_cache`` (append + de-dup by timestamp), so
re-running only fills gaps.

Storage target is controlled by ``NAS_FEED_ROOT`` (the feed_cache root). Point
it at the SSD, e.g.:

    NAS_FEED_ROOT=/media/scott/SSD_4TB/feed_cache \\
        python3 scripts/collect_backtest_data.py --days 365

The script is network-only and idempotent; it never places orders.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.feed_cache import save_candles, save_records, ensure_root, get_metrics

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("collect")

# ── defaults ────────────────────────────────────────────────────────────────
COINBASE_API = "https://api.exchange.coinbase.com"
YAHOO_API = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
COINGECKO_API = "https://api.coingecko.com/api/v3/coins/{cg}/market_chart?vs_currency=usd&days={days}"
BINANCE_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"

# Stocks for the Yahoo leg (key-free). Daily bars, long history.
YAHOO_STOCKS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD",
    "SPY", "QQQ", "IWM", "TLT", "GLD", "SLV", "USO", "DIA",
]
# Crypto symbols also available on Yahoo (key-free, daily).
YAHOO_CRYPTO = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD"]

# CoinGecko id -> canonical label used for the cache subdir.
COINGECKO_IDS = {
    "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL", "ripple": "XRP",
    "cardano": "ADA", "dogecoin": "DOGE", "avalanche-2": "AVAX",
    "polkadot": "DOT", "chainlink": "LINK", "uniswap": "UNI",
    "usd-coin": "USDC", "tether": "USDT",
}

MAX_PER_PAGE = 300
UA = {"User-Agent": "Mozilla/5.0 PortfolioOptimizer/1.0"}


# ── low-level fetchers ────────────────────────────────────────────────────────

def _get_json(url: str, timeout: int = 20) -> Optional[Any]:
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:  # pragma: no cover - network
        log.debug("GET failed %s: %s", url.split("?")[0], e)
        return None


def _coinbase_chunk(product_id: str, granularity: int, start: int, end: int) -> List[List[float]]:
    url = (f"{COINBASE_API}/products/{product_id}/candles"
           f"?start={start}&end={end}&granularity={granularity}")
    data = _get_json(url)
    if not isinstance(data, list):
        return []
    out = []
    for row in data:
        # Coinbase returns [t, low, high, open, close, volume]
        t, lo, hi, o, c, v = row[0], row[1], row[2], row[3], row[4], row[5]
        out.append([float(t), float(o), float(hi), float(lo), float(c), float(v)])
    return out


def _yahoo_candles(symbol: str, days: int) -> List[List[float]]:
    """Daily Yahoo OHLCV -> [t,o,h,l,c,v] (epoch seconds)."""
    rng = "1y" if days >= 360 else ("6mo" if days >= 180 else ("3mo" if days >= 90 else "1mo"))
    url = YAHOO_API.format(symbol=symbol) + f"?range={rng}&interval=1d&includePrePost=false"
    data = _get_json(url)
    if not data:
        return []
    res = (data.get("chart") or {}).get("result")
    if not res:
        return []
    r0 = res[0]
    ts = r0.get("timestamp", [])
    q = (r0.get("indicators") or {}).get("quote", [{}])[0]
    out = []
    for i, t in enumerate(ts):
        o = (q.get("open") or [None])[i]
        c = (q.get("close") or [None])[i]
        if o is None or c is None:
            continue
        h = (q.get("high") or [None])[i] or c
        lo = (q.get("low") or [None])[i] or c
        v = (q.get("volume") or [None])[i] or 0
        out.append([float(t), float(o), float(h), float(lo), float(c), float(v)])
    return out


def _coingecko_candles(cg_id: str, days: int) -> List[List[float]]:
    """CoinGecko market_chart prices -> [t,c,c,c,c,v] candle-form (no OHLC from CG)."""
    url = COINGECKO_API.format(cg=cg_id, days=days)
    data = _get_json(url)
    if not isinstance(data, dict):
        return []
    prices = data.get("prices") or []
    vols = {int(t): v for t, v in (data.get("total_volumes") or [])}
    out = []
    for t, c in prices:
        v = vols.get(int(t), 0.0)
        out.append([float(t) / 1000.0, float(c), float(c), float(c), float(c), float(v)])
    return out


def _binance_funding_snapshot() -> Optional[Dict]:
    data = _get_json(BINANCE_URL)
    if not isinstance(data, list):
        return None
    now = int(time.time())
    rows = []
    for e in data:
        sym = e.get("symbol")
        if not sym or "USDT" not in sym:
            continue
        try:
            rows.append({
                "ts": now,
                "symbol": sym,
                "lastFundingRate": float(e.get("lastFundingRate", 0)),
                "markPrice": float(e.get("markPrice", 0)),
                "indexPrice": float(e.get("indexPrice", 0)),
            })
        except (TypeError, ValueError):
            continue
    return {"ts": now, "rates": rows} if rows else None


# ── collectors ────────────────────────────────────────────────────────────────

async def _coinbase_backfill(symbols: Sequence[str], granularities: List[int],
                             days: int, sleep: float) -> int:
    total = 0
    now = int(time.time())
    oldest = now - days * 86400
    for product_id in symbols:
        for g in granularities:
            end = now
            while end > oldest:
                start = end - MAX_PER_PAGE * g
                if start < oldest:
                    start = oldest
                candles = _coinbase_chunk(product_id, g, start, end)
                if candles:
                    total += save_candles("coinbase_candles", product_id, g, candles)
                end = start
                if sleep:
                    await asyncio.sleep(sleep)
            log.info("coinbase %s %ds: done", product_id, g)
    return total


def _yahoo_backfill(symbols: Sequence[str], days: int) -> int:
    total = 0
    for sym in symbols:
        candles = _yahoo_candles(sym, days)
        if candles:
            total += save_candles("yahoo_candles", sym, 86400, candles)
            log.info("yahoo %s: %d daily bars", sym, len(candles))
    return total


def _coingecko_backfill(days: int) -> int:
    total = 0
    for cg_id, label in COINGECKO_IDS.items():
        candles = _coingecko_candles(cg_id, days)
        if candles:
            total += save_candles("coingecko_candles", label, 86400, candles)
            log.info("coingecko %s (%s): %d daily bars", label, cg_id, len(candles))
    return total


def _binance_backfill() -> int:
    snap = _binance_funding_snapshot()
    if not snap:
        log.warning("binance funding snapshot empty (geo-blocked?)")
        return 0
    n = save_records("binance_funding", "premium_index", [snap])
    log.info("binance funding: %d snapshot(s), %d pairs", n, len(snap["rates"]))
    return n


# ── orchestration ─────────────────────────────────────────────────────────────

async def run(args) -> Dict[str, int]:
    ensure_root()
    summary: Dict[str, int] = {}

    if args.sources in ("all", "coinbase"):
        try:
            from coinbase.src.pair_discovery import top_coinbase_pairs
            pairs = top_coinbase_pairs(n=args.top_n)
            symbols = [p[0] for p in pairs]
        except Exception as e:
            log.warning("coinbase pair discovery failed: %s", e)
            symbols = ["BTC-USD", "ETH-USD", "SOL-USD"]
        granularities = [int(g) for g in args.granularities.split(",")]
        log.info("Coinbase: %d pairs x %s for %dd", len(symbols), granularities, args.days)
        summary["coinbase"] = await _coinbase_backfill(symbols, granularities, args.days, args.sleep)

    if args.sources in ("all", "yahoo"):
        ysyms = list(YAHOO_STOCKS)
        if args.include_crypto_yahoo:
            ysyms += YAHOO_CRYPTO
        log.info("Yahoo: %d symbols daily for %dd", len(ysyms), args.days)
        summary["yahoo"] = _yahoo_backfill(ysyms, args.days)

    if args.sources in ("all", "coingecko"):
        log.info("CoinGecko: %d assets daily for %dd", len(COINGECKO_IDS), args.days)
        summary["coingecko"] = _coingecko_backfill(args.days)

    if args.sources in ("all", "binance"):
        summary["binance"] = _binance_backfill()

    return summary


def cli():
    p = argparse.ArgumentParser(description="Aggregate key-free backtest data into feed_cache")
    p.add_argument("--sources", default="all",
                   choices=["all", "coinbase", "yahoo", "coingecko", "binance"])
    p.add_argument("--top-n", type=int, default=50, help="top Coinbase pairs by volume")
    p.add_argument("--granularities", default="60,300,900,3600,86400",
                   help="Coinbase candle granularities (seconds)")
    p.add_argument("--days", type=int, default=365, help="history window in days")
    p.add_argument("--sleep", type=float, default=0.3, help="seconds between Coinbase page fetches")
    p.add_argument("--include-crypto-yahoo", action="store_true",
                   help="also pull BTC-USD/ETH-USD/etc from Yahoo (in addition to Coinbase)")
    args = p.parse_args()
    summary = asyncio.run(run(args))
    log.info("Collection complete: %s", summary)
    log.info("feed_cache metrics: %s", get_metrics())


if __name__ == "__main__":
    cli()
