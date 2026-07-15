"""Coinbase pair discovery — get all available pairs with volume filtering."""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import urllib3

from coinbase.src.api_throttle import api_slot

log = logging.getLogger(__name__)

_http = urllib3.PoolManager(maxsize=5)
_QUOTE_USD_CACHE: Dict[str, float] = {}

# Result cache for get_all_coinbase_pairs / top_coinbase_pairs to avoid
# hammering /products + 437 ticker fetches on every scan.
_PAIRS_CACHE: Dict[Tuple[float, Tuple[str, ...]], Tuple[float, List[Dict]]] = {}
_PAIRS_CACHE_TTL = 60.0


_STABLE_QUOTES = {
    "USD",
    "USDC",
    "USDT",
    "DAI",
    "USD1",
    "USDS",
    "PAX",
    "GUSD",
    "TUSD",
    "FDUSD",
    "PYUSD",
}


def _quote_to_usd_rate(quote: str) -> Optional[float]:
    quote = (quote or "").upper()
    if quote in _QUOTE_USD_CACHE:
        return _QUOTE_USD_CACHE[quote]
    if quote in _STABLE_QUOTES:
        _QUOTE_USD_CACHE[quote] = 1.0
        return 1.0
    if quote in ("BTC", "ETH"):
        pid = f"{quote}-USD"
        try:
            with api_slot():
                r = _http.request("GET", f"https://api.exchange.coinbase.com/products/{pid}/ticker", timeout=10)
            t = json.loads(r.data)
            px = float(t.get("price", 0) or 0)
            if px > 0:
                _QUOTE_USD_CACHE[quote] = px
                return px
        except Exception:
            return None
    return None


def get_all_coinbase_pairs(
    min_volume_usd: float = 500_000,
    quote_currencies: Tuple[str, ...] = ("USD", "USDC", "BTC", "ETH"),
) -> List[Dict]:
    """Fetch all online Coinbase products with optional volume filter.

    Args:
        min_volume_usd: minimum 24h volume in USD (0 = no filter)
        quote_currencies: filter by quote currency

    Returns:
        List of product dicts: {"id", "base", "quote", "volume_24h"}
    """
    cache_key = (min_volume_usd, tuple(quote_currencies))
    cached = _PAIRS_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < _PAIRS_CACHE_TTL:
        return cached[1]

    try:
        with api_slot():
            r = _http.request(
                "GET",
                "https://api.exchange.coinbase.com/products",
                timeout=15,
            )
        all_products = json.loads(r.data)
    except Exception as e:
        log.error("Failed to fetch Coinbase products: %s", e)
        return []

    active = []
    for p in all_products:
        if p.get("status") != "online":
            continue
        if p.get("trading_disabled", False):
            continue
        if p.get("quote_currency") not in quote_currencies:
            continue
        active.append({
            "id": p["id"],
            "base": p["base_currency"],
            "quote": p["quote_currency"],
        })

    log.info("Found %d active Coinbase pairs (%s)", len(active), ", ".join(quote_currencies))

    if min_volume_usd > 0:
        active = _filter_by_volume(active, min_volume_usd)

    _PAIRS_CACHE[cache_key] = (time.time(), active)
    return active


def _filter_by_volume(
    products: List[Dict],
    min_volume: float,
    max_workers: int = 5,
) -> List[Dict]:
    """Get tickers for all products and filter by 24h volume.

    Throttled via the global API slot so we never exceed Coinbase's
    per-IP connection ceiling even with hundreds of products.
    """
    def get_volume(p: Dict) -> Optional[Dict]:
        try:
            with api_slot():
                r = _http.request(
                    "GET",
                    f"https://api.exchange.coinbase.com/products/{p['id']}/ticker",
                    timeout=10,
                )
            t = json.loads(r.data)
            quote_rate = _quote_to_usd_rate(p.get("quote", ""))
            if quote_rate is None:
                return None
            vol = float(t.get("volume", 0) or 0) * float(t.get("price", 0) or 0) * quote_rate
            p["volume_24h"] = vol
            return p if vol >= min_volume else None
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(get_volume, p): p["id"] for p in products}
        filtered = []
        for fut in as_completed(futures):
            try:
                r = fut.result()
                if r:
                    filtered.append(r)
            except Exception:  # pragma: no cover - get_volume swallows its own errors
                pass

    filtered.sort(key=lambda p: -p.get("volume_24h", 0))
    log.info("  After volume filter ($%.0f): %d pairs", min_volume, len(filtered))
    return filtered


def top_coinbase_pairs(
    n: int = 100,
    min_volume_usd: float = 500_000,
    quote_currencies: Tuple[str, ...] = ("USD", "USDC", "BTC", "ETH"),
) -> List[Tuple[str, str]]:
    """Get top N Coinbase pairs by volume as (product_id, base) tuples."""
    products = get_all_coinbase_pairs(
        min_volume_usd=min_volume_usd,
        quote_currencies=quote_currencies,
    )
    return [(p["id"], p["base"]) for p in products[:n]]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    pairs = top_coinbase_pairs(30, min_volume_usd=1_000_000)
    print(f"\nTop {len(pairs)} pairs by volume:")
    for pid, base in pairs:
        print(f"  {pid:12s}")
