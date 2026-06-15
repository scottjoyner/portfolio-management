"""Appendix: Complete fetcher implementation (continued from coinbase_history.py)"""

# This file contains the continuation of CoinbaseHistoryFetcher.get_candles() 
# after the patch point, plus a convenience wrapper for backtesting integration.


import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from pathlib import Path
import os

from trading_system.data.coinbase_history import Candle, FetchConfig, FetchResult


def _fetch_batch(
    cli_path: str,
    product_id: str, 
    granularity: str, 
    start_ts: int,
    end_ts: int,
    limit: int,
    environment: str
) -> Tuple[List[Dict], Optional[str]]:
    """Fetch a single batch of candles via CLI."""
    start_str = datetime.utcfromtimestamp(start_ts).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = datetime.utcfromtimestamp(end_ts).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    cmd = [
        cli_path, "products", "candles", product_id,
        f"start=={start_str}",
        f"end=={end_str}", 
        f"granularity=={granularity}",
        f"limit=={limit}",
        "-e", environment
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            env={**os.environ, "PATH": "/home/scott/.npm-global/bin:$PATH"}
        )
        
        if result.returncode != 0:
            return [], result.stderr or result.stdout.strip()
            
        try:
            data = json.loads(result.stdout)
            if isinstance(data, list):
                return data, None
            elif "candles" in data:
                return data["candles"], None
            else:
                return [], f"No candles in response: {data}"
        except json.JSONDecodeError as e:
            return [], f"JSON parse error: {e}. Raw: {result.stdout[:200]}"
            
    except subprocess.TimeoutExpired:
        return [], "Timeout after 30s"
    except FileNotFoundError:
        return [], "CLI not found at path"


def _normalize_candles(
    raw_data: List[Dict], 
    granularity: str
) -> List[Candle]:
    """Normalize CLI response to standard format."""
    gran_offsets = {"1m": 0, "5m": 0, "15m": 0, "30m": 0, "1h": 0, "2h": 0, "4h": 0, "6h": 0, "1d": 86400}
    offset = gran_offsets.get(granularity, 0)
    
    candles = []
    for row in raw_data:
        try:
            start_ts = int(float(row["start"])) // 60 * 60 + offset
            candles.append(Candle(
                ts=start_ts,
                open=float(row.get("open", 0.0)),
                high=float(row.get("high", 0.0)),
                low=float(row.get("low", 0.0)),
                close=float(row.get("close", 0.0)),
                volume=float(row.get("volume", 0.0))
            ))
        except (KeyError, ValueError):
            continue
    
    return candles


def fetch_historical_candles(
    product_id: str,
    granularity: str = "1h",
    days_back: int = 90,
    limit: Optional[int] = None,
    environment: str = "live",
    cache_dir: Optional[str] = None
) -> FetchResult:
    """
    Convenience wrapper for backtesting integration.
    
    Fetches historical candles using the CLI-based method with 
    automatic normalization and caching.
    
    Args:
        product_id: Trading pair (e.g., 'BTC-USD')
        granularity: Candle interval ('1m', '5m', '1h', '6h', '1d')
        days_back: How far back to fetch
        limit: Max candles per batch (default 300)
        environment: 'live' or 'sandbox'
        cache_dir: Optional directory for cached results
        
    Returns:
        FetchResult with normalized Candle objects
    """
    
    config = FetchConfig(
        environment=environment,
        cache_dir=cache_dir,
        max_candles_per_batch=limit or 300
    )
    
    fetcher = CoinbaseHistoryFetcher(config=config)
    
    # Use the new get_candles method (renamed from fetch_candles)
    return fetcher.get_candles(
        product_id=product_id,
        granularity=granularity,
        days_back=days_back
    )


def fetch_multiple_products(
    products: List[str],
    granularity: str = "1h",
    days_back: int = 90,
    environment: str = "live"
) -> Dict[str, FetchResult]:
    """
    Fetch historical data for multiple trading pairs.
    
    Args:
        products: List of product IDs (e.g., ['BTC-USD', 'ETH-USD'])
        granularity: Candle interval
        days_back: How far back
        environment: Trading environment
        
    Returns:
        Dict mapping product_id to FetchResult
    """
    results = {}
    for product in products:
        result = fetch_historical_candles(
            product_id=product,
            granularity=granularity,
            days_back=days_back,
            environment=environment
        )
        results[product] = result
        print(f"{product}: {len(result.candles)} candles{' (cached)' if result.cached else ''}")
    return results


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Test BTC-USD
    print("=" * 60)
    print("Testing CLI-based historical fetch (production)")
    print("=" * 60)
    
    result = fetch_historical_candles(
        "BTC-USD", 
        granularity="1h", 
        days_back=7
    )
    
    print(f"\nSuccess: {result.success}")
    print(f"Candles: {len(result.candles)}")
    if result.candles:
        c = result.candles[0]
        print(f"First candle:")
        print(f"  ts={c.ts} ({datetime.utcfromtimestamp(c.ts)})")
        print(f"  open=${c.open:.2f}, high=${c.high:.2f}")
        print(f"  low=${c.low:.2f}, close=${c.close:.2f}")
