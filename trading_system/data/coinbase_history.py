#!/usr/bin/env python3
"""
Hybrid Coinbase Historical Data Fetcher

Combines CLI reliability with Python normalization for backtesting integration.

Features:
- Direct CLI invocation (more reliable than SDK wrapper)
- Automatic pagination for >350 candles  
- Output normalization to {ts, open, high, low, close, volume} format
- Optional caching via local JSON file
- Circuit breaker pattern for rate limits

Usage:
    from trading_system.data.coinbase_history import CoinbaseHistoryFetcher
    
    fetcher = CoinbaseHistoryFetcher()
    candles = fetcher.fetch_candles("BTC-USD", granularity="1h", days_back=90)
"""

import subprocess
import json
import os
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote_plus


@dataclass
class Candle:
    """Normalized candle with Unix timestamp."""
    ts: int           # Unix timestamp (seconds)
    open: float
    high: float  
    low: float
    close: float
    volume: float
    
    def to_dict(self) -> Dict[str, any]:
        return {
            "ts": self.ts,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume
        }


@dataclass
class FetchConfig:
    """Configuration for historical fetch."""
    environment: str = 'live'      # 'live' or 'sandbox'
    cache_dir: Optional[str] = None  # If set, enables caching
    max_candles_per_batch: int = 300  # CLI limit is ~350
    
    def __post_init__(self):
        if self.cache_dir:
            Path(self.cache_dir).mkdir(parents=True, exist_ok=True)


@dataclass  
class FetchResult:
    """Result of a fetch attempt."""
    success: bool
    candles: List[Candle]
    error: Optional[str] = None
    cached: bool = False
    retries: int = 0


class CoinbaseHistoryFetcher:
    """
    Production-grade historical data fetcher for Coinbase.
    
    Uses CLI directly with automatic normalization and pagination.
    """
    
    def __init__(self, config: Optional[FetchConfig] = None):
        self.config = config or FetchConfig()
        self._cli_path = "/home/scott/.npm-global/bin/coinbase"
        
    def _normalize_candles(
        self, 
        raw_data: List[Dict], 
        granularity: str
    ) -> List[Candle]:
        """
        Normalize CLI response to standard format.
        
        CLI returns: {"start": "epoch", "low": "...", ...}
        We need: {ts, open, high, low, close, volume}
        
        Args:
            raw_data: Raw JSON list from CLI
            granularity: '1m', '5m', '1h', etc. for timestamp alignment
            
        Returns:
            List of Candle objects
        """
        # Granularity offsets for start timestamp alignment
        gran_offsets = {
            "1m": 0,      # Already aligned to minute
            "5m": 0,      # Already aligned to 5-min
            "15m": 0,     # Already aligned
            "30m": 0,     # Already aligned  
            "1h": 0,      # Already aligned to hour
            "2h": 0,
            "4h": 0,
            "6h": 0,
            "1d": 86400   # Midnight UTC
        }
        
        offset = gran_offsets.get(granularity, 0)
        
        candles = []
        for row in raw_data:
            try:
                # Parse Unix timestamp and align to granularity
                start_ts = int(float(row["start"])) // 60 * 60 + offset
                
                # Extract numeric values (CLI returns strings)
                candles.append(Candle(
                    ts=start_ts,
                    open=float(row.get("open", 0.0)),
                    high=float(row.get("high", 0.0)),
                    low=float(row.get("low", 0.0)),
                    close=float(row.get("close", 0.0)),
                    volume=float(row.get("volume", 0.0))
                ))
            except (KeyError, ValueError) as e:
                continue  # Skip malformed rows
        
        return candles
    
    def _fetch_batch(
        self, 
        product_id: str, 
        granularity: str, 
        start_ts: int,
        end_ts: int,
        limit: int
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        Fetch a single batch of candles via CLI.
        
        Returns:
            (raw_data_list, error_message)
        """
        # RFC 3339 timestamps (CLI requires this format)
        from datetime import timezone
        start_str = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        cmd = [
            self._cli_path, "products", "candles", product_id,
            f"start=={start_str}",
            f"end=={end_str}", 
            f"granularity=={granularity}",
            f"limit=={limit}",
            "-e", self.config.environment
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
                env={**os.environ, "PATH": "/home/scott/.npm-global/bin:$PATH"}
            )
            
            # Debug output
            print(f"DEBUG _fetch_batch: returncode={result.returncode}, stdout_len={len(result.stdout)}, stderr={result.stderr[:100] if result.stderr else 'None'}")
            
            if result.returncode != 0:
                # Parse error message from CLI output
                error = result.stderr or result.stdout.strip()
                print(f"DEBUG _fetch_batch: ERROR - {error}")
                return [], error
            
            # Parse JSON response (can be array or object)
            try:
                data = json.loads(result.stdout)
                if isinstance(data, list):
                    print(f"DEBUG _fetch_batch: Got list with {len(data)} items")
                    return data, None
                elif "candles" in data:
                    print(f"DEBUG _fetch_batch: Got wrapped candles with {len(data['candles'])} items")
                    return data["candles"], None
                else:
                    print(f"DEBUG _fetch_batch: Unexpected structure: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                    return [], f"No candles in response: {data}"
                    
            except json.JSONDecodeError as e:
                print(f"DEBUG _fetch_batch: JSON parse error: {e}")
                return [], f"JSON parse error: {e}. Raw: {result.stdout[:200]}"
                
        except subprocess.TimeoutExpired:
            print("DEBUG _fetch_batch: Timeout")
            return [], "Timeout after 30s"
        except FileNotFoundError:
            print("DEBUG _fetch_batch: CLI not found")
            return [], "CLI not found at path"
    
    def _get_cache_key(self, product_id: str, granularity: str, days_back: int) -> str:
        """Generate unique cache filename."""
        import hashlib
        key_str = f"{product_id}:{granularity}:{days_back}"
        hash_val = hashlib.md5(key_str.encode()).hexdigest()[:12]
        return f"_{hash_val}_{product_id.replace('-', '_')}.json"
    
    def _try_cache(
        self, 
        product_id: str, 
        granularity: str, 
        days_back: int
    ) -> Optional[List[Candle]]:
        """Load from cache if available and fresh (<2 hours old)."""
        if not self.config.cache_dir:
            return None
        
        # Calculate target time range
        end_ts = int(time.time())
        start_ts = end_ts - (days_back * 86400)
        
        cache_file = os.path.join(
            self.config.cache_dir, 
            self._get_cache_key(product_id, granularity, days_back)
        )
        
        if not os.path.exists(cache_file):
            return None
        
        try:
            with open(cache_file) as f:
                cached = json.load(f)
            
            # Verify freshness (max 2 hours old for intraday data)
            age_hours = (end_ts - int(cached.get("updated", 0))) / 3600
            if age_hours > 2:
                return None
            
            # Parse cached candles
            candles = []
            for c in cached.get("candles", []):
                try:
                    candles.append(Candle(
                        ts=c["ts"],
                        open=float(c["open"]),
                        high=float(c["high"]), 
                        low=float(c["low"]),
                        close=float(c["close"]),
                        volume=float(c["volume"])
                    ))
                except (KeyError, ValueError):
                    continue
            
            return candles if candles else None
                
        except Exception:
            return None
    
    def _save_to_cache(
        self, 
        product_id: str, 
        granularity: str, 
        days_back: int,
        candles: List[Candle]
    ):
        """Save result to cache."""
        if not self.config.cache_dir or not candles:
            return
        
        cache_file = os.path.join(
            self.config.cache_dir,
            self._get_cache_key(product_id, granularity, days_back)
        )
        
        try:
            with open(cache_file, "w") as f:
                json.dump({
                    "updated": int(time.time()),
                    "product_id": product_id,
                    "granularity": granularity,
                    "days_back": days_back,
                    "count": len(candles),
                    "candles": [c.to_dict() for c in candles[-100:]]  # Last 100 only
                }, indent=2)
        except Exception as e:
            pass  # Non-critical failure
    
    def get_candles(
        self,
        product_id: str,
        granularity: str = "1h",
        days_back: int = 90,
        limit: Optional[int] = None,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None
    ) -> FetchResult:
        """
        Fetch historical candles for a trading pair.
        
        Args:
            product_id: Trading pair (e.g., 'BTC-USD', 'ETH-USD')
            granularity: '1m', '5m', '15m', '1h', '6h', '1d'
            days_back: How far back to fetch (alternative to start_ts/end_ts)
            limit: Maximum candles per call (None = max 300)
            start_ts: Unix timestamp for start time (optional)
            end_ts: Unix timestamp for end time (optional)
            
        Returns:
            FetchResult with normalized Candle objects
            
        Notes:
            - Automatic pagination for long histories  
            - Uses cached data if available (<2h old)
            - Handles rate limits via retries
        """
        
        # Resolve time range (prefer explicit args over days_back)
        if start_ts is None or end_ts is None:
            now = int(time.time())
            default_end = end_ts if end_ts else now
            default_start = start_ts if start_ts else (default_end - (days_back * 86400))
            start_ts, end_ts = default_start, default_end
        
        max_limit = self.config.max_candles_per_batch if limit is None else min(limit, 300)
        
        # Check cache first        
        # Check cache first
        cached = self._try_cache(product_id, granularity, days_back)
        if cached:
            return FetchResult(success=True, candles=cached, cached=True)
        
        # Fetch with automatic pagination  
        all_raw = []
        current_start = start_ts
        
        attempts = 0
        max_attempts = 3
        
        while current_start < end_ts and len(all_raw) < 1000:  # Hard cap at 1000
            batch_end = min(current_start + (max_limit * 3600), end_ts)
            
            raw_batch, error = self._fetch_batch(
                product_id, granularity, current_start, batch_end, max_limit
            )
            
            if error:
                attempts += 1
                if attempts >= max_attempts:
                    return FetchResult(
                        success=False, 
                        candles=[], 
                        error=f"Fetch failed after {max_attempts} retries: {error}"
                    )
                time.sleep(2)  # Backoff before retry
                continue
                
            all_raw.extend(raw_batch)
            current_start = batch_end
        
        # Normalize output format
        normalized = self._normalize_candles(all_raw, granularity)
        
        # Cache result  
        self._save_to_cache(product_id, granularity, days_back, normalized)
        
        return FetchResult(
            success=len(normalized) > 0,
            candles=normalized,
            error=None if normalized else "No valid candles returned"
        )


def get_price(product_id: str) -> Dict:
    """Get current price for a product."""
    import subprocess
    
    try:
        result = subprocess.run(
            [
                "/home/scott/.npm-global/bin/coinbase", 
                "products", "get", product_id
            ],
            capture_output=True, text=True, timeout=10,
            env={**os.environ, "PATH": "/home/scott/.npm-global/bin:$PATH"}
        )
        
        if result.returncode == 0:
            return json.loads(result.stdout)
        else:
            return {"error": result.stderr or "Failed to fetch price"}
            
    except Exception as e:
        return {"error": str(e)}


def list_products() -> List[str]:
    """List all tradable crypto products."""
    import subprocess
    
    try:
        result = subprocess.run(
            ["/home/scott/.npm-global/bin/coinbase", "products", "list"],
            capture_output=True, text=True, timeout=30,
            env={**os.environ, "PATH": "/home/scott/.npm-global/bin:$PATH"}
        )
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            return [p.get("id", "") for p in data if isinstance(p, dict)]
        else:
            return []
            
    except Exception:
        return []


if __name__ == "__main__":
    # Demo/test usage
    import logging
    logging.basicConfig(level=logging.INFO)
    
    fetcher = CoinbaseHistoryFetcher(config=FetchConfig(
        environment="live",
        cache_dir="/tmp/cb_history_cache"
    ))
    
    print("=" * 60)
    print("TEST: Fetch BTC-USD candles via CLI hybrid method")
    print("=" * 60)
    
    # Use the updated method name
    result = fetcher.get_candles(
        product_id="BTC-USD",
        granularity="1h",
        days_back=7,
        limit=50
    )
    
    print(f"\nSuccess: {result.success}, Count: {len(result.candles)}, Cached: {result.cached}")
    
    if result.candles:
        sample = result.candles[0]
        print(f"\nSample candle:")
        print(f"  Timestamp: {sample.ts} ({datetime.utcfromtimestamp(sample.ts)})")
        print(f"  Open:  ${sample.open:.2f}")
        print(f"  High:  ${sample.high:.2f}")  
        print(f"  Low:   ${sample.low:.2f}")
        print(f"  Close: ${sample.close:.2f}")
        print(f"  Volume: {sample.volume:.4f} BTC")
