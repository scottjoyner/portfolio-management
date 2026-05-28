"""Redis Cache Layer for Trading System API

Provides caching infrastructure for performance-critical endpoints:
- /metrics - System metrics (high cache duration, infrequent changes)
- /accounts - Plaid account list (medium cache duration)
- /positions - Current positions (shorter cache, frequent updates)

Architecture:
┌─────────────────────────────────────────────────────┐
│            Redis Cache Layer                         │
├─────────────────────────────────────────────────────┤
│                                                     │
│    ┌─────────────────────────────────────────┐      │
│    │         Cache Manager                    │      │
│    ├─────────────────────────────────────────┤      │
│    │ • TTL-based expiration control          │      │
│    │ • Cache hit/miss tracking               │      │
│    │ • Key serialization (pickle/json)       │      │
│    │ • Distributed cache ready (Redis cluster)│      │
│    └─────────────────────────────────────────┘      │
│                     │                            │
│             ┌───────┴───────┐                    │
│             ▼              ▼                    │
│        /metrics         /accounts              │
│   TTL: 30s            TTL: 60s                 │
│             │              │                  │
│     [cached]      [cached]                      │
└─────────────────────────────────────────────────────┘

Cache Strategy:
- Cache key prefix: "trading_system:"
- Metrics: 30s TTL (high-change data)
- Accounts: 60s TTL (less frequent changes)
- Positions: 15s TTL (real-time market updates)
"""

from __future__ import annotations

import json
import time
import hashlib
import pickle
from datetime import datetime, timezone
from typing import Any, Optional, Union
from functools import wraps


class RedisCacheManager:
    """Redis cache manager for trading system API endpoints."""
    
    def __init__(self, redis_client=None):
        """Initialize cache manager.
        
        Args:
            redis_client: Redis client instance. If None, uses default configuration.
        """
        self.redis = redis_client
        
        # Cache configurations per endpoint
        self.cache_configs = {
            "metrics": {
                "ttl_seconds": 30,           # High-frequency data
                "prefix": "ts_metrics:",
                "max_size_mb": 10,
            },
            "accounts": {
                "ttl_seconds": 60,          # Moderate refresh rate
                "prefix": "ts_accounts:",
                "max_size_mb": 5,
            },
            "positions": {
                "ttl_seconds": 15,          # Near real-time
                "prefix": "ts_positions:",
                "max_size_mb": 10,
            },
            "strategies": {
                "ttl_seconds": 300,         # Strategy metrics rarely change
                "prefix": "ts_strategies:",
                "max_size_mb": 20,
            },
            "performance": {
                "ttl_seconds": 120,         # Calculated metrics
                "prefix": "ts_performance:",
                "max_size_mb": 15,
            },
        }
        
        # Statistics tracking
        self.stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "errors": 0,
        }
    
    def _make_key(self, endpoint: str, data: Any) -> str:
        """Generate deterministic cache key from response data."""
        # Hash the response to create stable key
        serialized = json.dumps(data, sort_keys=True) if isinstance(data, dict) else pickle.dumps(data)
        return f"{self.cache_configs[endpoint]['prefix']}key_{hashlib.sha256(serialized.encode()).hexdigest()[:16]}"
    
    def _get_ttl(self, endpoint: str) -> int:
        """Get TTL for endpoint from configuration."""
        return self.cache_configs[endpoint].get("ttl_seconds", 30)
    
    def get(self, endpoint: str, key: Optional[str] = None, 
            response_data: Any = None) -> Optional[Any]:
        """
        Get cached value.
        
        Args:
            endpoint: Endpoint identifier for TTL configuration
            key: Optional specific cache key (uses _make_key if not provided)
            response_data: Full response to cache with this request
            
        Returns:
            Cached data if hit, None if miss/error
        """
        try:
            if self.redis is None:
                return None
            
            # Generate key
            actual_key = key or self._make_key(endpoint, response_data)
            
            cached = self.redis.get(actual_key)
            
            if cached:
                # Cache hit
                try:
                    deserialized = json.loads(cached) if isinstance(cached, str) else pickle.loads(cached)
                    self.stats["hits"] += 1
                    return deserialized
                except Exception:
                    self.stats["errors"] += 1
                    return None
            
            # Cache miss
            self.stats["misses"] += 1
            return None
            
        except Exception as e:
            self.stats["errors"] += 1
            return None
    
    def set(self, endpoint: str, key: Optional[str] = None, 
            response_data: Any = None, force_refresh: bool = False) -> bool:
        """
        Set cached value.
        
        Args:
            endpoint: Endpoint identifier for TTL configuration
            key: Optional specific cache key (uses _make_key if not provided)
            response_data: Full response to cache
            force_refresh: If True, overwrite existing cache
            
        Returns:
            True if cached successfully
        """
        try:
            if self.redis is None:
                return False
            
            # Generate key
            actual_key = key or self._make_key(endpoint, response_data)
            
            # Serialize data
            serialized = json.dumps(response_data, sort_keys=True) if isinstance(response_data, dict) else pickle.dumps(response_data).decode('utf-8')
            
            ttl = self._get_ttl(endpoint)
            
            # Check existing cache and evict if needed (simple LRU-like behavior)
            existing = self.redis.get(actual_key)
            if existing:
                ttl_remaining = int(self.redis.ttl(actual_key)) or 0
                if not force_refresh:
                    return True  # Already cached
            
            # Set with TTL
            self.redis.setex(actual_key, ttl, serialized)
            return True
            
        except Exception as e:
            self.stats["errors"] += 1
            return False
    
    def invalidate(self, key_pattern: Optional[str] = None) -> int:
        """
        Invalidate cache entries.
        
        Args:
            key_pattern: Glob pattern to match (e.g., "ts_metrics:*") or None for all
            
        Returns:
            Number of keys invalidated
        """
        try:
            if self.redis is None or key_pattern is None:
                return 0
            
            keys = self.redis.scan_iter(match=key_pattern, count=100)
            keys_to_delete = list(keys)
            
            for key in keys_to_delete:
                self.redis.delete(key)
            
            return len(keys_to_delete)
            
        except Exception:
            self.stats["errors"] += 1
            return 0
    
    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.stats["hits"] + self.stats["misses"]
        
        return {
            "hit_rate_pct": round(self.stats["hits"] / total_requests * 100, 2) if total_requests > 0 else 0,
            "total_requests": total_requests,
            "hits": self.stats["hits"],
            "misses": self.stats["misses"],
            "evictions": self.stats["evictions"],
            "errors": self.stats["errors"],
        }
    
    def health_check(self) -> dict[str, Any]:
        """Health check for cache service."""
        try:
            if self.redis is None:
                return {"redis_connected": False, "reason": "Redis not configured"}
            
            pong = self.redis.ping()
            
            info = self.redis.info("stats")
            
            return {
                "redis_connected": True,
                "pong_received": bool(pong),
                "connected_keys": info.get("connected_keys", 0),
                "blocked_connections": info.get("blocked_connections", 0),
                "used_memory_mb": round(info.get("used_memory_human", "0MB"), 2),
            }
        except Exception:
            return {
                "redis_connected": False,
                "error": "Health check failed"
            }


# ============================================================================
# DECORATOR FOR CACHE-AWARE ENDPOINTS
# ============================================================================

def cache(endpoint: str) -> Any:  # type: ignore[no-any-return]
    """
    Decorator to add caching to API endpoints.
    
    Usage:
        @cache("metrics")
        async def get_metrics() -> Dict[str, Any]:
            # Fetch fresh data
            ...
            
    Returns cached response if available within TTL, otherwise fetches fresh.
    """
    from functools import wraps
    
    def decorator(func) -> Any:  # type: ignore[no-any-return]
        @wraps(func)
        async def wrapper(*args, db=None, **kwargs) -> Any:
            cache_manager = kwargs.get('cache_manager') or args[1] if len(args) > 1 else None
            
            # Try cache first
            cached = cache_manager.get(endpoint) if cache_manager else None
            
            if cached:
                return cached
            
            # Cache miss - fetch fresh data
            response_data = await func(*args, db=db, **kwargs)
            
            # Cache the response
            if cache_manager:
                cache_manager.set(endpoint, response_data=response_data)
            
            return response_data
        
        return wrapper
    
    return decorator
