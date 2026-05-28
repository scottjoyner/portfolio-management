"""Redis Cache Factory - Create Cache Manager Instances

This module provides cache manager factories for different deployment scenarios:
- Development: Mock cache (no Redis connection)
- Production: Real Redis client with connection pooling
- Docker: Redis container integration
"""

from typing import Any, Optional


def create_cache_manager(redis_url: Optional[str] = None, 
                         use_mock: bool = False) -> Any:  # type: ignore[no-any-return]
    """
    Create a cache manager instance.
    
    Args:
        redis_url: Redis connection URL (e.g., "redis://localhost:6379/0")
        use_mock: If True, create a mock cache (for development)
    
    Returns:
        CacheManager instance ready for endpoint integration
    """
    try:
        from trading_system.cache.redis import RedisCacheManager
        
        # Create actual Redis client or mock
        if use_mock:
            print("[CACHE] Using mock cache (development mode)")
            return _create_mock_cache_manager()
        elif redis_url:
            print(f"[CACHE] Connecting to Redis at {redis_url}")
            return _create_real_redis_cache(redis_url)
        else:
            # No Redis configured - use mock with warnings
            print("[CACHE] No Redis URL provided, using mock cache")
            return _create_mock_cache_manager()
    except ImportError:
        # Redis not installed - use mock mode by default
        print("[CACHE] Redis package not found, using mock cache")
        return _create_mock_cache_manager()


def _create_mock_cache_manager() -> Any:  # type: ignore[no-any-return]
    """Create a mock cache manager for development/testing."""
    
    class MockRedisClient:
        def get(self, key):
            return None
        
        def setex(self, key, ttl, value):
            pass
        
        def ping(self):
            return True
        
        def info(self, section=None):
            return {"connected_keys": 0}
    
    mock_redis = MockRedisClient()
    cache_manager = RedisCacheManager(redis_client=mock_redis)
    
    # Mark as development mode
    cache_manager.is_mock = True
    
    return cache_manager


def _create_real_redis_cache(redis_url: str) -> Any:  # type: ignore[no-any-return]
    """Create a real Redis client with connection pooling."""
    
    try:
        import redis
        
        # Create Redis pool for connection management
        pool = redis.ConnectionPool.from_url(
            redis_url,
            max_connections=10,
            decode_responses=False,  # Return bytes internally
        )
        
        redis_client = redis.Redis(connection_pool=pool)
        
        return RedisCacheManager(redis_client=redis_client)
    except ImportError:
        print("[CACHE] Cannot create real cache - redis package not available")
        return _create_mock_cache_manager()


def get_cache_for_endpoint(endpoint: str, cache_manager: Any) -> Optional[Any]:  # type: ignore[no-any-return]
    """
    Get cached data for specific endpoint if available.
    
    Args:
        endpoint: Endpoint identifier (e.g., "metrics", "accounts")
        cache_manager: RedisCacheManager instance
    
    Returns:
        Cached response or None if miss/error
    """
    return cache_manager.get(endpoint)


def set_cache_for_endpoint(endpoint: str, response_data: dict, 
                          cache_manager: Any) -> bool:  # type: ignore[no-any-return]
    """
    Set cached response for endpoint.
    
    Args:
        endpoint: Endpoint identifier
        response_data: Response data to cache
        cache_manager: RedisCacheManager instance
    
    Returns:
        True if cached successfully
    """
    return cache_manager.set(endpoint, response_data=response_data)

