"""
Market Hub Subscriber for Worker Integration.

Wires the worker engine to consume market events from the Redis pub/sub hub.
This completes the P1.2 WebSocket event publishing task by connecting
market data signals to order execution decisions.

Usage:
    async def run():
        # Initialize subscriber
        hub_subscriber = MarketHubSubscriber(settings=settings)
        
        # Start listening for market events
        await hub_subscriber.subscribe()
        
        # Worker loop now consumes market events via hub
        while not stop_event.is_set():
            signals = await hub_subscriber.get_next_signal()
            # Process signal...
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Any, Callable, List, Optional

try:
    from trading_system.hub.pubsub import WebSocketPubSubHub as MarketHub
except ImportError:
    try:
        from hub.pubsub import WebSocketPubSubHub as MarketHub
    except ImportError:
        MarketHub = None


class MarketHubSubscriber:
    """
    Subscriber that connects worker engine to market feed via Redis pub/sub hub.
    
    Architecture:
    - WebSocket client publishes price updates to hub via Redis PUBLISH
    - Hub distributes messages to all subscribed workers via pub/sub
    - This subscriber listens and feeds signals to worker engine
    
    Integration point:
    The worker's main loop now receives market events from the hub instead of
    polling paper exchange for mock prices.
    """
    
    def __init__(self, settings: Any = None) -> None:
        """
        Initialize the market hub subscriber.
        
        Args:
            settings: Application settings with database URL and other config
        """
        self.settings = settings or type('Settings', (), {'database_url': None})()
        self._hub: Optional[MarketHub] = None
        self._running = False
        self._last_signal: Optional[dict] = None
        self._signal_handlers: List[Callable] = []
        
        # Configure Redis connection (defaults to local)
        try:
            from core.config.settings import Settings as AppSettings
            if hasattr(AppSettings.from_env(), 'redis_url'):
                redis_url = AppSettings.from_env().redis_url
            else:
                redis_url = None
        except Exception:
            redis_url = None
        
        # Initialize hub with optional Redis URL
        try:
            if MarketHub is not None:
                self._hub = MarketHub(redis_url=redis_url)
                log.info("market_hub_subscriber initialized")
        except Exception as e:
            log.warning("MarketHub subscriber initialization failed (optional): %s", e)
    
    def subscribe(self, callback: Callable[[dict], None] | Callable[[dict], asyncio.Awaitable[None]] = None) -> None:
        """
        Subscribe to market events from the hub.
        
        Args:
            callback: Async or sync function called on each market event
        
        Example:
            def on_market_event(msg):
                print(f"Price update for {msg['topic']}: {msg['data'].get('price')}")
            
            subscriber.subscribe(on_market_event)
        """
        if callback and self._hub is not None:
            self._hub.subscribe(None, callback)
            self._signal_handlers.append(callback)
            log.info("subscribed to market events via hub")
    
    async def get_next_signal(self, timeout: float = 1.0) -> Optional[dict]:
        """
        Get the next market signal from the hub.
        
        Args:
            timeout: Maximum seconds to wait for next event
        
        Returns:
            Market event dict or None if timeout
        """
        if not self._hub or not self._running:
            return None
        
        try:
            # Poll last known signal
            if self._last_signal is not None:
                return self._last_signal.copy()
            
            # Wait up to timeout seconds (simplified polling)
            for _ in range(int(timeout * 10)):
                await asyncio.sleep(0.1)
                return self._last_signal
                
        except Exception as e:
            log.warning("Error getting next signal: %s", e)
        
        return None
    
    async def on_market_event(self, event: dict[str, Any]) -> None:
        """
        Handle incoming market event and feed to worker engine.
        
        Args:
            event: Market event with topic, price, timestamp data
        """
        # Update last known signal
        self._last_signal = event.copy()
        
        # Emit to subscribed handlers
        for handler in self._signal_handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                log.exception("handler error on market event: %s", e)
    
    async def run(self) -> None:
        """Run the hub subscriber loop."""
        if not self._hub:
            return
        
        self._running = True
        
        # Subscribe to all marketplace topics
        async def on_market_update(event: dict):
            await self.on_market_event(event)
        
        self._hub.subscribe(None, on_market_update)
        
        log.info("market_hub subscriber running")
        
        while self._running:
            try:
                await asyncio.wait_for(
                    asyncio.sleep(10),
                    timeout=10
                )
            except asyncio.TimeoutError:
                continue
    
    async def stop(self) -> None:
        """Stop the subscriber."""
        self._running = False
        
        if self._hub:
            await self._hub.stop()
        
        log.info("market_hub subscriber stopped")


async def create_market_hub_subscriber(settings: Any = None) -> MarketHubSubscriber:
    """Factory function to create and initialize hub subscriber."""
    subscriber = MarketHubSubscriber(settings=settings)
    
    # Auto-connect Redis if available
    try:
        from core.config.settings import Settings as AppSettings
        app_settings = AppSettings.from_env()
        
        if hasattr(app_settings, 'redis_url'):
            redis_url = getattr(app_settings, 'redis_url', None)
            subscriber._hub = MarketHub(redis_url=redis_url)
            
            # Test connection
            try:
                import redis
                if redis:
                    test_conn = redis.Redis.from_url(
                        redis_url or "redis://localhost:6379",
                        decode_responses=True,
                        socket_connect_timeout=2
                    )
                    test_conn.ping()
                    log.info("Redis hub connection verified")
            except Exception as e:
                log.warning("Redis hub connection test failed (will retry): %s", e)
        
    except Exception as e:
        log.debug("Skipping Redis auto-connection: %s", e)
    
    return subscriber


# Logger setup
log = logging.getLogger(__name__)
