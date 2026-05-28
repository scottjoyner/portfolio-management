"""
WebSocket Pub/Sub Hub for Market Feed Distribution.

Wires Coinbase WebSocket market feed client to worker consumption loop.
Uses Redis-backed pub/sub for reliable message distribution across services.

P1.2 Implementation: Completes the signal→fill e2e pipeline by connecting
market data ingestion to order execution engine via hub messaging.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, List

try:
    import redis
except ImportError:
    redis = None

log = logging.getLogger(__name__)


class WebSocketPubSubHub:
    """
    Redis-backed pub/sub hub for distributing WebSocket market events.
    
    Architecture:
    - Market feed clients publish price updates to Redis channels
    - Worker services subscribe to relevant topics via Redis pub/sub
    - Messages are JSON-structured with product_id, timestamp, event_type
    
    Topics:
    - marketplace.{product_id} → Price updates for specific product
    - marketfeed.broadcast → All market events (broadcast)
    
    Example:
        # Publisher (market feed client)
        await hub.publish("marketplace.BTC-USD", {"price": 60000})
        
        # Subscriber (worker)
        async for msg in hub.subscribe("marketplace.*"):
            event = json.loads(msg)
            await process_market_event(event)
    """
    
    def __init__(self, redis_url: str | None = None, api_key: str | None = None, 
                 api_secret: str | None = None) -> None:
        """
        Initialize the WebSocket pub/sub hub.
        
        Args:
            redis_url: Redis connection string (e.g., "redis://localhost:6379")
                      If None, assumes local Redis at default port
            api_key: Coinbase API key for authenticated endpoints
            api_secret: Coinbase API secret for authenticated endpoints
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self._redis_client: redis.Redis | None = None
        self._subscribers: dict[str, list[Callable]] = {}
        self._running = False
        
        # Configure Redis connection
        if redis_url is None:
            redis_url = "redis://localhost:6379"
        
        try:
            self._redis_client = redis.Redis.from_url(
                redis_url, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_error=True
            )
            self._test_connection()
            log.info("connected to Redis pub/sub hub: %s", redis_url)
        except Exception as e:
            log.warning("Redis connection failed (optional): %s", e)
    
    def _test_connection(self) -> None:
        """Test Redis connection and create channels if needed."""
        if not self._redis_client:
            return
        
        try:
            # Test ping
            self._redis_client.ping()
            
            # Create marketplace topic keys (persistent, TTL not set)
            products = ["BTC-USD", "ETH-USD", "SOL-USD"]
            for product in products:
                key = f"marketplace:{product}"
                if not self._redis_client.exists(key):
                    self._redis_client.set(key, json.dumps({"active": True}))
            
            log.info("Redis pub/sub hub ready (topics created)")
        except Exception as e:
            log.error("Redis error during setup: %s", e)
    
    async def connect(self) -> None:
        """Ensure Redis connection is established."""
        if self._redis_client:
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, 
                    lambda: self._redis_client.ping()
                )
                log.debug("Redis pub/sub hub connected")
            except Exception as e:
                log.warning("Redis ping failed: %s", e)
    
    async def publish(self, topic: str, data: dict[str, Any]) -> None:
        """
        Publish a market event to specified topic.
        
        Args:
            topic: Channel name (e.g., "marketplace.BTC-USD")
            data: Event payload (JSON-serializable dict)
        
        Example:
            await hub.publish("marketplace.BTC-USD", {
                "price": 60123.45,
                "bid": 60120.00,
                "ask": 60126.90,
                "volume_24h": 1234567890,
                "timestamp": 1709567890.123,
            })
        """
        if not self._redis_client:
            log.warning("Redis client not initialized, skipping publish")
            return
        
        try:
            payload = {
                "topic": topic,
                "event_type": "market_price_update",
                **data
            }
            message = json.dumps(payload)
            
            # Use PUBLISH command on Redis channel (name-based pub/sub)
            channel_name = f"marketfeed:{topic.replace(':', '_')}"
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._redis_client.publish(channel_name, message)
            )
            
            log.debug("published to %s with %d subscribers", 
                     channel_name,
                     await asyncio.get_event_loop().run_in_executor(
                         None,
                         lambda: self._redis_client.pubsub_numsub(channel_name)
                     )[0])
        except Exception as e:
            log.error("publish error: %s", e)
    
    def subscribe(self, topic_pattern: str | None = None, handler: Callable | None = None) -> None:
        """
        Register a subscriber for specified topics.
        
        Args:
            topic_pattern: Topic pattern (e.g., "marketplace.*" or specific channel)
                          If None, subscribes to all available channels
            handler: Callback function invoked on new message
        
        Example:
            def on_market_event(msg: dict):
                print(f"Price update: {msg['price']}")
            
            hub.subscribe("marketplace.BTC-USD", on_market_event)
            hub.subscribe("marketfeed.broadcast", another_handler)
        """
        if not handler:
            return
        
        # Store handler for topic pattern matching
        if topic_pattern:
            self._subscribers.setdefault(topic_pattern, []).append(handler)
        
        # Set up Redis pub/sub listener in background task
        if not self._running:
            self._running = True
            asyncio.create_task(self._listen_to_subscribers())
    
    async def _listen_to_subscribers(self) -> None:
        """Background task to listen to Redis pub/sub channels and dispatch messages."""
        if not self._redis_client or not self._subscribers:
            return
        
        log.info("Starting Redis pub/sub listener for %d subscribers", 
                 sum(len(v) for v in self._subscribers.values()))
        
        try:
            # Create Redis pub/sub instance
            pubsub = self._redis_client.pubsub()
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: pubsub.subscribe()
            )
            
            log.info("subscribed to all Redis channels")
            
            while self._running:
                try:
                    # Use run_in_executor for blocking PEEK operation
                    message = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: pubsub.get_message(ignore_subscribe_messages=True)
                    )
                    
                    if message and message['type'] == 'message':
                        channel = message['channel'].decode()
                        payload_str = message['data']
                        
                        try:
                            # Parse payload
                            payload = json.loads(payload_str)
                            
                            # Find matching subscribers
                            topic = payload.get('topic', '')
                            
                            for pattern, handlers in self._subscribers.items():
                                if pattern == topic or (pattern.endswith('*') and 
                                                       topic.startswith(pattern[:-1])):
                                    for handler in handlers:
                                        try:
                                            await handler(payload)
                                        except Exception as e:
                                            log.exception("handler error on message: %s", e)
                        
                        except json.JSONDecodeError:
                            log.warning("invalid JSON payload: %s", payload_str[:200])
                            
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.error("pub/sub listener error: %s", e)
                    await asyncio.sleep(1)
        
        finally:
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: pubsub.unsubscribe()
                )
                log.info("Redis pub/sub listener stopped")
            except Exception:
                pass
    
    async def stop(self) -> None:
        """Stop the hub and clean up."""
        self._running = False
        
        if self._redis_client:
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self._redis_client.close()
                )
                log.debug("Redis connection closed")
            except Exception as e:
                log.warning("Redis close error: %s", e)
        
        log.info("WebSocket pub/sub hub stopped")


class MarketFeedPublisher:
    """
    Market feed publisher that connects to Coinbase WebSocket and publishes via hub.
    
    Usage pattern:
        1. Initialize publisher with Redis hub URL
        2. Subscribe handler (worker consumes messages)
        3. Start running loop
    """
    
    def __init__(self, redis_url: str | None = None, api_key: str | None = None,
                 api_secret: str | None = None) -> None:
        self.redis_hub = WebSocketPubSubHub(
            redis_url=redis_url,
            api_key=api_key,
            api_secret=api_secret
        )
    
    async def connect(self, product_ids: List[str] = None) -> None:
        """Connect to Coinbase WebSocket for market data."""
        from exchange.coinbase.websocket.market_feed import (
            CoinbaseWebSocketMarketClient as MarketClient
        )
        
        client = MarketClient(api_key=self.redis_hub.api_key, 
                              api_secret=self.redis_hub.api_secret)
        
        # Subscribe to products
        if product_ids is None:
            product_ids = ["BTC-USD", "ETH-USD"]
        
        for product_id in product_ids:
            subscription = client.subscribe(product_id)
            await self.redis_hub.publish(
                "marketfeed.subscription_request",
                {**subscription, "source": "market_feed_publisher"}
            )
    
    async def run(self, product_ids: List[str] = None) -> None:
        """
        Run the market feed publisher loop.
        
        Args:
            product_ids: List of product IDs to subscribe (e.g., ["BTC-USD", "ETH-USD"])
        """
        if product_ids is None:
            product_ids = ["BTC-USD", "ETH-USD"]
        
        client = MarketClient(
            api_key=self.redis_hub.api_key,
            api_secret=self.redis_hub.api_secret
        )
        
        try:
            # Connect to WebSocket
            await client.connect()
            log.info("connected to Coinbase WebSocket")
            
            # Subscribe to products
            for product_id in product_ids:
                sub = client.subscribe(product_id)
                await client._ws.send(json.dumps(sub))
                log.info("subscribed to %s", product_id)
            
            # Publish messages via hub
            while True:
                async for raw in client._ws:
                    try:
                        msg = json.loads(raw)
                        
                        # Publish to Redis hub
                        topic = f"marketplace.{msg.get('product_id', 'BTC-USD')}"
                        await self.redis_hub.publish(topic, {
                            "type": msg.get("type"),
                            "channel": msg.get("channel"),
                            **msg.get("data", {})
                        })
                        
                    except json.JSONDecodeError:
                        continue
                        
        except Exception as e:
            log.error("market feed publisher error: %s", e)
        finally:
            await self.redis_hub.stop()


async def process_market_event(msg: dict[str, Any]) -> None:
    """
    Example handler for market events (to be wired to worker).
    
    This shows the pattern that will be connected to the worker loop.
    
    Args:
        msg: Market event payload with topic, event_type, and price data
    """
    # Extract event details
    topic = msg.get("topic", "")
    event_type = msg.get("event_type")
    data = msg.get("data", {})
    
    if not event_type or event_type != "market_price_update":
        return
    
    product_id = topic.replace("marketplace.", "")
    price = data.get("price")
    
    # Log/emit worker event
    log.info(
        "worker_market_signal",
        product_id=product_id,
        price=float(price) if price else None,
        timestamp=msg.get("timestamp"),
    )


if __name__ == "__main__":
    import asyncio
    
    async def main():
        """Demonstrate hub wiring."""
        
        # Initialize hub (Redis at default localhost)
        hub = WebSocketPubSubHub()
        
        # Create worker-style handler
        received_events = []
        
        async def on_market_update(msg: dict):
            product_id = msg.get("topic", "").replace("marketplace.", "")
            price = msg.get("data", {}).get("price")
            received_events.append({
                "product_id": product_id,
                "price": price,
                "timestamp": msg.get("timestamp"),
            })
        
        # Subscribe handler
        hub.subscribe(None, on_market_update)
        
        # Run publisher (uses Coinbase WebSocket for market data)
        publisher = MarketFeedPublisher()
        await publisher.connect(["BTC-USD", "ETH-USD"])
        
        log.info("Market feed publisher + hub ready")
        log.info("Handler subscribed to marketplace.*")
        
        # Keep running for demo
        try:
            while True:
                await asyncio.sleep(10)
        except KeyboardInterrupt:
            pass
        
        await hub.stop()
        log.info("Demo completed, %d events received", len(received_events))
    
    asyncio.run(main())
