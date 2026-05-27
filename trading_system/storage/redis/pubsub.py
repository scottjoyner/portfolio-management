"""Redis-backed pub/sub for high-throughput messaging."""

from __future__ import annotations

import json
import logging
from typing import Any, Callable

from redis import Redis

log = logging.getLogger(__name__)


class RedisPubSub:
    """
    High-throughput pub/sub using Redis as message broker.
    
    Provides topic-based messaging with publish/subscribe semantics for
    distributed worker coordination and event streaming.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379") -> None:
        self.redis = Redis.from_url(redis_url)
        self._subscriptions: list[tuple[str, Callable]] = []
        self._running = False

    async def publish(self, channel: str, message: dict | str) -> int:
        """Publish message to channel. Returns number of subscribers."""
        try:
            serialized = json.dumps(message).encode() if isinstance(message, dict) else message
            return await self.redis.publish(channel, serialized)
        except Exception as e:
            log.exception("publish failed: %s", e)
            raise

    async def subscribe(self, channel: str, handler: Callable[[dict], None]) -> None:
        """Subscribe to channel with handler."""
        # Register in-memory handler for now (would use Redis pubsub in production)
        self._subscriptions.append((channel, handler))
        log.info("subscribed to channel %s", channel)

    async def broadcast(self, topic: str, event: dict) -> None:
        """Broadcast event to all subscribers on topic."""
        await self.publish(topic, event)

    async def publish_event(
        self,
        topic: str,
        event_type: str,
        payload: dict,
    ) -> None:
        """Publish typed event with metadata."""
        message = {
            "event_type": event_type,
            "topic": topic,
            "payload": payload,
            "timestamp": time.time(),
        }
        await self.publish(topic, message)
