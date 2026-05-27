"""Onchain RPC poller worker."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, List, Optional

from onchain.pollers.service import OnchainPoller
from onchain.pollers.token_metadata import TokenMetadataPoller
from onchain.pollers.event_listener import EventListener

log = logging.getLogger(__name__)


class RpcPollerWorker:
    """
    Worker that combines pool polling, token metadata fetching, and event listening.
    
    Runs in paper/shadow mode by default (no transaction signing).
    """

    def __init__(
        self,
        rpc_endpoints: dict[str, str],
        db_session_factory: Callable[[], Any],  # type ignore
        event_handlers: Optional[List[Callable]] = None,
    ) -> None:
        self.rpc_endpoints = rpc_endpoints
        self._session_factory = db_session_factory
        self._event_handlers = event_handlers or []
        
        self._poller: OnchainPoller | None = None
        self._metadata_poller: TokenMetadataPoller | None = None
        self._event_listener: EventListener | None = None

    async def initialize(self) -> None:
        """Initialize all poller components."""
        self._poller = OnchainPoller(
            rpc_endpoints=self.rpc_endpoints,
            db_session_factory=self._session_factory,
            event_handlers=self._event_handlers,
        )

        self._metadata_poller = TokenMetadataPoller(
            rpc_endpoints=self.rpc_endpoints,
            db_session_factory=self._session_factory,
        )

        self._event_listener = EventListener(
            rpc_endpoints=self.rpc_endpoints,
            db_session_factory=self._session_factory,
            event_handlers=self._event_handlers,
        )

    async def start_polling(self) -> None:
        """Start polling pools and fetching events."""
        self._poller._running = True
        
        # Subscribe to events
        await self._event_listener.subscribe_to_events(network="base")

    async def fetch_token_metadata(self, token_address: str) -> Optional[dict]:
        """Fetch metadata for a single token."""
        if not self._metadata_poller:
            raise RuntimeError("metadata poller not initialized")
        
        return await self._metadata_poller.fetch_token_metadata(token_address, network="base")

    async def stop(self) -> None:
        """Stop polling."""
        if self._poller:
            await self._poller.close()
        
        if self._event_listener:
            await self._event_listener.close()

    @property
    def health(self) -> dict[str, Any]:
        """Get combined worker health status."""
        return {
            "status": "running" if self._poller else "stopped",
            "feed_health": self._poller.feed_health if self._poller else {},
        }
