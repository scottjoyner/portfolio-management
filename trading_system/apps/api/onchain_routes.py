"""Onchain operations API routes."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter
from sqlalchemy.ext.asyncio import AsyncSession

from core.config.settings import Settings

log = logging.getLogger(__name__)

router = APIRouter(prefix="/onchain", tags=["onchain"])


@router.get("/health")
async def health_check() -> dict[str, Any]:
    """Check onchain integration status."""
    # Would initialize worker if not already initialized
    return {
        "status": "initialized",
        "mode": Settings.from_env().onchain_mode or "paper",
    }


@router.post("/poll/{network}")
async def poll_network(network: str, db_session: AsyncSession) -> dict[str, Any]:
    """Trigger manual pool polling for specified network."""
    # This would initialize the worker if needed and call poll_pools()
    return {
        "network": network,
        "status": "pending",  # Would actually poll in real implementation
    }


@router.get("/tokens/{token_address}")
async def get_token_metadata(token_address: str) -> dict[str, Any]:
    """Get cached or freshly fetched token metadata."""
    return {
        "address": token_address,
        "symbol": None,  # Would fetch from chain/cache
        "name": None,
        "decimals": None,
    }


@router.post("/tokens/{token_address}/refresh")
async def refresh_token_metadata(token_address: str) -> dict[str, Any]:
    """Force refresh token metadata."""
    return {
        "address": token_address,
        "status": "fetching",  # Would actually fetch from chain
    }


@router.get("/events/{network}")
async def get_events(network: str) -> dict[str, Any]:
    """Get recent onchain events for specified network."""
    return {
        "network": network,
        "events": [],  # Would query database for stored events
    }


@router.get("/feed/health")
async def get_feed_health() -> dict[str, Any]:
    """Get feed health status."""
    return {
        "status": "online",
        "last_poll": None,
    }
