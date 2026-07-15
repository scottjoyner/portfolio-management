import asyncio

from unittest.mock import AsyncMock, MagicMock

from trading_system.apps.api import onchain_routes


def run(coro):
    return asyncio.run(coro)


def test_health_check():
    out = run(onchain_routes.health_check())
    assert out["status"] == "initialized"
    assert "mode" in out


def test_poll_network():
    db = MagicMock()
    out = run(onchain_routes.poll_network("ethereum", db))
    assert out["network"] == "ethereum"
    assert out["status"] == "pending"


def test_get_token_metadata():
    out = run(onchain_routes.get_token_metadata("0xABC"))
    assert out["address"] == "0xABC"
    assert out["symbol"] is None


def test_refresh_token_metadata():
    out = run(onchain_routes.refresh_token_metadata("0xABC"))
    assert out["address"] == "0xABC"
    assert out["status"] == "fetching"


def test_get_events():
    out = run(onchain_routes.get_events("ethereum"))
    assert out["network"] == "ethereum"
    assert out["events"] == []


def test_get_feed_health():
    out = run(onchain_routes.get_feed_health())
    assert out["status"] == "online"
    assert out["last_poll"] is None
