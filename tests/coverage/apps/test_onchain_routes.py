import asyncio
from unittest.mock import MagicMock

from trading_system.apps.api import onchain_routes as oc


def run(coro):
    return asyncio.run(coro)


def test_health_check_mode_set(monkeypatch):
    settings = MagicMock()
    settings.onchain_mode = "live"
    monkeypatch.setattr(oc.Settings, "from_env", staticmethod(lambda: settings))
    out = run(oc.health_check())
    assert out["status"] == "initialized"
    assert out["mode"] == "live"


def test_health_check_mode_default(monkeypatch):
    settings = MagicMock()
    settings.onchain_mode = None
    monkeypatch.setattr(oc.Settings, "from_env", staticmethod(lambda: settings))
    out = run(oc.health_check())
    assert out["mode"] == "paper"


def test_poll_network():
    assert run(oc.poll_network("ethereum", MagicMock()))["status"] == "pending"


def test_get_token_metadata():
    out = run(oc.get_token_metadata("0xabc"))
    assert out["address"] == "0xabc"


def test_refresh_token_metadata():
    out = run(oc.refresh_token_metadata("0xabc"))
    assert out["status"] == "fetching"


def test_get_events():
    assert run(oc.get_events("ethereum"))["network"] == "ethereum"


def test_get_feed_health():
    assert run(oc.get_feed_health())["status"] == "online"
