import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from _helpers import install_fakes


class _FeedHealthRecord:
    __table__ = MagicMock()
    address = MagicMock()
    network = MagicMock()


install_fakes({
    "storage.postgres.models": {"FeedHealthRecord": _FeedHealthRecord},
})


@pytest.fixture(autouse=True)
def _patch_select(monkeypatch):
    import sqlalchemy
    monkeypatch.setattr(sqlalchemy, "select", lambda *a, **k: MagicMock())


from trading_system.apps.api import onchain_routes_v2 as oc2


def run(coro):
    return asyncio.run(coro)


def _settings(mode):
    s = MagicMock()
    s.onchain_mode = mode
    return s


def _raise():
    raise RuntimeError("x")


def test_health_paper(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _settings("paper")))
    db = AsyncMock()
    out = run(oc2.health_check(MagicMock(), db))
    assert out["status"] == "initialized"
    assert out["service_healthy"] is True
    db.execute.assert_not_called()


def test_health_nonpaper(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _settings("live")))
    db = AsyncMock()
    out = run(oc2.health_check(MagicMock(), db))
    assert out["service_healthy"] is True
    db.execute.assert_called_once()


def test_health_error(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _settings("live")))
    db = AsyncMock()
    db.execute.side_effect = RuntimeError("db down")
    out = run(oc2.health_check(MagicMock(), db))
    assert out["status"] == "error"
    assert out["service_healthy"] is False


def test_poll_network_paper(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _settings("paper")))
    out = run(oc2.poll_network("eth", AsyncMock()))
    assert out["status"] == "pending"


def test_poll_network_nonpaper(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _settings("live")))
    out = run(oc2.poll_network("eth", AsyncMock()))
    assert out["mode"] == "live"


def test_poll_network_error(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _raise()))
    with pytest.raises(HTTPException):
        run(oc2.poll_network("eth", AsyncMock()))


def test_get_token_metadata(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _settings("paper")))
    out = run(oc2.get_token_metadata("0x1", AsyncMock()))
    assert out["address"] == "0x1"


def test_get_token_metadata_error(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _raise()))
    with pytest.raises(HTTPException):
        run(oc2.get_token_metadata("0x1", AsyncMock()))


def test_refresh_token_metadata(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _settings("paper")))
    out = run(oc2.refresh_token_metadata("0x1", AsyncMock()))
    assert out["status"] == "fetching"


def test_refresh_token_metadata_error(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _raise()))
    with pytest.raises(HTTPException):
        run(oc2.refresh_token_metadata("0x1", AsyncMock()))


def test_refresh_token_metadata_nonpaper(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _settings("live")))
    out = run(oc2.refresh_token_metadata("0x1", AsyncMock()))
    assert out["status"] == "fetching"


def test_get_events(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _settings("paper")))
    out = run(oc2.get_events("eth", AsyncMock()))
    assert out["network"] == "eth"


def test_get_events_error(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _raise()))
    with pytest.raises(HTTPException):
        run(oc2.get_events("eth", AsyncMock()))


def test_get_feed_health(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _settings("paper")))
    out = run(oc2.get_feed_health())
    assert out["status"] == "online"


def test_get_feed_health_error(monkeypatch):
    monkeypatch.setattr(oc2.Settings, "from_env", staticmethod(lambda: _raise()))
    with pytest.raises(HTTPException):
        run(oc2.get_feed_health())
