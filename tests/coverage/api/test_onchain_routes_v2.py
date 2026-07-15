import asyncio

from unittest.mock import AsyncMock, MagicMock

import sqlalchemy

from trading_system.apps.api import onchain_routes_v2


def run(coro):
    return asyncio.run(coro)


def _mock_select():
    sel = MagicMock()
    sel.return_value.where.return_value = "QUERY"
    return sel


class _PaperSettings:
    onchain_mode = "paper"

    @classmethod
    def from_env(cls):
        return cls()


class _ProdSettings:
    onchain_mode = "production"

    @classmethod
    def from_env(cls):
        return cls()


# Force a deterministic Settings so the tests don't depend on whichever
# `core.config.settings` implementation gets imported.
onchain_routes_v2.Settings = _PaperSettings


def test_health_check_paper_mode():
    db = MagicMock()
    db.execute = AsyncMock()
    out = run(onchain_routes_v2.health_check(MagicMock(), db))
    assert out["status"] == "initialized"
    assert out["service_healthy"] is True
    db.execute.assert_not_awaited()


def test_health_check_non_paper_mode_executes_db(monkeypatch):
    monkeypatch.setattr(onchain_routes_v2, "Settings", _ProdSettings)
    db = MagicMock()
    db.execute = AsyncMock(return_value=MagicMock())
    out = run(onchain_routes_v2.health_check(MagicMock(), db))
    assert out["service_healthy"] is True
    db.execute.assert_awaited_once()


def test_health_check_exception(monkeypatch):
    monkeypatch.setattr(onchain_routes_v2, "Settings", _ProdSettings)
    db = MagicMock()
    db.execute = AsyncMock(side_effect=RuntimeError("db down"))
    out = run(onchain_routes_v2.health_check(MagicMock(), db))
    assert out["status"] == "error"
    assert out["service_healthy"] is False


def test_poll_network_paper():
    db = MagicMock()
    out = run(onchain_routes_v2.poll_network("ethereum", db))
    assert out["network"] == "ethereum"
    assert out["status"] == "pending"
    assert out["mode"] == "paper"


def test_poll_network_non_paper_logs(monkeypatch):
    monkeypatch.setattr(onchain_routes_v2, "Settings", _ProdSettings)
    db = MagicMock()
    out = run(onchain_routes_v2.poll_network("ethereum", db))
    assert out["mode"] == "production"


def test_poll_network_exception(monkeypatch):
    monkeypatch.setattr(
        onchain_routes_v2, "Settings",
        type("Broken", (), {"from_env": classmethod(lambda cls: (_ for _ in ()).throw(RuntimeError("cfg boom")))}),
    )
    db = MagicMock()
    try:
        run(onchain_routes_v2.poll_network("ethereum", db))
        assert False, "expected HTTPException"
    except Exception as e:
        assert e.status_code == 500


def test_get_token_metadata(monkeypatch):
    monkeypatch.setattr(sqlalchemy, "select", _mock_select())
    db = MagicMock()
    db.execute = AsyncMock(return_value=MagicMock())
    out = run(onchain_routes_v2.get_token_metadata("0xABC", db))
    assert out["address"] == "0xABC"
    assert out["cached"] is False
    db.execute.assert_awaited_once()


def test_get_token_metadata_exception(monkeypatch):
    monkeypatch.setattr(sqlalchemy, "select", _mock_select())
    db = MagicMock()
    db.execute = AsyncMock(side_effect=RuntimeError("boom"))
    try:
        run(onchain_routes_v2.get_token_metadata("0xABC", db))
        assert False, "expected HTTPException"
    except Exception as e:
        assert e.status_code == 500


def test_refresh_token_metadata_paper():
    db = MagicMock()
    out = run(onchain_routes_v2.refresh_token_metadata("0xABC", db))
    assert out["status"] == "fetching"


def test_refresh_token_metadata_non_paper(monkeypatch):
    monkeypatch.setattr(onchain_routes_v2, "Settings", _ProdSettings)
    db = MagicMock()
    out = run(onchain_routes_v2.refresh_token_metadata("0xABC", db))
    assert out["mode"] == "production"


def test_refresh_token_metadata_exception(monkeypatch):
    monkeypatch.setattr(
        onchain_routes_v2, "Settings",
        type("Broken", (), {"from_env": classmethod(lambda cls: (_ for _ in ()).throw(RuntimeError("x")))}),
    )
    db = MagicMock()
    try:
        run(onchain_routes_v2.refresh_token_metadata("0xABC", db))
        assert False
    except Exception as e:
        assert e.status_code == 500


def test_get_events(monkeypatch):
    monkeypatch.setattr(sqlalchemy, "select", _mock_select())
    db = MagicMock()
    db.execute = AsyncMock(return_value=MagicMock())
    out = run(onchain_routes_v2.get_events("ethereum", db))
    assert out["network"] == "ethereum"
    assert out["events_count"] == 0
    db.execute.assert_awaited_once()


def test_get_events_exception(monkeypatch):
    monkeypatch.setattr(sqlalchemy, "select", _mock_select())
    db = MagicMock()
    db.execute = AsyncMock(side_effect=RuntimeError("boom"))
    try:
        run(onchain_routes_v2.get_events("ethereum", db))
        assert False
    except Exception as e:
        assert e.status_code == 500


def test_get_feed_health():
    out = run(onchain_routes_v2.get_feed_health(network="ethereum"))
    assert out["status"] == "online"
    assert out["network"] == "ethereum"
    assert out["feed_health_records_query_ready"] is True


def test_get_feed_health_exception(monkeypatch):
    monkeypatch.setattr(
        onchain_routes_v2, "Settings",
        type("Broken", (), {"from_env": classmethod(lambda cls: (_ for _ in ()).throw(RuntimeError("x")))}),
    )
    try:
        run(onchain_routes_v2.get_feed_health())
        assert False
    except Exception as e:
        assert e.status_code == 500
