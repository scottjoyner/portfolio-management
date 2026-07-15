import os
from unittest.mock import MagicMock

import pytest

from trading_system.apps.api import main


def test_env_bool(monkeypatch):
    monkeypatch.delenv("MY_TF", raising=False)
    assert main._env_bool("MY_TF", True) is True
    assert main._env_bool("MY_TF", False) is False
    monkeypatch.setenv("MY_TF", "true")
    assert main._env_bool("MY_TF", False) is True
    monkeypatch.setenv("MY_TF", "1")
    assert main._env_bool("MY_TF", False) is True
    monkeypatch.setenv("MY_TF", "yes")
    assert main._env_bool("MY_TF", False) is True
    monkeypatch.setenv("MY_TF", "y")
    assert main._env_bool("MY_TF", False) is True
    monkeypatch.setenv("MY_TF", "on")
    assert main._env_bool("MY_TF", False) is True
    monkeypatch.setenv("MY_TF", "0")
    assert main._env_bool("MY_TF", True) is False
    monkeypatch.setenv("MY_TF", "false")
    assert main._env_bool("MY_TF", True) is False


def test_health_ready_mode():
    assert main.health() == {"status": "ok"}
    assert main.ready() == {"status": "ok", "database": "not_required_for_read_only_api"}
    assert main.mode()["mode"] == os.getenv("TRADING_MODE", "paper").lower()


def test_current_mode_override(monkeypatch):
    monkeypatch.setenv("TRADING_MODE", "live")
    assert main.current_mode() == "live"
    monkeypatch.delenv("TRADING_MODE", raising=False)
    assert main.current_mode() == "paper"


def test_helper_factories():
    # Exercises the real get_coinbase_service / get_event_recorder definitions.
    svc = main.get_coinbase_service()
    assert hasattr(svc, "get_connection_status")
    rec = main.get_event_recorder()
    assert hasattr(rec, "path")


def test_runtime_status_connected(monkeypatch):
    svc = MagicMock()
    svc.get_connection_status.return_value = {"connected": True, "error": "boom"}
    rec = MagicMock()
    rec.path.exists.return_value = True
    monkeypatch.setattr(main, "CoinbaseService", lambda *a, **k: svc)
    monkeypatch.setattr(main, "EventRecorder", lambda *a, **k: rec)
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("WORKER_STATUS", "healthy")
    data = main.runtime_status()
    assert data["coinbase_connected"] is True
    assert data["coinbase_error"] == "boom"
    assert data["live_trading_enabled"] is True
    assert data["worker_status"] == "healthy"
    assert data["event_log_status"] == "available"


def test_runtime_status_disconnected(monkeypatch):
    svc = MagicMock()
    svc.get_connection_status.return_value = {"connected": False, "error": None}
    rec = MagicMock()
    rec.path.exists.return_value = False
    monkeypatch.setattr(main, "CoinbaseService", lambda *a, **k: svc)
    monkeypatch.setattr(main, "EventRecorder", lambda *a, **k: rec)
    data = main.runtime_status()
    assert data["coinbase_connected"] is False
    assert data["event_log_status"] == "empty"
    assert data["coinbase_error"] is None


def test_coinbase_status(monkeypatch):
    svc = MagicMock()
    svc.get_connection_status.return_value = {"connected": True}
    monkeypatch.setattr(main, "CoinbaseService", lambda *a, **k: svc)
    assert main.coinbase_status() == {"connected": True}


def test_coinbase_balances_ok(monkeypatch):
    svc = MagicMock()
    snap = MagicMock()
    snap.to_dict.return_value = {"cash": 1}
    svc.get_balances_snapshot.return_value = snap
    monkeypatch.setattr(main, "CoinbaseService", lambda *a, **k: svc)
    assert main.coinbase_balances() == {"cash": 1}


def test_coinbase_balances_error(monkeypatch):
    svc = MagicMock()
    svc.get_balances_snapshot.side_effect = RuntimeError("down")
    monkeypatch.setattr(main, "CoinbaseService", lambda *a, **k: svc)
    with pytest.raises(Exception):
        main.coinbase_balances()


def test_coinbase_price_ok(monkeypatch):
    svc = MagicMock()
    svc.get_price.return_value = {"price": 123.0}
    monkeypatch.setattr(main, "CoinbaseService", lambda *a, **k: svc)
    assert main.coinbase_price("btc-usd") == {"price": 123.0}


def test_coinbase_price_error(monkeypatch):
    svc = MagicMock()
    svc.get_price.side_effect = ValueError("nope")
    monkeypatch.setattr(main, "CoinbaseService", lambda *a, **k: svc)
    with pytest.raises(Exception):
        main.coinbase_price("x")


def test_strategy_catalog_no_filter(monkeypatch):
    def fake(category=None):
        return [
            {"name": "a", "category": "trend", "status": "ok"},
            {"name": "b", "category": "momentum", "status": "ok"},
        ]

    monkeypatch.setattr(main, "list_all_phase1_strategies", fake)
    out = main.strategy_catalog()
    assert out["count"] == 2


def test_strategy_catalog_with_filter(monkeypatch):
    def fake(category=None):
        all_s = [
            {"name": "a", "category": "trend"},
            {"name": "b", "category": "momentum"},
        ]
        return [s for s in all_s if s["category"] == category] if category else all_s

    monkeypatch.setattr(main, "list_all_phase1_strategies", fake)
    out = main.strategy_catalog(category="trend")
    assert out["count"] == 1
    assert out["strategies"][0]["name"] == "a"


def test_strategies_status(monkeypatch):
    def fake(category=None):
        return [{"name": "a", "category": "trend", "status": "running"}]

    monkeypatch.setattr(main, "list_all_phase1_strategies", fake)
    out = main.strategies_status()
    assert out["count"] == 1
    s = out["strategies"][0]
    assert s["strategy_id"] == "a"
    assert s["enabled"] is False
    assert s["mode"] == "paper"


def test_strategy_detail_found(monkeypatch):
    def fake(category=None):
        return [{"name": "a", "category": "trend", "status": "ok"}]

    monkeypatch.setattr(main, "list_all_phase1_strategies", fake)
    assert main.strategy_detail("a")["name"] == "a"


def test_strategy_detail_found_second_iteration(monkeypatch):
    def fake(category=None):
        return [
            {"name": "x", "category": "trend"},
            {"name": "a", "category": "trend"},
        ]

    monkeypatch.setattr(main, "list_all_phase1_strategies", fake)
    assert main.strategy_detail("a")["name"] == "a"


def test_strategy_detail_not_found(monkeypatch):
    monkeypatch.setattr(main, "list_all_phase1_strategies", lambda category=None: [])
    with pytest.raises(Exception):
        main.strategy_detail("missing")


def test_events(monkeypatch):
    rec = MagicMock()
    rec.tail.return_value = [{"a": 1}, {"b": 2}]
    monkeypatch.setattr(main, "EventRecorder", lambda *a, **k: rec)
    out = main.events(limit=10, strategy_id="s", source="src", event_type="e")
    assert out["count"] == 2
    rec.tail.assert_called_once_with(
        limit=10, strategy_id="s", source="src", event_type="e"
    )
