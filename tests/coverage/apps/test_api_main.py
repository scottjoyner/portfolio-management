import os
from unittest.mock import MagicMock

import pytest

from trading_system.apps.api import main


@pytest.fixture
def coinbase_service(monkeypatch):
    svc = MagicMock()
    svc.get_connection_status.return_value = {"connected": True, "error": None}
    snap = MagicMock()
    snap.to_dict.return_value = {"cash": 1}
    svc.get_balances_snapshot.return_value = snap
    svc.get_price.return_value = {"price": 100}
    monkeypatch.setattr(main, "CoinbaseService", lambda *a, **k: svc)
    return svc


@pytest.fixture
def event_recorder(monkeypatch):
    rec = MagicMock()
    rec.path.exists.return_value = True
    rec.tail.return_value = [{"a": 1}]
    monkeypatch.setattr(main, "EventRecorder", lambda *a, **k: rec)
    return rec


@pytest.fixture
def strategies(monkeypatch):
    def fake_list(category=None):
        return [
            {"name": "s1", "category": "trend", "status": "ok"},
            {"name": "s2", "category": "momentum", "status": "ok"},
        ]
    monkeypatch.setattr(main, "list_all_phase1_strategies", fake_list)
    return fake_list


def test_health_ready_mode():
    assert main.health() == {"status": "ok"}
    assert main.ready()["status"] == "ok"
    assert main.mode()["mode"] == os.getenv("TRADING_MODE", "paper").lower()


def test_env_bool(monkeypatch):
    monkeypatch.delenv("MY_TEST_FLAG", raising=False)
    assert main._env_bool("MY_TEST_FLAG", True) is True
    assert main._env_bool("MY_TEST_FLAG", False) is False
    monkeypatch.setenv("MY_TEST_FLAG", "true")
    assert main._env_bool("MY_TEST_FLAG", False) is True
    monkeypatch.setenv("MY_TEST_FLAG", "0")
    assert main._env_bool("MY_TEST_FLAG", True) is False


def test_runtime_status(coinbase_service, event_recorder, monkeypatch):
    class FakeStatus:
        def __init__(self, **kw):
            self._d = kw
        def to_dict(self):
            return self._d
    monkeypatch.setattr(main, "RuntimeStatus", FakeStatus)
    monkeypatch.setenv("WORKER_STATUS", "up")
    out = main.runtime_status()
    assert out["coinbase_connected"] is True
    assert out["coinbase_error"] is None


def test_coinbase_status(coinbase_service):
    assert main.coinbase_status() == {"connected": True, "error": None}


def test_coinbase_balances(coinbase_service):
    assert main.coinbase_balances() == {"cash": 1}


def test_coinbase_balances_error(coinbase_service):
    coinbase_service.get_balances_snapshot.side_effect = Exception("boom")
    with pytest.raises(main.HTTPException) as exc:
        main.coinbase_balances()
    assert exc.value.status_code == 503


def test_coinbase_price(coinbase_service):
    assert main.coinbase_price("btc-usd") == {"price": 100}


def test_coinbase_price_error(coinbase_service):
    coinbase_service.get_price.side_effect = RuntimeError("x")
    with pytest.raises(main.HTTPException) as exc:
        main.coinbase_price("btc-usd")
    assert exc.value.status_code == 503


def test_strategy_catalog_all(strategies):
    out = main.strategy_catalog()
    assert out["count"] == 2


def test_strategy_catalog_filtered(strategies):
    out = main.strategy_catalog(category="trend")
    assert out["count"] == 1
    assert out["strategies"][0]["name"] == "s1"


def test_strategies_status(strategies):
    out = main.strategies_status()
    assert out["count"] == 2
    assert out["strategies"][0]["enabled"] is False


def test_strategy_detail_found(strategies):
    assert main.strategy_detail("s1")["name"] == "s1"


def test_strategy_detail_not_found(strategies):
    with pytest.raises(Exception):
        main.strategy_detail("nope")


def test_events(event_recorder):
    out = main.events(limit=10)
    assert out["count"] == 1
    event_recorder.tail.assert_called_once()
