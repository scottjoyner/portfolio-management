"""Tests for trading_system.api.main (FastAPI app, >=90% line+branch)."""

import asyncio

import pytest
from fastapi.testclient import TestClient

import trading_system.api.routes as routes_mod
import trading_system.api.main as m


class MappingRow:
    def __init__(self, data):
        self._mapping = data

    def __getattr__(self, k):
        return self._mapping.get(k)


class FakeResult:
    def __init__(self, rows, mapping=True):
        self._rows = rows
        self._mapping = mapping

    def fetchall(self):
        return [MappingRow(r) if self._mapping else SimpleNs(r) for r in self._rows]

    def fetchone(self):
        return MappingRow(self._rows[0]) if self._rows else None


class SimpleNs:
    def __init__(self, data):
        for k, v in data.items():
            setattr(self, k, v)


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(routes_mod, "_exec", lambda q, p=None: FakeResult([{"x": 1}]))
    with TestClient(m.app) as c:
        yield c


def test_root(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "endpoints_available" in r.json()


def test_health(client):
    assert client.get("/health").status_code == 200


def test_metrics(client):
    assert client.get("/metrics").status_code == 200


def test_accounts(client):
    assert client.get("/accounts").status_code == 200


def test_trades(client):
    assert client.get("/trades").status_code == 200


def test_positions(client):
    assert client.get("/positions").status_code == 200


def test_strategies(client):
    assert client.get("/strategies").status_code == 200


def test_performance(client):
    assert client.get("/performance").status_code == 200


def test_price_estimates_post(client):
    r = client.post("/evaluations/price/BTC-USD")
    assert r.status_code == 200


def test_approvals(client):
    assert client.get("/approvals").status_code == 200


def test_research_hypotheses(client):
    assert client.get("/research/hypotheses").status_code == 200


def test_market_regime(client):
    assert client.get("/market/regime").status_code == 200


def test_backtests(client):
    assert client.get("/backtests").status_code == 200


def test_capital_allocation(client):
    assert client.get("/capital/allocation").status_code == 200


def test_mocks_load(client):
    r = client.get("/api/mocks/load")
    assert r.status_code == 200
    assert r.json()["status"] == "mock_data_loaded"


def test_api_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"
