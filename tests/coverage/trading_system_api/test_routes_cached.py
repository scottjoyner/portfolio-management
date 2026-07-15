"""Tests for trading_system.api.routes_cached (>=90% line+branch)."""

import asyncio
import sys
import types

import pytest

import trading_system.api.routes_cached as m


class MockCache:
    def __init__(self, store=None):
        self.store = dict(store or {})
        self.set_calls = []

    def get(self, key):
        return self.store.get(key)

    def set(self, key, response_data=None, **kwargs):
        self.set_calls.append((key, response_data))
        self.store[key] = response_data


def run(coro):
    return asyncio.run(coro)


def test_health_check():
    r = run(m.health_check())
    assert r["status"] == "healthy"
    assert r["components"]["database"] is True


def test_get_metrics_no_cache():
    r = run(m.get_metrics())
    assert "metrics" in r
    assert "redis" in r["metrics"]


def test_get_metrics_cache_hit():
    cached = {"metrics": {"redis": {}}}
    r = run(m.get_metrics(cache_manager=MockCache({"metrics": cached})))
    assert r["cache_status"] == "hit"
    assert r["cached_at"]


def test_get_metrics_cache_miss():
    cm = MockCache({})
    r = run(m.get_metrics(cache_manager=cm))
    assert r["metrics"]["redis"]
    assert cm.set_calls  # wrote through to cache


def test_list_accounts_no_cache():
    r = run(m.list_accounts())
    assert r["total_accounts"] == 0


def test_list_accounts_cache_hit():
    cached = {"accounts": [{"id": "a"}]}
    r = run(m.list_accounts(cache_manager=MockCache({"accounts": cached})))
    assert r["cache_status"] == "hit"


def test_list_accounts_cache_miss():
    cm = MockCache({})
    r = run(m.list_accounts(cache_manager=cm))
    assert r["cache_status"] == "miss"
    assert cm.set_calls


def test_list_trades_no_cache():
    r = run(m.list_trades())
    assert r["trades"] == []
    assert r["total_trades"] == 0


def test_list_trades_cache_hit():
    cached = {"trades": [{"id": 1}]}
    r = run(m.list_trades(cache_manager=MockCache({"trades": cached})))
    assert r["trades"] == [{"id": 1}]


def test_list_trades_cache_miss():
    cm = MockCache({})
    r = run(m.list_trades(limit=10, offset=5, cache_manager=cm))
    assert r["limit"] == 10
    assert r["offset"] == 5


def test_list_positions_no_cache():
    r = run(m.list_positions())
    assert r["positions"] == []


def test_list_positions_cache_hit():
    cached = {"positions": [{"x": 1}]}
    r = run(m.list_positions(cache_manager=MockCache({"positions": cached})))
    assert r["positions"] == [{"x": 1}]


def test_list_positions_cache_miss():
    cm = MockCache({})
    r = run(m.list_positions(portfolio_id="p1", cache_manager=cm))
    assert r["total_positions"] == 0


def test_list_strategies_no_cache():
    r = run(m.list_strategies())
    assert r["strategies"] == []


def test_list_strategies_cache_hit():
    cached = {"strategies": [{"s": 1}]}
    r = run(m.list_strategies(cache_manager=MockCache({"strategies": cached})))
    assert r["strategies"] == [{"s": 1}]


def test_list_strategies_cache_miss():
    cm = MockCache({})
    r = run(m.list_strategies(cache_manager=cm))
    assert r["total_strategies"] == 0


def test_get_performance_no_cache():
    r = run(m.get_performance())
    assert r["portfolio_performance"] == {}


def test_get_performance_cache_hit():
    cached = {"portfolio_performance": {"nav": 1}}
    r = run(m.get_performance(cache_manager=MockCache({"performance": cached})))
    assert r["portfolio_performance"] == {"nav": 1}


def test_get_performance_cache_miss():
    cm = MockCache({})
    r = run(m.get_performance(cache_manager=cm))
    assert r["risk_metrics"] == {}


def test_get_price_estimations():
    r = run(m.get_price_estimations("BTC-USD"))
    assert r["instrument"] == "BTC-USD"
    assert r["confidence_score"] is None


def test_get_approvals_no_cache():
    r = run(m.get_approvals())
    assert r["approvals"] == []


def test_get_approvals_cache_hit():
    cached = {"approvals": [{"id": 1}]}
    r = run(m.get_approvals(cache_manager=MockCache({"approvals": cached})))
    assert r["approvals"] == [{"id": 1}]


def test_get_approvals_cache_miss():
    cm = MockCache({})
    r = run(m.get_approvals(cache_manager=cm))
    assert r["pending_count"] == 0


def test_get_research_hypotheses():
    r = run(m.get_research_hypotheses())
    assert r["hypotheses"] == []


async def _sample_endpoint(*args, **kwargs):
    return {"ok": True}


async def _sample_endpoint_nondict(*args, **kwargs):
    return [1, 2, 3]


def test_endpoint_wrapper_dict():
    wrapped = m.endpoint_wrapper(_sample_endpoint)
    r = run(wrapped())
    assert r["ok"] is True
    assert "timestamp" in r


def test_endpoint_wrapper_nondict():
    wrapped = m.endpoint_wrapper(_sample_endpoint_nondict)
    r = run(wrapped())
    assert isinstance(r, list)
