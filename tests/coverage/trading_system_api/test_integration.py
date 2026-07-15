"""Tests for trading_system.api.integration (>=90% line+branch)."""

import asyncio
import builtins
import runpy
import types

import pytest

import trading_system.api.integration as m


def run(coro):
    return asyncio.run(coro)


def patch_runner(monkeypatch):
    orig = builtins.__import__

    def fake(name, *a, **k):
        if name == "trading_system.apps.backtester.runner":
            mod = types.ModuleType(name)
            mod.get_backtest_results_for_strategies = lambda: {}
            return mod
        return orig(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake)


def test_setup_database_routes_ok():
    assert m.setup_database_routes() == {}


def test_setup_database_routes_import_error(monkeypatch):
    orig = builtins.__import__

    def fake(name, *a, **k):
        if name == "trading_system.database.queries.accounts":
            raise ImportError("no")
        return orig(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake)
    assert m.setup_database_routes() == {}


def test_setup_database_routes_success(monkeypatch):
    orig = builtins.__import__

    def fake(name, *a, **k):
        if name == "trading_system.database.queries.accounts":
            mod = types.ModuleType(name)
            mod.get_accounts = lambda: []
            return mod
        if name == "trading_system.database.queries.positions":
            mod = types.ModuleType(name)
            mod.get_positions = lambda: []
            return mod
        if name == "trading_system.database.queries.trades":
            mod = types.ModuleType(name)
            mod.get_trades = lambda: []
            return mod
        return orig(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake)
    assert m.setup_database_routes() == {}


def test_setup_backtest_routes(monkeypatch):
    patch_runner(monkeypatch)
    fn = m.setup_backtest_routes()
    assert callable(fn)


def test_setup_backtest_routes_import_error():
    try:
        m.setup_backtest_routes()
        assert False, "expected ImportError"
    except ImportError:
        pass


def test_setup_research_routes():
    r = m.setup_research_routes()
    assert "get_hypotheses" in r


def test_setup_valuation_routes():
    r = m.setup_valuation_routes()
    assert "calculate_dcf" in r
    result = run(r["calculate_dcf"]("BTC-USD"))
    assert isinstance(result, dict)


def test_setup_all_routes(monkeypatch):
    patch_runner(monkeypatch)
    r = m.setup_all_routes()
    assert "get_backtest_results" in r


def test_get_all_api_endpoints(monkeypatch):
    patch_runner(monkeypatch)
    eps = m.get_all_api_endpoints()
    assert "strategies_with_backtest" in eps
    sb = run(eps["strategies_with_backtest"]())
    assert "strategies" in sb
    val = run(eps["get_valuation"]("BTC-USD"))
    assert val["symbol"] == "BTC-USD"
    rd = run(eps["get_research_data"]("BTC-USD"))
    assert rd["symbol"] == "BTC-USD"


def test_main_block():
    runpy.run_path(m.__file__, run_name="__main__")


def test_strategies_with_backtest_import_error():
    eps = m.get_all_api_endpoints()
    sb = run(eps["strategies_with_backtest"]())
    assert "strategies" in sb
