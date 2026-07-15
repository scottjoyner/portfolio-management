"""Tests for trading_system.api.routes_backtest (>=90% line+branch)."""

import asyncio
import sqlite3 as _real_sqlite3
import sys
import types

import pytest

import trading_system.api.routes_backtest as m


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def shared_db(tmp_path, monkeypatch):
    """Point every sqlite3.connect(':memory:') call at a shared on-disk file."""
    db_file = str(tmp_path / "bt.db")
    # Ensure a fresh empty file
    orig_connect = _real_sqlite3.connect
    conn = orig_connect(db_file)
    conn.close()

    def _connect(*a, **k):
        return orig_connect(db_file)

    monkeypatch.setattr(_real_sqlite3, "connect", _connect)
    return db_file


def test_trigger_backtest_invalid_empty():
    r = run(m.trigger_backtest(""))
    assert r["status"] == "error"
    assert r["error_type"] == "invalid_strategy_id"


def test_trigger_backtest_invalid_too_long():
    r = run(m.trigger_backtest("x" * 129))
    assert r["status"] == "error"


def test_trigger_backtest_success(shared_db):
    r = run(m.trigger_backtest("stratA"))
    assert r["status"] == "success"
    assert "results" in r
    assert r["results"]["strategy_id"] == "stratA"


def test_generate_equity_curve_zero_capital():
    pts = m.generate_equity_curve(0, 10.0, 5)
    assert len(pts) == 1
    assert pts[0]["total_equity"] == 0


def test_generate_equity_curve_zero_trades():
    pts = m.generate_equity_curve(1000.0, 10.0, 0)
    assert len(pts) == 1


def test_generate_equity_curve_normal():
    pts = m.generate_equity_curve(100000.0, 5.0, 10)
    assert len(pts) > 1


def test_simulate_trade_log_zero_return():
    assert m.simulate_trade_log(1, 0.0) == []


def test_simulate_trade_log_normal():
    trades = m.simulate_trade_log(1, 5.0)
    assert len(trades) == 20
    assert "side" in trades[0]


def test_store_backtest_result_exception(monkeypatch):
    def _boom(*a, **k):
        raise _real_sqlite3.Error("boom")
    monkeypatch.setattr(_real_sqlite3, "connect", _boom)
    results = {
        "strategy_id": "s",
        "backtest_id": "b",
        "period": {"start": "a", "end": "b"},
        "capital": {"initial_usd": 1.0, "realized_pnl_usd": 0.0, "unrealized_pnl_usd": 0.0, "total_return_pct": 1.0},
        "risk_metrics": {"sharpe_ratio": 1.0, "max_drawdown_pct": -5.0, "sortino_ratio": 1.0},
        "trading_stats": {"trade_count": 1, "winning_trades": 1, "losing_trades": 0, "win_rate_pct": 100.0, "profit_factor": 1.5, "avg_trade_pnl_usd": 0.0, "gross_traded_usd": 1.0},
        "cost_analysis": {"fees_paid_usd": 0.0, "slippage_costs_usd": 0.0, "total_cost_usd": 0.0},
    }
    assert m.store_backtest_result(results) == 0


def test_get_backtest_results_missing_params():
    r = run(m.get_backtest_results(backtest_id=None, strategy_id=None))
    assert r["status"] == "error"
    assert r["error_type"] == "missing_parameters"


def test_get_backtest_results_not_found_id(shared_db):
    r = run(m.get_backtest_results(backtest_id="999"))
    assert r["status"] == "not_found"


def test_get_backtest_results_not_found_strategy(shared_db):
    r = run(m.get_backtest_results(backtest_id=None, strategy_id="nope"))
    assert r["status"] == "not_found"


def test_get_backtest_results_found_by_strategy(shared_db):
    run(m.trigger_backtest("stratB"))
    r = run(m.get_backtest_results(backtest_id=None, strategy_id="stratB"))
    assert r["status"] == "success"
    assert r["strategy_id"] == "stratB"
    assert r["equity_curve"]


def test_get_backtest_results_found_by_id(shared_db):
    run(m.trigger_backtest("stratC"))
    r = run(m.get_backtest_results(backtest_id="1"))
    assert r["status"] == "success"
    assert r["id"] == 1


def test_get_backtest_results_exception(monkeypatch):
    def _boom(*a, **k):
        raise _real_sqlite3.Error("boom")
    monkeypatch.setattr(_real_sqlite3, "connect", _boom)
    r = run(m.get_backtest_results(backtest_id="1"))
    assert r["status"] == "error"
    assert r["error_type"] == "database_error"


def test_invalidate_backtest_success(shared_db):
    r = run(m.invalidate_backtest("1"))
    assert r["status"] == "success"
    assert r["action"] == "invalidated"


def test_invalidate_backtest_fresh_db(tmp_path, monkeypatch):
    db_file = str(tmp_path / "fresh.db")
    orig = _real_sqlite3.connect
    conn = orig(db_file)
    conn.close()
    monkeypatch.setattr(_real_sqlite3, "connect", lambda *a, **k: orig(db_file))
    r = run(m.invalidate_backtest("1"))
    assert r["status"] == "success"


def test_invalidate_backtest_exception(monkeypatch):
    def _boom(*a, **k):
        raise _real_sqlite3.Error("boom")
    monkeypatch.setattr(_real_sqlite3, "connect", _boom)
    r = run(m.invalidate_backtest("1"))
    assert r["status"] == "error"
    assert r["error_type"] == "database_error"


def test_import_backtest_json_dict():
    r = run(m.import_backtest_data("json", '{"a": 1}'))
    assert r["status"] == "success"
    assert r["records_processed"] == 1


def test_import_backtest_json_list():
    r = run(m.import_backtest_data("json", '[{"a": 1}, {"b": 2}]'))
    assert r["records_processed"] == 2


def test_import_backtest_json_invalid():
    r = run(m.import_backtest_data("json", "{not json"))
    assert r["status"] == "error"
    assert r["error_type"] == "invalid_json"


def test_import_backtest_csv():
    r = run(m.import_backtest_data("csv", "a,b\n1,2"))
    assert r["status"] == "success"
    assert r["source_format"] == "csv"


def test_import_backtest_unsupported():
    r = run(m.import_backtest_data("xml", "<x/>"))
    assert r["status"] == "error"
    assert r["error_type"] == "unsupported_format"
