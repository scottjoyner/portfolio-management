import asyncio
from unittest.mock import MagicMock

from trading_system.apps.backtester import results_storage as rs


def run(coro):
    return asyncio.run(coro)


def test_storage_no_db():
    s = rs.BacktestResultsStorage()
    assert s.db is None
    assert run(s.store_backtest_result({"x": 1}, "k")) == 1
    assert run(s.get_backtest_stats("k")) == {}


def test_storage_with_db():
    db = MagicMock()
    s = rs.BacktestResultsStorage(db_connection=db)
    assert s.db is db
    assert run(s.store_backtest_result({"x": 1}, "k")) == 0
    stats = run(s.get_backtest_stats("k"))
    assert stats["strategy_key"] == "k"
    assert stats["sharpe_ratio"] == 0.0


def test_historical_api():
    s = rs.BacktestResultsStorage()
    api = rs.HistoricalPerformanceAPI(s)
    assert run(api._get_live_metrics("k"))["current_pnl_usd"] == 0.0
    assert run(api.get_benchmark_comparison("k"))["spy_returns_30d"] == 0.0
    assert run(api._get_performance_attribution("k"))["alpha_pct"] == 0.0
    perf = run(api.get_strategy_performance("k"))
    assert perf["strategy_key"] == "k"
    assert "performance_attribution" in perf


def test_list_strategies_no_cache():
    out = run(rs.list_strategies_with_backtest_history())
    assert out["strategies"] == []
    assert out["total_strategies"] == 0


def test_list_strategies_cache_hit():
    cache = MagicMock()
    cache.get.return_value = {"strategies": [1]}
    out = run(rs.list_strategies_with_backtest_history(cache_manager=cache))
    assert out == {"strategies": [1]}
    cache.get.assert_called_once_with("strategies")


def test_list_strategies_cache_miss():
    cache = MagicMock()
    cache.get.return_value = None
    out = run(rs.list_strategies_with_backtest_history(cache_manager=cache))
    assert out["strategies"] == []


def test_get_strategy_details_cache_miss():
    cache = MagicMock()
    cache.get.return_value = None
    out = run(rs.get_strategy_details("k", cache_manager=cache))
    assert out["strategy_key"] == "k"


def test_get_strategy_details_no_cache():
    out = run(rs.get_strategy_details("k"))
    assert out["strategy_key"] == "k"


def test_get_strategy_details_cache_hit():
    cache = MagicMock()
    cache.get.return_value = {"hit": True}
    out = run(rs.get_strategy_details("k", cache_manager=cache))
    assert out == {"hit": True}
    cache.get.assert_called_once_with("strategies", key="strategy:k")


def test_get_backtest_comparison():
    out = run(rs.get_backtest_comparison("k", benchmark="SPY"))
    assert out["benchmark"] == "SPY"
