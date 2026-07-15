import asyncio
from unittest.mock import MagicMock

from trading_system.apps.research import routes


def run(coro):
    return asyncio.run(coro)


def test_basic_endpoints():
    assert run(routes.get_news("AAPL"))["symbol"] == "AAPL"
    assert run(routes.get_price("AAPL"))["symbol"] == "AAPL"
    assert run(routes.get_fundamentals("AAPL"))["symbol"] == "AAPL"
    assert run(routes.get_sentiment("AAPL"))["symbol"] == "AAPL"


def test_get_hypotheses_no_cache():
    assert run(routes.get_hypotheses())["hypotheses"] == []


def test_get_hypotheses_cache_miss():
    cache = MagicMock()
    cache.get.return_value = None
    out = run(routes.get_hypotheses(symbols=["AAPL"], cache_manager=cache))
    assert out["hypotheses"] == []
    cache.get.assert_called_once_with("hypotheses")


def test_get_hypotheses_cache_hit():
    sentinel = {"hypotheses": ["x"], "active_searches": [], "last_analysis_timestamp": None}
    cache = MagicMock()
    cache.get.return_value = sentinel
    assert run(routes.get_hypotheses(cache_manager=cache)) == sentinel


def test_run_comprehensive_analysis_no_cache():
    out = run(routes.run_comprehensive_analysis("AAPL"))
    assert out["symbol"] == "AAPL"
    assert "news" in out


def test_run_comprehensive_analysis_cache_hit():
    sentinel = {"symbol": "AAPL", "cached": True}
    cache = MagicMock()
    cache.get.return_value = sentinel
    assert run(routes.run_comprehensive_analysis("AAPL", cache_manager=cache)) == sentinel


def test_run_comprehensive_analysis_cache_miss_sets():
    cache = MagicMock()
    cache.get.return_value = None
    out = run(routes.run_comprehensive_analysis("AAPL", cache_manager=cache))
    assert out["symbol"] == "AAPL"
    assert cache.set.called
