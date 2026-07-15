import asyncio

from trading_system.apps.research import agents


def run(coro):
    return asyncio.run(coro)


def test_research_agents_methods():
    ra = agents.ResearchAgents()
    assert run(ra.get_news_for_instrument("AAPL"))["symbol"] == "AAPL"
    assert run(ra.get_price_data("AAPL"))["symbol"] == "AAPL"
    assert run(ra.get_fundamentals("AAPL"))["symbol"] == "AAPL"


def test_news_monitor_agent():
    a = agents.NewsMonitorAgent()
    assert run(a.monitor_filings("AAPL")) == []
    assert run(a.monitor_earnings("AAPL"))["symbol"] == "AAPL"


def test_price_watcher_agent():
    a = agents.PriceWatcherAgent()
    assert run(a.get_ohlc("AAPL"))["symbol"] == "AAPL"
    assert run(a.get_technical_indicators("AAPL"))["sma_20"] is None


def test_fundamental_analyst_agent():
    a = agents.FundamentalAnalystAgent()
    assert run(a.get_valuation_ratios("AAPL"))["pe_ttm"] is None
    assert run(a.get_balance_sheet("AAPL"))["total_assets_b"] is None


def test_sentiment_analyzer_agent():
    a = agents.SentimentAnalyzerAgent()
    assert run(a.analyze_sentiment("AAPL"))["sentiment_score"] is None


def test_hypothesis_generator_agent():
    a = agents.HypothesisGeneratorAgent()
    assert run(a.generate_hypothesis("AAPL"))["symbol"] == "AAPL"


def test_module_functions():
    assert run(agents.get_research_hypotheses())["hypotheses"] == []
    assert run(agents.store_research_output({"x": 1})) == 0
    assert run(agents.store_market_regime_analysis({"x": 1})) == 0


def test_module_functions_with_cache():
    # agents.py helpers do not consult a cache; ensure they still accept the kwarg.
    assert run(agents.get_research_hypotheses(cache_manager=None))["hypotheses"] == []
    assert run(agents.store_research_output({"x": 1}, agent_name="X")) == 0
    assert run(agents.store_market_regime_analysis({"x": 1})) == 0
