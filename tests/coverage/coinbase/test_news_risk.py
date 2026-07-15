import json
import os
import tempfile
import unittest
from unittest import mock

from coinbase.src import news_risk
from coinbase.src.news_risk import (
    KnowledgeGraphReader, MCPSentimentClient, NewsRiskAdjuster, NewsAwareRiskStrategy,
    NewsRiskSnapshot,
)
from coinbase.src.protocols import Direction, Opportunity


def write_kg(tmpdir, data):
    p = os.path.join(tmpdir, "kg.json")
    with open(p, "w") as f:
        json.dump(data, f)
    return p


def fake_response(payload, status=200):
    resp = mock.MagicMock()
    resp.read.return_value = json.dumps(payload).encode()
    resp.status = status
    cm = mock.MagicMock()
    cm.__enter__.return_value = resp
    cm.__exit__.return_value = False
    return cm


class TestKnowledgeGraphReader(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_read_missing(self):
        r = KnowledgeGraphReader(path=os.path.join(self.tmp, "nope.json"))
        data = r.read()
        self.assertIn("tickers", data)

    def test_read_valid(self):
        p = write_kg(self.tmp, {"tickers": {"BTC": {"avg_sentiment": 0.2}}, "articles": [], "metadata": {}})
        r = KnowledgeGraphReader(path=p)
        data = r.read()
        self.assertIn("BTC", data["tickers"])

    def test_read_nondict_list(self):
        p = os.path.join(self.tmp, "kg.json")
        with open(p, "w") as f:
            f.write("[1,2,3]")
        r = KnowledgeGraphReader(path=p)
        data = r.read()
        self.assertIn("articles", data)

    def test_read_exception(self):
        p = os.path.join(self.tmp, "kg.json")
        with open(p, "w") as f:
            f.write("{not valid")
        r = KnowledgeGraphReader(path=p)
        data = r.read()
        self.assertIn("tickers", data)

    def test_cache_ttl(self):
        p = write_kg(self.tmp, {"tickers": {"BTC": {}}, "articles": [], "metadata": {}})
        r = KnowledgeGraphReader(path=p, cache_ttl_secs=60)
        r.read()
        self.assertIsNotNone(r._cache)

    def test_get_ticker_sentiment_by_ticker(self):
        p = write_kg(self.tmp, {"tickers": {"BTC": {"avg_sentiment": 0.2, "count": 3}}, "articles": [], "metadata": {}})
        r = KnowledgeGraphReader(path=p)
        info = r.get_ticker_sentiment("BTC")
        self.assertEqual(info["avg_sentiment"], 0.2)

    def test_get_ticker_sentiment_by_article(self):
        p = write_kg(self.tmp, {"tickers": {}, "articles": [
            {"tickers": ["ETH"], "sentiment_score": 0.9, "title": "big hack exploit",
             "summary": "sec investigation", "is_breaking": True, "topic": "sec"}
        ], "metadata": {}})
        r = KnowledgeGraphReader(path=p)
        info = r.get_ticker_sentiment("ETH")
        self.assertEqual(info["hack_count"], 1)
        self.assertEqual(info["regulation_count"], 1)
        self.assertTrue(info["breaking_count"])

    def test_get_ticker_sentiment_none(self):
        r = KnowledgeGraphReader(path=os.path.join(self.tmp, "nope.json"))
        self.assertIsNone(r.get_ticker_sentiment("XYZ"))

    def test_breaking_topics_and_pulse(self):
        p = write_kg(self.tmp, {"tickers": {"BTC": {"avg_sentiment": 0.3}},
                                "articles": [], "metadata": {"breaking_topics": ["hacks_security"]}})
        r = KnowledgeGraphReader(path=p)
        self.assertEqual(r.get_breaking_topics(), ["hacks_security"])
        self.assertAlmostEqual(r.global_sentiment_pulse(), 0.3)


class TestMCPSentimentClient(unittest.TestCase):
    def test_not_allowed(self):
        c = MCPSentimentClient()
        c._breaker.failure_threshold = 1
        c._breaker.on_failure(RuntimeError())
        self.assertIsNone(c.query_sentiment("BTC"))
        self.assertFalse(c.is_available())

    def test_query_sentiment_success(self):
        c = MCPSentimentClient()
        payload = {"results": [{"avg_score": 0.4, "article_count": 3}]}
        with mock.patch("urllib.request.Request"), \
             mock.patch("urllib.request.urlopen", return_value=fake_response(payload)):
            res = c.query_sentiment("BTC")
        self.assertEqual(res["avg_score"], 0.4)

    def test_query_sentiment_empty(self):
        c = MCPSentimentClient()
        payload = {"results": []}
        with mock.patch("urllib.request.Request"), \
             mock.patch("urllib.request.urlopen", return_value=fake_response(payload)):
            self.assertIsNone(c.query_sentiment("BTC"))

    def test_query_sentiment_failure(self):
        c = MCPSentimentClient()
        with mock.patch("urllib.request.Request"), \
             mock.patch("urllib.request.urlopen", side_effect=RuntimeError("net")):
            self.assertIsNone(c.query_sentiment("BTC"))

    def test_query_news(self):
        c = MCPSentimentClient()
        payload = {"results": [{"x": 1}]}
        with mock.patch("urllib.request.Request"), \
             mock.patch("urllib.request.urlopen", return_value=fake_response(payload)):
            self.assertEqual(len(c.query_news("BTC")), 1)

    def test_is_available_true(self):
        c = MCPSentimentClient()
        with mock.patch("urllib.request.Request"), \
             mock.patch("urllib.request.urlopen", return_value=fake_response({}, status=200)):
            self.assertTrue(c.is_available())

    def test_is_available_false(self):
        c = MCPSentimentClient()
        with mock.patch("urllib.request.Request"), \
             mock.patch("urllib.request.urlopen", side_effect=RuntimeError("x")):
            self.assertFalse(c.is_available())


class TestNewsRiskAdjusterStatics(unittest.TestCase):
    def test_compute_sentiment_risk(self):
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._compute_sentiment_risk(0.5, 0, False, False, 5), 0.5 - 0.25)
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._compute_sentiment_risk(-0.5, 0, False, False, 5), 1.0)
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._compute_sentiment_risk(0.0, 0.5, True, True, 5), 1.0)
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._compute_sentiment_risk(0.0, 0.0, False, False, 0), 0.3)
        self.assertLessEqual(news_risk.NewsRiskAdjuster._compute_sentiment_risk(-5, 5, True, True, 5), 1.0)

    def test_size_multiplier(self):
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._size_multiplier(0.0, 0, False), 1.0)
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._size_multiplier(-0.5, 0.5, True), max(0.3, 1.0 - 0.75 - 0.6 - 0.5))

    def test_stop_multiplier(self):
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._stop_multiplier(0.0, False, False), 1.0)
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._stop_multiplier(-0.5, True, True), min(3.0, 1.0 + 0.75 + 1.0 + 0.5))

    def test_confidence_penalty(self):
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._confidence_penalty(0.0, 0, False), 0.0)
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._confidence_penalty(-0.5, 0.5, True), min(0.5, 0.75 + 0.15 + 0.3))

    def test_leverage_cap(self):
        self.assertEqual(news_risk.NewsRiskAdjuster._leverage_cap(True, False, 0.0), 1.0)
        self.assertEqual(news_risk.NewsRiskAdjuster._leverage_cap(False, True, 0.0), 1.5)
        self.assertEqual(news_risk.NewsRiskAdjuster._leverage_cap(False, False, 0.5), 2.0)
        self.assertEqual(news_risk.NewsRiskAdjuster._leverage_cap(False, False, 0.0), 3.0)

    def test_var_adjustment(self):
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._var_adjustment(0.0, 0, False), 1.0)
        self.assertAlmostEqual(news_risk.NewsRiskAdjuster._var_adjustment(-0.5, 0.5, True), min(3.0, 1.0 + 0.5 + 0.5 + 1.0))


class TestNewsRiskAdjuster(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        p = write_kg(self.tmp, {
            "tickers": {"BTC-USD": {"avg_sentiment": -0.5, "count": 4, "breaking_count": 2,
                                    "hack_count": 1, "regulation_count": 0, "topics": []}},
            "articles": [], "metadata": {"breaking_topics": ["hacks_security"], "total_articles": 4}})
        self.kg = KnowledgeGraphReader(path=p)
        self.adj = NewsRiskAdjuster(kg_reader=self.kg, enable_mcp=False)

    def test_assess_no_news(self):
        adj = NewsRiskAdjuster(kg_reader=KnowledgeGraphReader(path=os.path.join(self.tmp, "nope.json")))
        snap = adj.assess_product("ETH-USD")
        self.assertEqual(snap.reason, "no news data")

    def test_assess_with_kg(self):
        snap = self.adj.assess_product("BTC-USD")
        self.assertEqual(snap.article_count, 4)
        self.assertTrue(snap.has_hacks)
        self.assertEqual(snap.sentiment_risk_score, 1.0)

    def test_assess_with_mcp(self):
        mcp = mock.MagicMock()
        mcp.is_available.return_value = True
        mcp.query_sentiment.return_value = {"avg_score": 0.3, "article_count": 5}
        adj = NewsRiskAdjuster(kg_reader=self.kg, mcp_client=mcp, enable_mcp=True)
        snap = adj.assess_product("BTC-USD")
        self.assertGreater(snap.avg_sentiment, -0.5)

    def test_adjust_opportunity_no_news(self):
        adj = NewsRiskAdjuster(kg_reader=KnowledgeGraphReader(path=os.path.join(self.tmp, "nope.json")))
        opp = Opportunity(product_id="ETH-USD", direction=Direction.LONG, instrument_type=None,
                          entry_price=100, stop_price=90, target_price=110, risk_reward=2,
                          confidence=0.6, reason="r", strategy_name="s")
        out = adj.adjust_opportunity(opp)
        self.assertEqual(out.confidence, 0.6)

    def test_adjust_opportunity_with_news(self):
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                          entry_price=100, stop_price=90, target_price=110, risk_reward=2,
                          confidence=0.6, reason="r", strategy_name="s")
        out = self.adj.adjust_opportunity(opp)
        self.assertIn("news_risk", out.meta)
        self.assertLess(out.confidence, 0.6)
        self.assertIn("[NEWS_RISK]", out.reason)

    def test_adjust_profile(self):
        class Profile:
            max_position_pct = 0.2
            max_notional_per_trade = 5000.0
            risk_per_trade_pct = 0.01
            max_leverage = 3.0
        prof = Profile()
        self.adj.adjust_profile(prof)
        self.assertEqual(prof.max_position_pct, 0.10)

    def test_summary(self):
        s = self.adj.summary()
        self.assertIn("BTC-USD", s["products_assessed"])


class TestNewsAwareRiskStrategy(unittest.TestCase):
    def test_basic(self):
        s = NewsAwareRiskStrategy()
        self.assertEqual(s.name(), "news_risk")
        self.assertIsNone(s.on_bar(None, []))
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                         entry_price=100, stop_price=90, target_price=110, risk_reward=2,
                         confidence=0.6, reason="r", strategy_name="s")
        out = s.adjust_opportunity(opp)
        self.assertIsInstance(out, Opportunity)


class TestNewsRiskEdgeCases(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        p = write_kg(self.tmp, {
            "tickers": {"BTC-USD": {"avg_sentiment": -0.5, "count": 4, "breaking_count": 2,
                                    "hack_count": 1, "regulation_count": 0, "topics": []}},
            "articles": [], "metadata": {"breaking_topics": ["hacks_security"], "total_articles": 4}})
        self.kg = KnowledgeGraphReader(path=p)
        self.adj = NewsRiskAdjuster(kg_reader=self.kg, enable_mcp=False)

    def test_adjust_opportunity_short_direction(self):
        opp = Opportunity(product_id="BTC-USD", direction=Direction.SHORT, instrument_type=None,
                          entry_price=100, stop_price=110, target_price=90, risk_reward=2,
                          confidence=0.6, reason="r", strategy_name="s")
        out = self.adj.adjust_opportunity(opp)
        self.assertIn("news_risk", out.meta)
        self.assertLess(out.confidence, 0.6)

    def test_assess_mcp_unavailable(self):
        mcp = mock.MagicMock()
        mcp.is_available.return_value = False
        adj = NewsRiskAdjuster(kg_reader=self.kg, mcp_client=mcp, enable_mcp=True)
        snap = adj.assess_product("BTC-USD")  # mcp path skipped -> no crash
        self.assertEqual(snap.article_count, 4)

    def test_assess_mcp_avg_none(self):
        mcp = mock.MagicMock()
        mcp.is_available.return_value = True
        mcp.query_sentiment.return_value = {"article_count": 5}  # no avg_score
        adj = NewsRiskAdjuster(kg_reader=self.kg, mcp_client=mcp, enable_mcp=True)
        snap = adj.assess_product("BTC-USD")
        self.assertEqual(snap.avg_sentiment, -0.5)  # not blended when mc_avg is None

    def test_adjust_opportunity_low_risk_no_reason_tag(self):
        # article_count > 0 but no hacks and risk <= 0.7 -> no [NEWS_RISK] tag / score cut
        p = write_kg(self.tmp, {
            "tickers": {"BTC-USD": {"avg_sentiment": 0.5, "count": 3, "breaking_count": 0,
                                    "hack_count": 0, "regulation_count": 0, "topics": []}},
            "articles": [], "metadata": {}})
        adj = NewsRiskAdjuster(kg_reader=KnowledgeGraphReader(path=p))
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                          entry_price=100, stop_price=90, target_price=110, risk_reward=2,
                          confidence=0.6, reason="r", strategy_name="s", score=1.0)
        out = adj.adjust_opportunity(opp)
        self.assertNotIn("[NEWS_RISK]", out.reason)
        self.assertEqual(out.score, 1.0)

    def test_global_pulse_no_tickers(self):
        p = write_kg(self.tmp, {"tickers": {}, "articles": [], "metadata": {}})
        r = KnowledgeGraphReader(path=p)
        self.assertEqual(r.global_sentiment_pulse(), 0.5)

    def test_adjust_profile_no_heavy_breaking(self):
        p = write_kg(self.tmp, {"tickers": {"ETH": {"avg_sentiment": 0.4}},
                                "articles": [], "metadata": {"breaking_topics": [], "total_articles": 0}})
        adj = NewsRiskAdjuster(kg_reader=KnowledgeGraphReader(path=p))
        class Profile:
            max_position_pct = 0.2
            max_notional_per_trade = 5000.0
            risk_per_trade_pct = 0.01
            max_leverage = 3.0
        prof = Profile()
        adj.adjust_profile(prof)  # pulse=0.4 >= threshold, no heavy breaking
        self.assertEqual(prof.max_position_pct, 0.2)

    def test_adjust_opportunity_hack_risk_score_cut(self):
        opp = Opportunity(product_id="BTC-USD", direction=Direction.LONG, instrument_type=None,
                          entry_price=100, stop_price=90, target_price=110, risk_reward=2,
                          confidence=0.6, reason="r", strategy_name="s", score=1.0)
        out = self.adj.adjust_opportunity(opp)
        self.assertIn("[NEWS_RISK]", out.reason)
        self.assertLess(out.score, 1.0)


if __name__ == "__main__":
    unittest.main()
