"""Coverage tests for event_markets.knowledge_gap (web/news mocked)."""

import sys
from unittest.mock import MagicMock, patch

import pytest

from event_markets import knowledge_gap as kg
from event_markets.knowledge_gap import (
    KnowledgeGapAnalyzer, KnowledgeGapAssessment, SentimentAnalyzer,
    WebResearcher, NewsResearcher, SearchResult, _extract_topics,
)
from event_markets.unified_client import PredictionMarket


# ── SearchResult / KnowledgeGapAssessment ──────────────────────────

def test_search_result_text():
    r = SearchResult(source="w", title="Bitcoin", snippet="surge profit", url="u")
    assert "Bitcoin" in r.text


def test_assessment_gap_pct_and_significant():
    a = KnowledgeGapAssessment(
        market_question="q", market_probability=0.5, evidence_score=0.8,
        evidence_count=5, sentiment_label="positive", gap=0.3,
        direction="undervalued", confidence=0.6, sources_used=["w"],
    )
    assert a.gap_pct == 30.0
    assert a.is_significant is True
    # not significant: small gap
    a2 = KnowledgeGapAssessment(
        market_question="q", market_probability=0.5, evidence_score=0.55,
        evidence_count=5, sentiment_label="positive", gap=0.05,
        direction="fair", confidence=0.6, sources_used=["w"],
    )
    assert a2.is_significant is False


def test_to_signal_dict_buy_and_sell():
    buy = KnowledgeGapAssessment(
        market_question="q", market_probability=0.2, evidence_score=0.7,
        evidence_count=5, sentiment_label="positive", gap=0.5,
        direction="undervalued", confidence=0.6, sources_used=["w"],
    )
    d = buy.to_signal_dict()
    assert d["action"] == "BUY"
    sell = KnowledgeGapAssessment(
        market_question="q", market_probability=0.8, evidence_score=0.3,
        evidence_count=5, sentiment_label="negative", gap=-0.5,
        direction="overvalued", confidence=0.6, sources_used=["w"],
    )
    assert sell.to_signal_dict()["action"] == "SELL"


# ── SentimentAnalyzer ──────────────────────────────────────────────

def test_sentiment_analyze():
    s, label = SentimentAnalyzer.analyze("big gain surge profit breakout")
    assert s > 0
    assert label == "positive"
    s2, label2 = SentimentAnalyzer.analyze("crash loss scam fraud bankruptcy")
    assert s2 < 0
    assert label2 == "negative"
    s3, label3 = SentimentAnalyzer.analyze("the coin traded today")
    assert label3 == "neutral"


def test_sentiment_aggregate():
    avg, label = SentimentAnalyzer.aggregate([])
    assert avg == 0.5 and label == "neutral"
    avg2, label2 = SentimentAnalyzer.aggregate([0.0, 0.0, 0.0])
    assert label2 == "neutral"
    avg3, label3 = SentimentAnalyzer.aggregate([1.0, 1.0])
    assert label3 == "positive"


# ── WebResearcher ──────────────────────────────────────────────────

def _wiki_session(positive=True):
    sess = MagicMock()
    words = "gain surge profit breakout boom adoption success" if positive else "crash loss fraud bankrupt"
    sess.get.return_value.json.return_value = {"query": {"search": [
        {"title": "Topic", "snippet": words, "index": 1, "pageid": 1},
    ]}}
    sess.get.return_value.raise_for_status.return_value = None
    return sess


def test_web_researcher_search():
    w = WebResearcher()
    with patch("requests.Session", return_value=_wiki_session()):
        with patch.object(w, "_get_page_extract", return_value="rally profit adoption boom"):
            res = w.search("bitcoin forecast", max_results=3)
    assert len(res) == 1
    assert res[0].source == "wikipedia"
    assert res[0].relevance_score == 1.0


def test_web_researcher_search_no_pageid():
    # When Wikipedia returns no pageid, the extract fallback is skipped.
    w = WebResearcher()
    sess = MagicMock()
    sess.get.return_value.json.return_value = {"query": {"search": [
        {"title": "Topic", "snippet": "gain surge", "index": 2},
    ]}}
    sess.get.return_value.raise_for_status.return_value = None
    with patch("requests.Session", return_value=sess):
        res = w.search("x", max_results=2)
    assert len(res) == 1
    assert res[0].snippet == "gain surge"


def test_news_researcher_no_keyword_match():
    n = NewsResearcher()
    with patch("event_markets.knowledge_gap.RSS_FEEDS", [("http://feed", "Feed")]):
        with patch.object(n, "_fetch_feed", return_value=[
            {"title": "Cooking show", "link": "u1", "description": "recipe cake bake"},
        ]):
            res = n.search("btc news", max_results=10)
    # No keyword from the query matched the article text -> empty
    assert res == []


def test_analyzer_question_empty_returns_none():
    a = KnowledgeGapAnalyzer()
    with patch.object(a._web, "search", return_value=[]), \
         patch.object(a._news, "search", return_value=[]):
        assert a.analyze_question("", 0.5) is None


def test_web_researcher_search_exception():
    w = WebResearcher()
    sess = MagicMock()
    sess.get.side_effect = Exception("boom")
    with patch("requests.Session", return_value=sess):
        assert w.search("x") == []


def test_web_page_extract_exception():
    w = WebResearcher()
    sess = MagicMock()
    sess.get.side_effect = Exception("boom")
    assert w._get_page_extract(1, sess) == ""


# ── NewsResearcher ────────────────────────────────────────────────

def _news_resp():
    r = MagicMock()
    r.raise_for_status.return_value = None
    r.text = (
        "<rss><channel>"
        "<item><title>Bitcoin surges</title><link>u1</link><description>btc profit gain rally</description></item>"
        "<item><title>BTC rises</title><link>u2</link><description>btc breakout boom</description></item>"
        "</channel></rss>"
    )
    return r


def test_news_researcher_search():
    n = NewsResearcher()
    with patch("event_markets.knowledge_gap.RSS_FEEDS", [("http://feed", "Feed")]):
        with patch.object(n, "_fetch_feed", return_value=[
            {"title": "Bitcoin surges", "link": "u1", "description": "btc profit gain rally"},
            {"title": "BTC rises", "link": "u2", "description": "btc breakout boom"},
        ]):
            res = n.search("btc news", max_results=10)
    assert len(res) == 2
    assert res[0].source == "Feed"


def test_news_extract_keywords():
    n = NewsResearcher()
    kw = n._extract_keywords("Will the bitcoin price go up in 2026?")
    assert "bitcoin" in kw
    assert "the" not in kw and "will" not in kw


def test_news_fetch_feed_cache_and_exception():
    n = NewsResearcher(cache_ttl=300)
    with patch("requests.get", return_value=_news_resp()):
        a = n._fetch_feed("http://feed", "Feed")
        b = n._fetch_feed("http://feed", "Feed")
    assert a == b  # cached
    with patch("requests.get", side_effect=Exception("boom")):
        assert n._fetch_feed("http://x", "X") == []


def test_news_parse_rss_valid_and_invalid():
    xml = (
        "<rss><channel>"
        "<item><title>T</title><link>l</link><description>d</description></item>"
        "</channel></rss>"
    )
    out = NewsResearcher._parse_rss(xml, "S")
    assert out[0]["title"] == "T"
    # invalid xml
    assert NewsResearcher._parse_rss("not xml <<<", "S") == []


# ── _extract_topics ───────────────────────────────────────────────

def test_extract_topics():
    topics = _extract_topics("Will BTC reach $100k by June 2026?")
    assert topics  # non-empty
    assert all("will" not in t for t in topics)
    # leading "does"
    t2 = _extract_topics("Does Ethereum go up?")
    assert t2
    # generic
    t3 = _extract_topics("What will happen with inflation in 2025?")
    assert t3


# ── KnowledgeGapAnalyzer ──────────────────────────────────────────

def test_analyzer_no_web_no_news_returns_none():
    a = KnowledgeGapAnalyzer(enable_web_search=False, enable_news_search=False)
    assert a.analyze_question("Will BTC go up?", 0.5) is None


def test_analyzer_question_significant():
    a = KnowledgeGapAnalyzer()
    with patch("requests.Session", return_value=_wiki_session(positive=True)):
        with patch.object(a._web, "_get_page_extract", return_value="rally profit adoption boom"):
            with patch("requests.get", return_value=_news_resp()):
                res = a.analyze_question("Will BTC reach 100k?", 0.5)
    assert res is not None
    assert res.direction == "undervalued"
    assert res.is_significant is True


def test_analyzer_question_insufficient_evidence():
    a = KnowledgeGapAnalyzer(min_evidence=10)
    with patch("requests.Session", return_value=_wiki_session()):
        with patch.object(a._web, "_get_page_extract", return_value="rally profit"):
            with patch("requests.get", return_value=_news_resp()):
                # only a few results but min_evidence high -> None
                res = a.analyze_question("Will BTC reach 100k?", 0.5)
    assert res is None


def test_analyzer_query_method():
    a = KnowledgeGapAnalyzer()
    m = PredictionMarket(
        platform="kalshi", market_id="k", question="Will BTC reach 100k?",
        outcomes=["YES", "NO"], outcome_prices={"YES": 0.5},
        volume=5000, end_date="2026-12-31", is_open=True,
    )
    with patch.object(a, "analyze", return_value=None):
        assert a.analyze(m) is None
    with patch.object(a, "analyze", return_value="ASSESS"):
        assert a.analyze(m) == "ASSESS"


def test_analyzer_markets():
    a = KnowledgeGapAnalyzer()
    m1 = PredictionMarket(
        platform="kalshi", market_id="k1", question="Will BTC reach 100k?",
        outcomes=["YES", "NO"], outcome_prices={"YES": 0.5},
        volume=5000, end_date="2026-12-31", is_open=True,
    )
    m2 = PredictionMarket(
        platform="polymarket", market_id="p1", question="Will ETH go up?",
        outcomes=["YES", "NO"], outcome_prices={"YES": 0.5},
        volume=100, end_date="2026-12-31", is_open=True,
    )
    with patch.object(a, "analyze", side_effect=[None, None]):
        assert a.analyze_markets([m1, m2]) == []
    with patch.object(a, "analyze", side_effect=RuntimeError("boom")):
        assert a.analyze_markets([m1]) == []


# ── main() CLI ────────────────────────────────────────────────────

def test_main_question_no_gap(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["kg", "--question", "Will BTC go up?",
                                       "--probability", "0.5", "--no-web", "--no-news"])
    kg.main()
    out = capsys.readouterr().out
    assert "No significant knowledge gap" in out


def test_main_question_with_gap(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["kg", "--question", "Will BTC go up?",
                                       "--probability", "0.5"])
    a = KnowledgeGapAnalyzer()
    with patch("requests.Session", return_value=_wiki_session(positive=True)):
        with patch.object(a._web, "_get_page_extract", return_value="rally profit adoption boom"):
            with patch("requests.get", return_value=_news_resp()):
                with patch("event_markets.knowledge_gap.KnowledgeGapAnalyzer",
                           return_value=a):
                    kg.main()
    out = capsys.readouterr().out
    assert "Question:" in out


def test_main_batch_no_gaps(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["kg", "--batch", "2"])
    fake_client = MagicMock()
    fake_client.get_crypto_markets.return_value = []
    with patch("event_markets.unified_client.UnifiedPredictionMarketClient",
               return_value=fake_client):
        with patch("event_markets.knowledge_gap.KnowledgeGapAnalyzer.analyze_markets",
                   return_value=[]):
            kg.main()
    assert "No significant knowledge gaps" in capsys.readouterr().out


def test_main_batch_with_gaps(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["kg", "--batch", "1"])
    fake_client = MagicMock()
    fake_client.get_crypto_markets.return_value = []
    assess = KnowledgeGapAssessment(
        market_question="Will BTC go up?", market_probability=0.2,
        evidence_score=0.8, evidence_count=5, sentiment_label="positive",
        gap=0.6, direction="undervalued", confidence=0.7, sources_used=["w"],
    )
    with patch("event_markets.unified_client.UnifiedPredictionMarketClient",
               return_value=fake_client):
        with patch("event_markets.knowledge_gap.KnowledgeGapAnalyzer.analyze_markets",
                   return_value=[assess]):
            kg.main()
    assert "knowledge gaps" in capsys.readouterr().out.lower()
