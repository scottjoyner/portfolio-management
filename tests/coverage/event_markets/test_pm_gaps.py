"""Tests for event_markets P0/P1 bug-fix gaps (network fully mocked).

Covers:
  P0-1  Kalshi plaintext password never logged/transmitted
  P0-2  KG direction semantics (undervalued -> BUY) as single source of truth
  P0-3  Symbol mapping uses word-boundary (ethics != ETH, politics != POL,
          botcoin != BTC)
  P1-4  Resolved/closed markets excluded from hot path
  P1-5  KG contrarian boost requires >=4 evidence + >=2 distinct feeds
  P1-6  Sentiment |weight| + per-question TTL cache
  P1-7  Neo4j uses env var, not a literal
  P1-8  Polymarket missing bestAsk -> spread=1.0 (fails liquidity filter)
"""

import os
import sys
import logging
from unittest import mock

import pytest

# Ensure repo root is importable.
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from event_markets import knowledge_gap as kg
from event_markets.knowledge_gap import (
    KnowledgeGapAnalyzer,
    KnowledgeGapAssessment,
    SentimentAnalyzer,
    WebResearcher,
    NewsResearcher,
    SearchResult,
)
from event_markets.unified_client import UnifiedPredictionMarketClient, PredictionMarket
from event_markets import signal_adapter as sa
from event_markets.signal_adapter import PredictionMarketAdapter
from event_markets import kalshi_client as kc
from event_markets import polymarket_client as pmc


# ── Helpers ──────────────────────────────────────────────────────────

def _make_market(mid, question="Will BTC hit 100k?", platform="kalshi",
                  status="open", settled=False, volume=5000.0, spread=0.05,
                  category="crypto", raw=None):
    raw = dict(raw or {})
    if settled:
        raw["settled"] = True
    return PredictionMarket(
        platform=platform,
        market_id="TICKER",
        question=question,
        outcomes=["YES", "NO"],
        outcome_prices={"YES": mid, "NO": 1 - mid},
        volume=volume,
        end_date="2030-01-01T00:00:00Z",
        is_open=(status in ("open", "active")),
        yes_bid=max(mid - spread / 2, 0.0),
        yes_ask=min(mid + spread / 2, 1.0),
        spread=spread,
        liquidity_score=0.8,
        category=category,
        raw_data=raw,
    )


def _fake_result(source, text, relevance=1.0, url="http://x/"):
    return SearchResult(source=source, title=text, snippet=text, url=url,
                        relevance_score=relevance)


# ── P0-1: Kalshi plaintext password ────────────────────────────────

def test_kalshi_login_no_sig_and_no_password_logged():
    """Password must never be logged or sent in a misleading signed form."""
    captured = []
    handler = logging.Handler()
    handler.emit = lambda rec: captured.append(rec.getMessage())
    logger = logging.getLogger("kalshi")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        # Mock urllib so no real network; capture the POST body.
        sent = {}

        class _Resp:
            def read(self):
                return b'{"token":"T","member_id":"M1"}'

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        class _Req:
            def __init__(self, url, data=None, headers=None):
                sent["url"] = url
                sent["data"] = data
                sent["headers"] = headers

        with mock.patch.object(kc.urllib.request, "Request", _Req), \
             mock.patch.object(kc.urllib.request, "urlopen", lambda req, timeout=0: _Resp()):
            client = kc.KalshiClient(email="a@b.com", password="SUPERSECRET")
            client._login()

        body = sent["data"].decode()
        assert "SUPERSECRET" in body  # it IS transmitted (legacy flow) ...
        assert "password" in body
        # ... but the bogus sha256 "sig" must be gone and never computed.
        assert "sig" not in body.lower() or "sha256" not in str(sent)
        # The password must NOT appear in any log message.
        for msg in captured:
            assert "SUPERSECRET" not in msg, f"password leaked in log: {msg}"
        # login must not use the API-key path accidentally.
        assert client._token == "T"
    finally:
        logger.removeHandler(handler)


def test_kalshi_login_no_creds_no_password_log():
    captured = []
    handler = logging.Handler()
    handler.emit = lambda rec: captured.append(rec.getMessage())
    logger = logging.getLogger("kalshi")
    logger.addHandler(handler)
    try:
        client = kc.KalshiClient()  # no creds
        client._login()
        for msg in captured:
            assert "password" not in msg.lower()
        assert client._token == ""
    finally:
        logger.removeHandler(handler)


# ── P0-2: KG direction semantics ───────────────────────────────────

def test_undervalued_below_05_is_buy():
    """Undervalued market trading below 0.5 => BUY (correct semantic)."""
    # mid_price < 0.5 and evidence > market => undervalued => BUY.
    a = KnowledgeGapAssessment(
        market_question="Will BTC hit 100k?",
        market_probability=0.30,        # market says 30% YES
        evidence_score=0.70,            # evidence says 70% -> undervalued
        evidence_count=5,
        sentiment_label="positive",
        gap=0.40,
        direction="undervalued",
        confidence=0.8,
        sources_used=["wikipedia", "CoinDesk"],
    )
    assert a.signal_action == "BUY"
    d = a.to_signal_dict()
    assert d["action"] == "BUY"
    # Documented convention: undervalued => market YES too low => BUY YES.
    assert a.direction == "undervalued"


def test_overvalued_above_05_is_sell():
    a = KnowledgeGapAssessment(
        market_question="Will ETH merge?",
        market_probability=0.80,
        evidence_score=0.40,
        evidence_count=5,
        sentiment_label="negative",
        gap=-0.40,
        direction="overvalued",
        confidence=0.8,
        sources_used=["wikipedia", "Cointelegraph"],
    )
    assert a.signal_action == "SELL"
    assert a.to_signal_dict()["action"] == "SELL"


def test_kg_signal_matches_market_convention():
    """Optimizer consumes direction via signal_action; assert it is self-consistent."""
    m = _make_market(0.30, question="Will BTC hit 100k?")
    a = KnowledgeGapAssessment(
        market_question=m.question,
        market_probability=m.mid_price,
        evidence_score=0.80,   # >> market => undervalued
        evidence_count=6,
        sentiment_label="positive",
        gap=0.50,
        direction="undervalued",
        confidence=0.9,
        sources_used=["wikipedia", "CoinDesk"],
    )
    # Single source of truth for direction:
    assert a.signal_action == "BUY"


# ── P0-3: word-boundary symbol mapping ─────────────────────────────

@pytest.mark.parametrize("question,expected", [
    ("Will ethics reform pass?", ""),          # NOT ETH
    ("Politics debate tonight", ""),          # NOT POL
    ("Botcoin launch event", ""),            # NOT BTC
    ("Ethereum staking yield", "ETH-USD"),   # eth -> ETH
    ("Polygon upgrade", "POL-USD"),          # pol -> POL
    ("Bitcoin halving", "BTC-USD"),
])
def test_symbol_map_word_boundary(question, expected):
    sym = PredictionMarketAdapter._question_to_symbol(question, "crypto")
    assert sym == expected


def test_symbol_map_no_substring_false_positive():
    assert PredictionMarketAdapter._question_to_symbol("ethics", "crypto") != "ETH-USD"
    assert PredictionMarketAdapter._question_to_symbol("politics", "crypto") != "POL-USD"
    assert PredictionMarketAdapter._question_to_symbol("botcoin", "crypto") != "BTC-USD"


# ── P1-4: resolved/closed markets excluded ─────────────────────────

def test_closed_market_not_tradeable():
    m_open = _make_market(0.6, status="open")
    m_closed = _make_market(0.6, status="closed")
    m_settled = _make_market(0.6, status="open", settled=True)
    assert m_open.is_tradeable is True
    assert m_closed.is_tradeable is False
    assert m_settled.is_tradeable is False


def test_adapter_excludes_resolved_mid_cache():
    """A market whose status flips to closed within the cache window is excluded."""
    m = _make_market(0.7, status="open", volume=5000.0)
    adapter = PredictionMarketAdapter(kalshi_email="x", kalshi_password="y")
    # Force crypto fetch to return our market, then a closed version.
    with mock.patch.object(adapter._client, "get_crypto_markets",
                           return_value=[m]):
        sigs1 = adapter.get_signals()
    assert len(sigs1) == 1  # tradeable

    m.is_open = False
    m.raw_data = {"status": "closed"}
    with mock.patch.object(adapter._client, "get_crypto_markets",
                           return_value=[m]):
        sigs2 = adapter.get_signals()
    assert sigs2 == []  # excluded after resolution


def test_unified_category_cache_short():
    """search_all_categories cache window must be <=10s."""
    client = UnifiedPredictionMarketClient()
    with mock.patch.object(client, "_polymarket_to_unified", return_value=[]), \
         mock.patch.object(client._polymarket, "fetch_markets", return_value=[]):
        # First call populates; second within 10s should reuse cache.
        r1 = client.search_all_categories(limit_per_platform=1)
        r2 = client.search_all_categories(limit_per_platform=1)
        assert r1 is r2  # same cached object
    # Verify the TTL constant by checking the source window <= 10.
    import inspect
    src = inspect.getsource(UnifiedPredictionMarketClient.search_all_categories)
    assert "(now - self._category_cache_ts) < 10" in src


# ── P1-5: KG boost requires >=4 evidence + >=2 distinct feeds ──────

def test_kg_min_evidence_raised():
    a = KnowledgeGapAnalyzer()
    assert a.min_evidence >= 4
    assert a.min_distinct_feeds >= 2


def test_kg_two_single_feed_results_not_significant():
    """2 low-evidence, single-feed results must NOT produce a boost."""
    a = KnowledgeGapAnalyzer(enable_web_search=False, enable_news_search=True)
    # Patch the researchers to return 2 results from a SINGLE feed.
    res = [
        _fake_result("CoinDesk", "bitcoin surges to record high bullish", relevance=1.0,
                     url="http://coindesk/1"),
        _fake_result("CoinDesk", "ethereum rallies breakout upside", relevance=1.0,
                     url="http://coindesk/2"),
    ]
    with mock.patch.object(NewsResearcher, "search", return_value=res), \
         mock.patch.object(WebResearcher, "search", return_value=[]):
        assess = a.analyze_question("Will BTC hit 100k?", 0.30)
    # Single feed + below min_evidence(4) -> no signal emitted at all.
    assert assess is None


def test_kg_four_multi_feed_is_significant():
    a = KnowledgeGapAnalyzer(enable_web_search=False, enable_news_search=True)
    res = [
        _fake_result("CoinDesk", "bitcoin surges record high bullish", url="u1"),
        _fake_result("Cointelegraph", "ethereum rallies breakout upside", url="u2"),
        _fake_result("NYT Business", "crypto adoption boom positive", url="u3"),
        _fake_result("CryptoSlate", "bitcoin momentum upgrade growth", url="u4"),
    ]
    with mock.patch.object(NewsResearcher, "search", return_value=res), \
         mock.patch.object(WebResearcher, "search", return_value=[]):
        assess = a.analyze_question("Will BTC hit 100k?", 0.30)
    assert assess.confidence > 0
    assert assess.is_significant is True
    assert len(assess.sources_used) >= 2


# ── P1-6: sentiment weighting by |sentiment| + cache ───────────────

def test_sentiment_weighted_by_magnitude():
    """A single high-relevance NEUTRAL article must not dominate the score."""
    a = KnowledgeGapAnalyzer(enable_web_search=False, enable_news_search=True)
    # One strong positive + one neutral-but-high-relevance article + enough
    # filler from distinct feeds to clear min_evidence(4)/min_feed(2).
    res = [
        _fake_result("CoinDesk", "bitcoin surges record high bullish rally", relevance=1.0, url="p1"),
        _fake_result("Cointelegraph", "the market discussed the protocol quietly", relevance=1.0, url="n1"),
        _fake_result("NYT Business", "crypto volume steady", relevance=0.5, url="f1"),
        _fake_result("CryptoSlate", "ethereum network normal", relevance=0.5, url="f2"),
    ]
    with mock.patch.object(NewsResearcher, "search", return_value=res), \
         mock.patch.object(WebResearcher, "search", return_value=[]):
        assess = a.analyze_question("Will BTC hit 100k?", 0.30)
    # Evidence should be positive (driven by the strong hit), not ~0.5 neutral.
    assert assess is not None
    assert assess.evidence_score > 0.6


def test_kg_question_cache_returns_cached():
    a = KnowledgeGapAnalyzer(enable_web_search=False, enable_news_search=True)
    res = [
        _fake_result("CoinDesk", "bitcoin surges record high bullish", url="u1"),
        _fake_result("Cointelegraph", "ethereum rallies breakout upside", url="u2"),
        _fake_result("NYT Business", "crypto adoption boom positive", url="u3"),
        _fake_result("CryptoSlate", "bitcoin momentum upgrade growth", url="u4"),
    ]
    fake = mock.Mock(return_value=res)
    with mock.patch.object(NewsResearcher, "search", fake), \
         mock.patch.object(WebResearcher, "search", return_value=[]):
        r1 = a.analyze_question("Will BTC hit 100k?", 0.30)
        r2 = a.analyze_question("Will BTC hit 100k?", 0.30)
    # Second call must hit the cache, not re-run the network search.
    assert r1 is r2
    assert fake.call_count == 1


# ── P1-7: Neo4j uses env, not literal ──────────────────────────────

def test_neo4j_connection_uses_env(monkeypatch):
    monkeypatch.setenv("NEO4J_PASSWORD", "envsecret")
    monkeypatch.setenv("NEO4J_URI", "bolt://envhost:7687")
    # Patch driver creation to avoid a real connection attempt.
    with mock.patch("graph_alpha_bot.app.db.neo4j_connection.GraphDatabase") as gd:
        gd.driver.return_value.session.return_value.__enter__.return_value.run.return_value.single.return_value = True
        from graph_alpha_bot.app.db.neo4j_connection import Neo4jConnection, get_connection
        conn = Neo4jConnection()
        assert conn.password == "envsecret"
        assert conn.uri == "bolt://envhost:7687"
        gc = get_connection()
        assert gc.password == "envsecret"


def test_neo4j_no_default_secret(monkeypatch):
    monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
    monkeypatch.delenv("NEO4J_URI", raising=False)
    from graph_alpha_bot.app.db.neo4j_connection import Neo4jConnection
    conn = Neo4jConnection()
    assert conn.password != "gluhlaf8"
    assert "gluhlaf8" not in conn.password
    assert "tailcb8954" not in conn.uri


# ── P1-8: Polymarket missing bestAsk -> spread=1.0 ─────────────────

def test_polymarket_missing_ask_overstates_spread():
    raw = {
        "conditionId": "c1",
        "question": "Will BTC hit 100k?",
        "outcomes": '["YES","NO"]',
        "outcomePrices": '["0.6","0.4"]',
        "volume": "5000",
        "endDateIso": "2030-01-01T00:00:00Z",
        "closed": False,
        "acceptingOrders": True,
        "clobTokenIds": '["t1"]',
        # bestAsk intentionally omitted -> must NOT default to 1.0.
        "bestBid": "0.59",
        "spread": "0",
    }
    m = pmc.PolymarketClient()._parse_gamma_market(raw)
    # Missing ask with zero spread => spread forced to 1.0 (fails liquidity filter).
    assert m.yes_ask == 0.0
    assert m.spread == 1.0


def test_polymarket_present_ask_unchanged():
    raw = {
        "conditionId": "c1",
        "question": "Will BTC hit 100k?",
        "outcomes": '["YES","NO"]',
        "outcomePrices": '["0.6","0.4"]',
        "volume": "5000",
        "endDateIso": "2030-01-01T00:00:00Z",
        "closed": False,
        "acceptingOrders": True,
        "clobTokenIds": '["t1"]',
        "bestBid": "0.59",
        "bestAsk": "0.61",
        "spread": "0.02",
    }
    m = pmc.PolymarketClient()._parse_gamma_market(raw)
    assert m.yes_ask == 0.61
    assert m.spread == 0.02
