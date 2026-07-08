"""
Knowledge Gap Analyzer — identifies information asymmetries between prediction
market probabilities and available web/news evidence.

For each prediction market bet, searches Wikipedia and news RSS feeds for
relevant information, computes an aggregate evidence score (0-1), and compares
it against the market's implied probability. A significant gap suggests the
market may be mispriced.

Usage:
    analyzer = KnowledgeGapAnalyzer()
    assessment = analyzer.analyze(market)
    if assessment and assessment.is_significant:
        print(f"Gap: {assessment.gap_pct:.1f}% | {assessment.direction}")
        print(f"  Evidence: {assessment.evidence_score:.2f} vs Market: {assessment.market_probability:.2f}")
"""

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

from .unified_client import PredictionMarket

logger = logging.getLogger("knowledge_gap")

WIKI_API = "https://en.wikipedia.org/w/api.php"
RSS_FEEDS = [
    ("https://www.coindesk.com/arc/rss/articles?topic=Cryptocurrency", "CoinDesk"),
    ("https://cointelegraph.com/news/feed", "Cointelegraph"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "NYT Business"),
    ("https://cryptoslate.com/feed/", "CryptoSlate"),
    # Sports news
    ("https://www.espn.com/espn/rss/news", "ESPN"),
    ("https://feeds.bbci.co.uk/sport/rss.xml", "BBC Sport"),
    # Politics news
    ("https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml", "NYT Politics"),
    ("https://feeds.bbci.co.uk/news/politics/rss.xml", "BBC Politics"),
    # Economics / business news
    ("https://feeds.bbci.co.uk/news/business/rss.xml", "BBC Business"),
    ("https://www.cnbc.com/id/100003114/device/rss/rss.html", "CNBC"),
]

POSITIVE_WORDS = frozenset({
    "gain", "rise", "surge", "bullish", "profit", "breakthrough", "success",
    "upgrade", "positive", "growth", "rally", "outperform", "beat", "exceed",
    "momentum", "adoption", "approve", "launch", "partnership", "innovation",
    "recovery", "boom", "upside", "breakout", "all-time high", "record",
})
NEGATIVE_WORDS = frozenset({
    "fall", "drop", "crash", "bearish", "loss", "downgrade", "scandal",
    "lawsuit", "negative", "decline", "recession", "sell-off", "underperform",
    "miss", "delay", "ban", "crackdown", "volatile", "risk", "warning",
    "debt", "default", "bankrupt", "fraud", "hack", "exploit", "fear",
})

QUERY_TEMPLATES = [
    "{question}",
    "{topic} forecast",
    "{topic} price prediction",
    "{topic} news",
    "{topic} analysis",
    "{topic} outlook",
]


@dataclass
class SearchResult:
    source: str
    title: str
    snippet: str
    url: str
    relevance_score: float = 0.5

    @property
    def text(self) -> str:
        return f"{self.title} {self.snippet}"


@dataclass
class KnowledgeGapAssessment:
    market_question: str
    market_probability: float
    evidence_score: float
    evidence_count: int
    sentiment_label: str
    gap: float
    direction: str
    confidence: float
    sources_used: List[str]
    top_results: List[Dict[str, str]] = field(default_factory=list)

    @property
    def gap_pct(self) -> float:
        return self.gap * 100

    @property
    def is_significant(self) -> bool:
        return abs(self.gap) > 0.10 and self.confidence > 0.25

    def to_signal_dict(self) -> Dict[str, Any]:
        return {
            "symbol": "?",
            "action": "BUY" if self.direction == "undervalued" else "SELL",
            "base_confidence": round(self.confidence, 3),
            "final_confidence": round(self.confidence, 3),
            "opportunity_score": round(abs(self.gap) * self.confidence, 4),
            "strategy_name": "KnowledgeGap",
            "signal_reason": (
                f"Knowledge gap: market={self.market_probability*100:.0f}% vs "
                f"evidence={self.evidence_score*100:.0f}% "
                f"({self.direction}, gap={self.gap_pct:.0f}%)"
            ),
            "estimated_volume_usd": 0,
            "market_data": {
                "question": self.market_question,
                "market_probability": self.market_probability,
                "evidence_score": self.evidence_score,
                "gap": self.gap,
                "evidence_count": self.evidence_count,
                "sentiment": self.sentiment_label,
                "sources": self.sources_used,
            },
        }


class SentimentAnalyzer:
    """Keyword-based sentiment analysis matching the existing codebase pattern."""

    @staticmethod
    def analyze(text: str) -> Tuple[float, str]:
        text_lower = text.lower()
        pos = sum(1 for w in POSITIVE_WORDS if w in text_lower)
        neg = sum(1 for w in NEGATIVE_WORDS if w in text_lower)
        total = pos + neg
        score = (pos - neg) / max(1, total)
        label = "positive" if score > 0.15 else "negative" if score < -0.15 else "neutral"
        return round(score, 3), label

    @staticmethod
    def aggregate(scores: List[float]) -> Tuple[float, str]:
        if not scores:
            return 0.5, "neutral"
        avg = sum(scores) / len(scores)
        label = "positive" if avg > 0.15 else "negative" if avg < -0.15 else "neutral"
        return round(avg, 3), label


class WebResearcher:
    """Searches Wikipedia for factual information about a given topic."""

    def __init__(self, timeout: int = 10):
        self._timeout = timeout
        self._session = None

    def _get_session(self):
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": "PortfolioOptimizer/1.0 (knowledge-gap analysis)",
            })
        return self._session

    def search(self, query: str, max_results: int = 5) -> List[SearchResult]:
        """Search Wikipedia for articles matching query."""
        session = self._get_session()
        results = []
        try:
            params = {
                "action": "query",
                "list": "search",
                "srsearch": query,
                "srlimit": max_results,
                "format": "json",
            }
            resp = session.get(WIKI_API, params=params, timeout=self._timeout)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("query", {}).get("search", []):
                page_title = item.get("title", "")
                snippet = item.get("snippet", "")
                snippet = re.sub(r"<[^>]+>", "", snippet)
                page_id = item.get("pageid", 0)
                results.append(SearchResult(
                    source="wikipedia",
                    title=page_title,
                    snippet=snippet,
                    url=f"https://en.wikipedia.org/wiki/{page_title.replace(' ', '_')}",
                    relevance_score=1.0 - (item.get("index", 1) - 1) * 0.1,
                ))
                if page_id:
                    extract = self._get_page_extract(page_id, session)
                    if extract and len(extract) > len(snippet):
                        results[-1].snippet = extract[:500]
        except Exception as e:
            logger.debug("Wikipedia search failed for '%s': %s", query, e)
        return results

    def _get_page_extract(self, page_id: int, session) -> str:
        try:
            params = {
                "action": "query",
                "prop": "extracts",
                "exintro": True,
                "explaintext": True,
                "pageids": page_id,
                "format": "json",
            }
            resp = session.get(WIKI_API, params=params, timeout=self._timeout)
            resp.raise_for_status()
            data = resp.json()
            pages = data.get("query", {}).get("pages", {})
            page = pages.get(str(page_id), {})
            return page.get("extract", "")
        except Exception:
            return ""


class NewsResearcher:
    """Searches news RSS feeds for articles mentioning a given topic."""

    def __init__(self, timeout: int = 10, cache_ttl: int = 300):
        self._timeout = timeout
        self._cache: Dict[str, Tuple[float, List[SearchResult]]] = {}
        self._cache_ttl = cache_ttl

    def search(self, query: str, max_results: int = 10) -> List[SearchResult]:
        """Fetch RSS feeds and filter articles matching the query keywords."""
        keywords = self._extract_keywords(query)
        results = []
        for feed_url, source_name in RSS_FEEDS:
            articles = self._fetch_feed(feed_url, source_name)
            for art in articles:
                text = f"{art.get('title', '')} {art.get('description', '')}"
                matched = sum(1 for kw in keywords if kw in text.lower())
                if matched > 0 and keywords:
                    results.append(SearchResult(
                        source=source_name,
                        title=art.get("title", ""),
                        snippet=art.get("description", "")[:300],
                        url=art.get("link", ""),
                        relevance_score=min(matched / len(keywords), 1.0),
                    ))
        results.sort(key=lambda r: r.relevance_score, reverse=True)
        return results[:max_results]

    def _extract_keywords(self, query: str) -> List[str]:
        """Extract meaningful keywords from a search query."""
        stopwords = {
            "the", "a", "an", "is", "will", "be", "to", "for", "of", "in",
            "on", "at", "by", "with", "about", "what", "when", "where",
            "how", "does", "do", "are", "was", "were", "has", "have", "had",
            "not", "no", "but", "or", "and", "if", "then", "than", "that",
            "this", "these", "those", "its", "it's", "it", "from",
        }
        tokens = re.findall(r"[a-z0-9]+", query.lower())
        meaningful = [t for t in tokens if t not in stopwords and len(t) > 2]
        return meaningful[:8]

    def _fetch_feed(self, feed_url: str, source_name: str) -> List[Dict[str, str]]:
        """Fetch and parse an RSS feed with caching."""
        now = time.time()
        if feed_url in self._cache:
            ts, articles = self._cache[feed_url]
            if now - ts < self._cache_ttl:
                return articles
        try:
            import requests
            resp = requests.get(feed_url, timeout=self._timeout,
                                headers={"User-Agent": "Mozilla/5.0 (PortfolioOptimizer/1.0)"})
            resp.raise_for_status()
            articles = self._parse_rss(resp.text, source_name)
            self._cache[feed_url] = (now, articles)
            return articles
        except Exception as e:
            logger.debug("RSS fetch failed for %s: %s", source_name, e)
            return []

    @staticmethod
    def _parse_rss(xml: str, source: str) -> List[Dict[str, str]]:
        articles = []
        try:
            root = ET.fromstring(xml)
            for item in root.findall(".//item"):
                title = item.findtext("title", "")
                link = item.findtext("link", "")
                desc = item.findtext("description", "")
                desc = re.sub(r"<[^>]+>", "", desc) if desc else ""
                articles.append({
                    "title": title,
                    "link": link,
                    "description": desc,
                    "source": source,
                })
        except Exception as e:
            logger.debug("RSS parse failed for %s: %s", source, e)
        return articles


def _extract_topics(question: str) -> List[str]:
    """Extract search topics from a prediction market question.

    Examples:
        "Will BTC reach $100k by June 2026?" → ["bitcoin price prediction 2026"]
        "Will Trump win the 2024 election?" → ["trump 2024 election odds"]
        "Will inflation be above 3% in 2025?" → ["inflation 2025 forecast"]
    """
    q = question.lower()
    q = re.sub(r"[^\w\s]", " ", q)
    q = re.sub(r"\s+", " ", q).strip()

    # Remove leading "will", "does", "is", "are", "can", "has"
    q = re.sub(r"^(will|does|is|are|can|has|have|do)\s+", "", q)
    # Remove trailing question words
    q = re.sub(r"\s+(by|before|in|at|on|after)\s+\S+\s*$", "", q)
    # Remove trailing date-like patterns
    q = re.sub(r"\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}\s*$", "", q, flags=re.I)

    tokens = q.split()
    topics = []

    # Full question as-is
    topics.append(q)

    # Try to find the core entity + predicate
    if len(tokens) > 2:
        topics.append(" ".join(tokens[:3]))

    # Just the first noun phrase
    if len(tokens) > 2:
        # Remove leading articles/determiners
        i = 0
        while i < len(tokens) and tokens[i] in ("the", "a", "an", "this", "that", "these", "those"):
            i += 1
        if i < len(tokens):
            topics.append(" ".join(tokens[i:i+3]))

    # Deduplicate
    seen = set()
    unique = []
    for t in topics:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return unique


class KnowledgeGapAnalyzer:
    """Orchestrates web/news search, sentiment analysis, and gap detection.

    For each prediction market:
      1. Extract topics from the question
      2. Search Wikipedia + news RSS for relevant content
      3. Compute aggregate evidence score (0-1, where >0.5 = bullish)
      4. Compare evidence score against market probability
      5. Return assessment if the gap is significant
    """

    def __init__(
        self,
        enable_web_search: bool = True,
        enable_news_search: bool = True,
        min_gap: float = 0.10,
        min_evidence: int = 2,
        cache_ttl: int = 300,
    ):
        self._web = WebResearcher() if enable_web_search else None
        self._news = NewsResearcher(cache_ttl=cache_ttl) if enable_news_search else None
        self._sentiment = SentimentAnalyzer()
        self.min_gap = min_gap
        self.min_evidence = min_evidence

    def analyze(self, market: PredictionMarket) -> Optional[KnowledgeGapAssessment]:
        """Run knowledge gap analysis on a single prediction market."""
        return self.analyze_question(market.question, market.mid_price, market.platform)

    def analyze_question(
        self,
        question: str,
        market_probability: float,
        platform: str = "",
    ) -> Optional[KnowledgeGapAssessment]:
        """Run knowledge gap analysis on a question with a given market probability."""
        topics = _extract_topics(question)
        if not topics:
            return None

        all_results: List[SearchResult] = []

        for topic in topics[:3]:
            if self._web:
                try:
                    all_results.extend(self._web.search(topic))
                except Exception as e:
                    logger.debug("Web search failed for topic '%s': %s", topic, e)
            if self._news:
                try:
                    all_results.extend(self._news.search(topic))
                except Exception as e:
                    logger.debug("News search failed for topic '%s': %s", topic, e)

        # Deduplicate by URL
        seen_urls = set()
        unique_results = []
        for r in all_results:
            if r.url and r.url not in seen_urls:
                seen_urls.add(r.url)
                unique_results.append(r)

        if len(unique_results) < self.min_evidence:
            logger.debug(
                "Insufficient evidence for '%s': %d results (need %d)",
                question[:60], len(unique_results), self.min_evidence,
            )
            return None

        # Score each result
        scores = []
        for r in unique_results:
            s, _ = self._sentiment.analyze(r.text)
            scores.append(s * r.relevance_score)

        evidence_score, sentiment_label = self._sentiment.aggregate(scores)

        # Normalize evidence_score from [-1, 1] to [0, 1]
        evidence_probability = (evidence_score + 1) / 2

        gap = evidence_probability - market_probability

        direction = "overvalued" if gap < -self.min_gap else "undervalued" if gap > self.min_gap else "fair"

        confidence = min(
            abs(gap) * 2,
            len(unique_results) / 10,
            0.9,
        )

        sources = list(dict.fromkeys(r.source for r in unique_results))
        top = [
            {"title": r.title[:100], "source": r.source, "url": r.url[:200]}
            for r in unique_results[:5]
        ]

        assessment = KnowledgeGapAssessment(
            market_question=question,
            market_probability=market_probability,
            evidence_score=evidence_probability,
            evidence_count=len(unique_results),
            sentiment_label=sentiment_label,
            gap=gap,
            direction=direction,
            confidence=confidence,
            sources_used=sources,
            top_results=top,
        )

        logger.info(
            "Knowledge gap: '%s' market=%.0f%% evidence=%.0f%% gap=%+.0f%% "
            "(%s, %d results, %.0f%% conf)",
            question[:60], market_probability * 100,
            evidence_probability * 100, gap * 100,
            direction, len(unique_results), confidence * 100,
        )

        return assessment

    def analyze_markets(
        self,
        markets: List[PredictionMarket],
        max_analyze: int = 5,
    ) -> List[KnowledgeGapAssessment]:
        """Analyze a batch of prediction markets, most liquid first."""
        ranked = sorted(markets, key=lambda m: m.volume, reverse=True)
        assessments = []
        for market in ranked[:max_analyze]:
            try:
                a = self.analyze(market)
                if a:
                    assessments.append(a)
            except Exception as e:
                logger.warning("Knowledge gap analysis failed for '%s': %s",
                               market.question[:60], e)
        return assessments


def main():
    """CLI: analyze knowledge gaps for prediction markets."""
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Knowledge Gap Analyzer")
    parser.add_argument("--question", help="Analyze a specific question (overrides market fetch)")
    parser.add_argument("--probability", type=float, default=0.5, help="Market probability (0-1)")
    parser.add_argument("--min-gap", type=float, default=0.10, help="Minimum gap threshold")
    parser.add_argument("--min-evidence", type=int, default=2, help="Minimum evidence items")
    parser.add_argument("--kalshi-email", help="Kalshi email (env: KALSHI_EMAIL)")
    parser.add_argument("--kalshi-password", help="Kalshi password (env: KALSHI_PASSWORD)")
    parser.add_argument("--no-web", action="store_true", help="Disable web search")
    parser.add_argument("--no-news", action="store_true", help="Disable news search")
    parser.add_argument("--batch", type=int, default=5, help="Max markets to analyze in batch")
    args = parser.parse_args()

    analyzer = KnowledgeGapAnalyzer(
        enable_web_search=not args.no_web,
        enable_news_search=not args.no_news,
        min_gap=args.min_gap,
        min_evidence=args.min_evidence,
    )

    if args.question:
        assessment = analyzer.analyze_question(args.question, args.probability)
        if assessment:
            print(f"Question: {assessment.market_question}")
            print(f"Market probability: {assessment.market_probability*100:.1f}%")
            print(f"Evidence probability: {assessment.evidence_score*100:.1f}%")
            print(f"Gap: {assessment.gap_pct:+.1f}% ({assessment.direction})")
            print(f"Confidence: {assessment.confidence*100:.0f}%")
            print(f"Evidence count: {assessment.evidence_count}")
            print(f"Sources: {', '.join(assessment.sources_used)}")
            print(f"Sentiment: {assessment.sentiment_label}")
            if assessment.top_results:
                print("\nTop results:")
                for r in assessment.top_results:
                    print(f"  [{r['source']}] {r['title']}")
                    print(f"    {r['url']}")
        else:
            print("No significant knowledge gap found.")
        return

    from .unified_client import UnifiedPredictionMarketClient
    client = UnifiedPredictionMarketClient(
        kalshi_email=args.kalshi_email or os.environ.get("KALSHI_EMAIL", ""),
        kalshi_password=args.kalshi_password or os.environ.get("KALSHI_PASSWORD", ""),
    )
    markets = client.get_crypto_markets(limit=30)
    print(f"Fetched {len(markets)} prediction markets, analyzing top {args.batch}...\n")

    assessments = analyzer.analyze_markets(markets, max_analyze=args.batch)
    if not assessments:
        print("No significant knowledge gaps found.")
        return

    print(f"Found {len(assessments)} knowledge gaps:\n")
    for a in sorted(assessments, key=lambda x: abs(x.gap), reverse=True):
        print(f"  {a.market_question[:80]}")
        print(f"    Market: {a.market_probability*100:.0f}%  "
              f"Evidence: {a.evidence_score*100:.0f}%  "
              f"Gap: {a.gap_pct:+.0f}% ({a.direction})  "
              f"Conf: {a.confidence*100:.0f}%  "
              f"Sources: {', '.join(a.sources_used)}")
        print()


if __name__ == "__main__":
    main()
