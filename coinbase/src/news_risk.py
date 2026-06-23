from __future__ import annotations
import json
import os
import math
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum

from .protocols import Direction, Opportunity, BaseStrategy

log = logging.getLogger(__name__)

KG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..",
    "graph-alpha-bot", "app", "data", "knowledge_graph.json"
)

BREAKING_TOPICS_HEAVY = {"hacks_security", "regulation"}
SENTIMENT_PENALTY_THRESHOLD = -0.2
BREAKING_RATIO_ALERT = 0.3
HACK_KEYWORDS = {"hack", "exploit", "breach", "scam", "fraud", "theft", "attack"}
REGULATION_KEYWORDS = {"sec", "regulation", "ban", "restrict", "crackdown", "lawsuit", "fine"}


@dataclass
class NewsRiskSnapshot:
    product_id: str
    article_count: int = 0
    breaking_count: int = 0
    breaking_ratio: float = 0.0
    avg_sentiment: float = 0.0
    has_hacks: bool = False
    has_regulation: bool = False
    hack_article_count: int = 0
    regulation_article_count: int = 0
    sentiment_risk_score: float = 0.5
    size_multiplier: float = 1.0
    stop_distance_multiplier: float = 1.0
    confidence_penalty: float = 0.0
    leverage_cap: float = 3.0
    var_adjustment: float = 1.0
    reason: str = "no news data"


class KnowledgeGraphReader:
    def __init__(self, path: str = KG_PATH, cache_ttl_secs: float = 60.0):
        self.path = path
        self.cache_ttl = cache_ttl_secs
        self._cache: Optional[Dict] = None
        self._cache_time: float = 0.0
        self._article_cache: List[Dict] = []

    def read(self) -> Dict:
        now = __import__("time").time()
        if self._cache and now - self._cache_time < self.cache_ttl:
            return self._cache
        if not os.path.exists(self.path):
            log.warning("knowledge_graph.json not found at %s", self.path)
            self._cache = {"tickers": {}, "articles": [], "metadata": {}}
            self._cache_time = now
            return self._cache
        try:
            with open(self.path) as f:
                data = json.load(f)
            if not isinstance(data, dict):
                data = {"articles": list(data) if isinstance(data, list) else [], "tickers": {}, "metadata": {}}
            self._cache = data
            self._cache_time = now
            return data
        except Exception as e:
            log.warning("Failed to read knowledge_graph.json: %s", e)
            self._cache = self._cache or {"tickers": {}, "articles": [], "metadata": {}}
            return self._cache

    def get_ticker_sentiment(self, ticker: str) -> Optional[Dict]:
        data = self.read()
        tickers = data.get("tickers", {})
        ticker_up = ticker.upper()
        for sym, info in tickers.items():
            if sym.upper() == ticker_up:
                return info
        articles = data.get("articles", [])
        relevant = [a for a in articles if ticker_up in [t.upper() for t in a.get("tickers", [])]]
        if not relevant:
            return None
        sentiments = [a.get("sentiment_score", 0.5) for a in relevant if isinstance(a.get("sentiment_score"), (int, float))]
        avg_s = sum(sentiments) / len(sentiments) if sentiments else 0.5
        hack_count = sum(1 for a in relevant if any(k in (a.get("title", "") + a.get("summary", "")).lower() for k in HACK_KEYWORDS))
        reg_count = sum(1 for a in relevant if any(k in (a.get("title", "") + a.get("summary", "")).lower() for k in REGULATION_KEYWORDS))
        breaking = sum(1 for a in relevant if a.get("is_breaking", False))
        return {
            "count": len(relevant), "avg_sentiment": avg_s,
            "breaking_count": breaking, "hack_count": hack_count,
            "regulation_count": reg_count,
            "topics": list(set(a.get("topic", "") for a in relevant if a.get("topic"))),
        }

    def get_breaking_topics(self) -> List[str]:
        data = self.read()
        return data.get("metadata", {}).get("breaking_topics", [])

    def global_sentiment_pulse(self) -> float:
        data = self.read()
        tickers = data.get("tickers", {})
        sentiments = [v.get("avg_sentiment", 0.5) for v in tickers.values() if isinstance(v, dict)]
        if not sentiments:
            return 0.5
        return sum(sentiments) / len(sentiments)


class MCPSentimentClient:
    def __init__(self, host: str = "localhost", port: int = 8080,
                 timeout_secs: float = 2.0):
        self.base_url = f"http://{host}:{port}"
        self.timeout = timeout_secs

    def query_sentiment(self, symbol: str) -> Optional[Dict]:
        try:
            import urllib.request
            url = f"{self.base_url}/query/sentiment?symbol={symbol}"
            req = urllib.request.Request(url)
            resp = urllib.request.urlopen(req, timeout=self.timeout)
            data = json.loads(resp.read().decode())
            results = data.get("results", [])
            if results:
                return results[-1]
            return None
        except Exception as e:
            log.debug("MCP sentiment query failed for %s: %s", symbol, e)
            return None

    def query_news(self, symbol: str, days: int = 1) -> Optional[List[Dict]]:
        try:
            import urllib.request
            url = f"{self.base_url}/query/news?symbol={symbol}&days={days}"
            req = urllib.request.Request(url)
            resp = urllib.request.urlopen(req, timeout=self.timeout)
            data = json.loads(resp.read().decode())
            return data.get("results", [])
        except Exception:
            return None

    def is_available(self) -> bool:
        try:
            import urllib.request
            req = urllib.request.Request(f"{self.base_url}/health")
            resp = urllib.request.urlopen(req, timeout=self.timeout)
            return resp.status == 200
        except Exception:
            return False


class NewsRiskAdjuster:
    def __init__(self, kg_reader: Optional[KnowledgeGraphReader] = None,
                 mcp_client: Optional[MCPSentimentClient] = None,
                 enable_mcp: bool = False):
        self.kg = kg_reader or KnowledgeGraphReader()
        self.mcp = mcp_client or MCPSentimentClient()
        self.enable_mcp = enable_mcp
        self._mcp_available: Optional[bool] = None

    def assess_product(self, product_id: str) -> NewsRiskSnapshot:
        kg_info = self.kg.get_ticker_sentiment(product_id)
        mcp_sentiment = None
        if self.enable_mcp:
            if self._mcp_available is None:
                self._mcp_available = self.mcp.is_available()
            if self._mcp_available:
                mcp_sentiment = self.mcp.query_sentiment(product_id)

        avg_sentiment = 0.5
        article_count = 0
        breaking_count = 0
        topics = []
        hack_count = 0
        reg_count = 0

        if kg_info:
            avg_sentiment = kg_info.get("avg_sentiment", 0.5)
            article_count = kg_info.get("count", 0)
            breaking_count = kg_info.get("breaking_count", 0)
            topics = kg_info.get("topics", [])
            hack_count = kg_info.get("hack_count", 0)
            reg_count = kg_info.get("regulation_count", 0)

        if mcp_sentiment and isinstance(mcp_sentiment, dict):
            mc_avg = mcp_sentiment.get("avg_score", None)
            if mc_avg is not None:
                avg_sentiment = (avg_sentiment + mc_avg) / 2
            mc_count = mcp_sentiment.get("article_count", 0)
            article_count = max(article_count, mc_count)

        breaking_ratio = breaking_count / max(article_count, 1)
        has_hacks = "hacks_security" in topics or hack_count > 0
        has_regulation = "regulation" in topics or reg_count > 0

        sentiment_risk_score = self._compute_sentiment_risk(
            avg_sentiment, breaking_ratio, has_hacks, has_regulation, article_count
        )

        size_mult = self._size_multiplier(avg_sentiment, breaking_ratio, has_hacks)
        stop_mult = self._stop_multiplier(avg_sentiment, has_hacks, has_regulation)
        conf_penalty = self._confidence_penalty(avg_sentiment, breaking_ratio, has_hacks)
        lev_cap = self._leverage_cap(has_hacks, has_regulation, breaking_ratio)
        var_adj = self._var_adjustment(avg_sentiment, breaking_ratio, has_hacks)

        reason_parts = []
        if article_count > 0:
            reason_parts.append(f"news_sent={avg_sentiment:+.2f}")
        if breaking_ratio > BREAKING_RATIO_ALERT:
            reason_parts.append(f"breaking={breaking_count}")
        if has_hacks:
            reason_parts.append("HACK_ALERT")
        if has_regulation:
            reason_parts.append(f"REG_ALERT(reg_count={reg_count})")

        return NewsRiskSnapshot(
            product_id=product_id,
            article_count=article_count,
            breaking_count=breaking_count,
            breaking_ratio=round(breaking_ratio, 3),
            avg_sentiment=round(avg_sentiment, 3),
            has_hacks=has_hacks,
            has_regulation=has_regulation,
            hack_article_count=hack_count,
            regulation_article_count=reg_count,
            sentiment_risk_score=round(sentiment_risk_score, 3),
            size_multiplier=round(size_mult, 3),
            stop_distance_multiplier=round(stop_mult, 3),
            confidence_penalty=round(conf_penalty, 3),
            leverage_cap=round(lev_cap, 1),
            var_adjustment=round(var_adj, 3),
            reason=" | ".join(reason_parts) if reason_parts else "no news data",
        )

    @staticmethod
    def _compute_sentiment_risk(sentiment: float, breaking_ratio: float,
                                 has_hacks: bool, has_regulation: bool,
                                 article_count: int) -> float:
        base = 0.5
        if sentiment < SENTIMENT_PENALTY_THRESHOLD:
            base += abs(sentiment) * 2.0
        elif sentiment > 0.3:
            base -= sentiment * 0.5

        if breaking_ratio > BREAKING_RATIO_ALERT:
            base += breaking_ratio * 1.5

        if has_hacks:
            base += 0.3
        if has_regulation:
            base += 0.15

        if article_count == 0:
            base = 0.3

        return max(0.0, min(1.0, base))

    @staticmethod
    def _size_multiplier(sentiment: float, breaking_ratio: float,
                         has_hacks: bool) -> float:
        mult = 1.0
        if sentiment < SENTIMENT_PENALTY_THRESHOLD:
            mult -= abs(sentiment) * 1.5
        if breaking_ratio > BREAKING_RATIO_ALERT:
            mult -= breaking_ratio * 1.2
        if has_hacks:
            mult -= 0.5
        return max(0.3, mult)

    @staticmethod
    def _stop_multiplier(sentiment: float, has_hacks: bool,
                         has_regulation: bool) -> float:
        mult = 1.0
        if sentiment < SENTIMENT_PENALTY_THRESHOLD:
            mult += abs(sentiment) * 1.5
        if has_hacks:
            mult += 1.0
        if has_regulation:
            mult += 0.5
        return min(3.0, mult)

    @staticmethod
    def _confidence_penalty(sentiment: float, breaking_ratio: float,
                            has_hacks: bool) -> float:
        penalty = 0.0
        if sentiment < SENTIMENT_PENALTY_THRESHOLD:
            penalty += abs(sentiment) * 1.5
        if breaking_ratio > BREAKING_RATIO_ALERT:
            penalty += 0.15
        if has_hacks:
            penalty += 0.3
        return min(0.5, penalty)

    @staticmethod
    def _leverage_cap(has_hacks: bool, has_regulation: bool,
                      breaking_ratio: float) -> float:
        cap = 3.0
        if has_hacks:
            cap = 1.0
        elif has_regulation:
            cap = 1.5
        elif breaking_ratio > BREAKING_RATIO_ALERT:
            cap = 2.0
        return cap

    @staticmethod
    def _var_adjustment(sentiment: float, breaking_ratio: float,
                        has_hacks: bool) -> float:
        adj = 1.0
        if sentiment < SENTIMENT_PENALTY_THRESHOLD:
            adj += abs(sentiment)
        if breaking_ratio > BREAKING_RATIO_ALERT:
            adj += 0.5
        if has_hacks:
            adj += 1.0
        return min(3.0, adj)

    def adjust_opportunity(self, opp: Opportunity) -> Opportunity:
        assessment = self.assess_product(opp.product_id)

        if assessment.article_count > 0:
            opp.base_size = max(0.0, opp.base_size * assessment.size_multiplier)
            original_stop = opp.entry_price - opp.stop_price if opp.direction == Direction.LONG else opp.stop_price - opp.entry_price
            if opp.direction == Direction.LONG:
                opp.stop_price = opp.entry_price - abs(original_stop) * assessment.stop_distance_multiplier
                opp.target_price = opp.entry_price + abs(opp.target_price - opp.entry_price)
            else:
                opp.stop_price = opp.entry_price + abs(original_stop) * assessment.stop_distance_multiplier
                opp.target_price = opp.entry_price - abs(opp.target_price - opp.entry_price)

            opp.confidence = max(0.0, opp.confidence - assessment.confidence_penalty)

            rr = abs(opp.target_price - opp.entry_price) / max(abs(opp.entry_price - opp.stop_price), 1e-9)
            opp.risk_reward = round(rr, 2)

            opp.leverage = min(opp.leverage, assessment.leverage_cap)

            opp.meta["news_risk"] = {
                "sentiment_risk_score": assessment.sentiment_risk_score,
                "size_multiplier": assessment.size_multiplier,
                "stop_distance_multiplier": assessment.stop_distance_multiplier,
                "confidence_penalty": assessment.confidence_penalty,
                "leverage_cap": assessment.leverage_cap,
                "var_adjustment": assessment.var_adjustment,
                "avg_sentiment": assessment.avg_sentiment,
                "breaking_ratio": assessment.breaking_ratio,
                "has_hacks": assessment.has_hacks,
                "has_regulation": assessment.has_regulation,
                "article_count": assessment.article_count,
                "reason": assessment.reason,
            }

            if assessment.has_hacks or (assessment.sentiment_risk_score > 0.7):
                opp.reason = f"[NEWS_RISK] {assessment.reason} | {opp.reason}"
                opp.score *= 0.5

        return opp

    def adjust_profile(self, profile) -> None:
        pulse = self.kg.global_sentiment_pulse()
        breaking_topics = self.kg.get_breaking_topics()

        active_heavy = [t for t in breaking_topics if t in BREAKING_TOPICS_HEAVY]
        if "hacks_security" in active_heavy:
            profile.max_position_pct = min(profile.max_position_pct, 0.10)
            profile.max_notional_per_trade = min(profile.max_notional_per_trade, 2000.0)
            profile.risk_per_trade_pct = min(profile.risk_per_trade_pct, 0.005)
            log.warning("Global hacks/security breaking news — risk capped aggressively")

        if pulse < SENTIMENT_PENALTY_THRESHOLD:
            profile.max_leverage = min(profile.max_leverage, 1.5)
            profile.max_position_pct = min(profile.max_position_pct, 0.12)
            log.info("Global sentiment pulse %.2f — risk tightened", pulse)

    def summary(self) -> Dict:
        data = self.kg.read()
        tickers = data.get("tickers", {})
        assessments = {}
        for sym in tickers:
            assessments[sym] = self.assess_product(sym)
        return {
            "global_sentiment_pulse": round(self.kg.global_sentiment_pulse(), 3),
            "breaking_topics": self.kg.get_breaking_topics(),
            "total_articles": data.get("metadata", {}).get("total_articles", 0),
            "products_assessed": {
                sym: {
                    "sentiment_risk_score": a.sentiment_risk_score,
                    "avg_sentiment": a.avg_sentiment,
                    "breaking_ratio": a.breaking_ratio,
                    "reason": a.reason,
                }
                for sym, a in assessments.items()
            },
        }


class NewsAwareRiskStrategy(BaseStrategy):
    def __init__(self, adjuster: Optional[NewsRiskAdjuster] = None):
        self.adjuster = adjuster or NewsRiskAdjuster()
        self._name = "news_risk"

    def name(self) -> str:
        return self._name

    def on_bar(self, bar, history) -> None:
        return None

    def adjust_opportunity(self, opp: Opportunity) -> Opportunity:
        return self.adjuster.adjust_opportunity(opp)
