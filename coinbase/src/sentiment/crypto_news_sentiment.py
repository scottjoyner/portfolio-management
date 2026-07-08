import time
import hashlib
import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from xml.etree import ElementTree
from urllib.request import urlopen, Request

from ..resilience import SourceCircuitBreaker, retry_call

logger = logging.getLogger(__name__)

CRYPTO_FEEDS: Dict[str, str] = {
    "coindesk": "https://www.coindesk.com/arc/rss/articles?topic=Cryptocurrency",
    "cointelegraph": "https://cointelegraph.com/news/feed",
    "cryptoslate": "https://cryptoslate.com/feed/",
    "theblock": "https://www.theblock.co/rss.xml",
    "decrypt": "https://decrypt.co/feed",
    "bitcoinmagazine": "https://bitcoinmagazine.com/feed",
}

KNOWN_CRYPTO_SYMBOLS: Dict[str, str] = {
    "BTC": "BTC-USD", "BITCOIN": "BTC-USD", "ETH": "ETH-USD", "ETHEREUM": "ETH-USD",
    "SOL": "SOL-USD", "SOLANA": "SOL-USD", "XRP": "XRP-USD", "ADA": "ADA-USD",
    "CARDANO": "ADA-USD", "DOT": "DOT-USD", "POLKADOT": "DOT-USD", "DOGE": "DOGE-USD",
    "DOGECOIN": "DOGE-USD", "AVAX": "AVAX-USD", "AVALANCHE": "AVAX-USD",
    "LINK": "LINK-USD", "CHAINLINK": "LINK-USD", "UNI": "UNI-USD", "UNISWAP": "UNI-USD",
    "MATIC": "POL-USD", "POLYGON": "POL-USD", "POL": "POL-USD",
    "ATOM": "ATOM-USD", "COSMOS": "ATOM-USD",
    "LTC": "LTC-USD", "LITECOIN": "LTC-USD", "BCH": "BCH-USD", "BITCOINCASH": "BCH-USD",
    "FIL": "FIL-USD", "FILECOIN": "FIL-USD", "NEAR": "NEAR-USD",
    "APT": "APT-USD", "APTOS": "APT-USD",
    "SUI": "SUI-USD", "ARB": "ARB-USD", "ARBITRUM": "ARB-USD",
    "OP": "OP-USD", "OPTIMISM": "OP-USD",
    "SEI": "SEI-USD", "INJ": "INJ-USD", "INJECTIVE": "INJ-USD",
    "TIA": "TIA-USD", "CELESTIA": "TIA-USD",
    "ALGO": "ALGO-USD", "ALGORAND": "ALGO-USD",
    "XLM": "XLM-USD", "STELLAR": "XLM-USD",
    "STX": "STX-USD", "STACKS": "STX-USD",
    "HBAR": "HBAR-USD", "HEDERA": "HBAR-USD",
    "ICP": "ICP-USD",
    "GRT": "GRT-USD",
    "SHIB": "SHIB-USD", "SHIBA": "SHIB-USD",
    "PEPE": "PEPE-USD", "BONK": "BONK-USD",
    "TRUMP": "TRUMP-USD", "FLOKI": "FLOKI-USD",
}

POSITIVE_WORDS: Set[str] = {
    "gain", "gains", "rising", "rise", "surge", "bullish", "profit", "profits",
    "breakthrough", "success", "upgrade", "positive", "growth", "growing",
    "rally", "outperform", "beat", "exceed", "momentum", "adoption",
    "approve", "approved", "approval", "launch", "launches", "launched",
    "partnership", "partner", "innovation", "recovery", "boom", "upside",
    "breakout", "record", "milestone", "soar", "soars", "soaring", "jump",
    "jumps", "jumped", "strong", "stronger", "opportunity", "opportunities",
    "expand", "expansion", "expanding", "institutional", "mainstream",
    "bullrun", "halving", "breakthrough", "favorable", "optimistic",
    "acquire", "acquired", "acquisition", "all-time", "ath",
}

NEGATIVE_WORDS: Set[str] = {
    "fall", "falls", "falling", "drop", "drops", "dropping", "crash", "crashes",
    "crashed", "bearish", "loss", "losses", "downgrade", "scandal",
    "lawsuit", "negative", "decline", "declining", "recession", "sell-off",
    "selloff", "underperform", "miss", "delay", "delays", "delayed",
    "ban", "bans", "banned", "crackdown", "volatile", "volatility", "risk",
    "risks", "risky", "warning", "debt", "default", "bankrupt", "bankruptcy",
    "fraud", "hack", "hacks", "hacked", "hacking", "exploit", "exploits",
    "exploited", "fear", "plunge", "plunges", "plunged", "slump", "slumps",
    "slumped", "liquidation", "panic", "investigation", "investigate",
    "scam", "rug", "breach", "breached", "theft", "stolen", "steal",
    "malware", "ransomware", "sanction", "sanctions", "regulatory",
    "restrict", "restriction", "restricted", "fine", "fines", "fined",
    "investigat", "controversy", "cancel", "canceled", "cancellation",
    "threat", "threats", "weak", "weakness", "weakening",
}


@dataclass
class NewsArticle:
    title: str
    source: str
    url: str
    published: str = ""
    sentiment_score: float = 0.0
    symbol: str = ""


@dataclass
class SentimentSignal:
    product_id: str
    action: str
    confidence: float
    sentiment_score: float
    article_count: int
    top_headline: str
    source_breakdown: Dict[str, int] = field(default_factory=dict)

    def to_opportunity(self) -> Dict:
        return {
            "action": self.action,
            "strategy": "crypto_news",
            "confidence": round(self.confidence, 3),
            "reason": f"news:{self.sentiment_score:.2f} ({self.article_count} articles): {self.top_headline[:80]}",
            "product_id": self.product_id,
            "sentiment_score": self.sentiment_score,
            "article_count": self.article_count,
            "top_headline": self.top_headline,
            "source_breakdown": self.source_breakdown,
        }


class CryptoNewsSentiment:
    def __init__(self, cache_ttl: int = 300):
        self._cache_ttl = cache_ttl
        self._articles: Dict[str, List[NewsArticle]] = {}
        self._last_fetch: float = 0.0
        self._lock = threading.Lock()
        self._seen_hashes: Set[str] = set()
        self._feed_breakers: Dict[str, SourceCircuitBreaker] = {
            source: SourceCircuitBreaker(f"news:{source}", failure_threshold=3, reset_timeout_s=300.0)
            for source in CRYPTO_FEEDS
        }

    def refresh(self) -> None:
        with self._lock:
            now = time.time()
            if now - self._last_fetch < self._cache_ttl:
                return
            try:
                all_articles: List[NewsArticle] = []
                for source, url in CRYPTO_FEEDS.items():
                    breaker = self._feed_breakers.setdefault(
                        source, SourceCircuitBreaker(f"news:{source}", failure_threshold=3, reset_timeout_s=300.0)
                    )
                    if not breaker.allow():
                        logger.debug("News feed %s skipped due to open circuit breaker", source)
                        continue
                    try:
                        articles = self._fetch_feed(source, url)
                        all_articles.extend(articles)
                        breaker.on_success()
                    except Exception as e:
                        breaker.on_failure(e)
                        logger.debug("News feed %s failed: %s", source, e)

                grouped: Dict[str, List[NewsArticle]] = {}
                new_hashes: Set[str] = set()
                for article in all_articles:
                    h = hashlib.md5(article.title.encode()).hexdigest()[:12]
                    if h in self._seen_hashes:
                        continue
                    self._seen_hashes.add(h)
                    new_hashes.add(h)
                    symbols = self._map_article(article)
                    if symbols:
                        article.sentiment_score = self._score_sentiment(article.title)
                        for sym in symbols:
                            grouped.setdefault(sym, []).append(article)

                if grouped:
                    self._articles = grouped
                self._last_fetch = now
                logger.info("News: %d new articles across %d products",
                            len(new_hashes), len(grouped))
            except Exception as e:
                logger.debug("News refresh error: %s", e)

    def get_signals(self, min_articles: int = 1) -> List[SentimentSignal]:
        self.refresh()
        signals: List[SentimentSignal] = []
        for pid, articles in self._articles.items():
            if len(articles) < min_articles:
                continue
            scores = [a.sentiment_score for a in articles]
            avg_sentiment = sum(scores) / len(scores)

            source_counts: Dict[str, int] = {}
            for a in articles:
                source_counts[a.source] = source_counts.get(a.source, 0) + 1
            unique_sources = len(source_counts)

            # Confidence: more articles + multiple sources = higher confidence
            article_conf = min(1.0, len(articles) / 5.0)
            source_conf = min(1.0, unique_sources / 3.0)
            strength = abs(avg_sentiment)
            confidence = (article_conf * 0.4 + source_conf * 0.3 + strength * 0.3)
            confidence = min(1.0, max(0.0, confidence))

            if avg_sentiment >= 0.15 and confidence >= 0.3:
                signals.append(SentimentSignal(
                    product_id=pid,
                    action="BUY",
                    confidence=confidence,
                    sentiment_score=avg_sentiment,
                    article_count=len(articles),
                    top_headline=articles[0].title,
                    source_breakdown=source_counts,
                ))
            elif avg_sentiment <= -0.15 and confidence >= 0.3:
                signals.append(SentimentSignal(
                    product_id=pid,
                    action="SELL",
                    confidence=confidence,
                    sentiment_score=avg_sentiment,
                    article_count=len(articles),
                    top_headline=articles[0].title,
                    source_breakdown=source_counts,
                ))
        signals.sort(key=lambda s: abs(s.sentiment_score) * s.confidence, reverse=True)
        return signals

    def get_summary(self) -> Dict:
        self.refresh()
        result: Dict[str, Dict] = {}
        for pid, articles in self._articles.items():
            scores = [a.sentiment_score for a in articles]
            avg = sum(scores) / len(scores) if scores else 0.0
            result[pid] = {
                "article_count": len(articles),
                "avg_sentiment": round(avg, 3),
                "top_headline": articles[0].title,
                "sources": list({a.source for a in articles}),
            }
        return result

    def _fetch_feed(self, source: str, url: str) -> List[NewsArticle]:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        def _fetch_raw() -> bytes:
            with urlopen(req, timeout=10) as resp:
                return resp.read()

        raw = retry_call(_fetch_raw, attempts=3, base_delay=0.5, max_delay=4.0)
        root = ElementTree.fromstring(raw)

        namespaces = {
            "atom": "http://www.w3.org/2005/Atom",
            "dc": "http://purl.org/dc/elements/1.1/",
            "content": "http://purl.org/rss/1.0/modules/content/",
        }

        articles: List[NewsArticle] = []
        for item in root.iter("item"):
            title_el = item.find("title")
            link_el = item.find("link")
            pub_el = item.find("pubDate")
            if title_el is None or title_el.text is None:
                continue
            title = title_el.text.strip()
            url = link_el.text.strip() if link_el is not None and link_el.text else ""
            published = pub_el.text.strip() if pub_el is not None and pub_el.text else ""
            articles.append(NewsArticle(title=title, source=source, url=url, published=published))

        for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
            title_el = entry.find("{http://www.w3.org/2005/Atom}title")
            link_el = entry.find("{http://www.w3.org/2005/Atom}link")
            pub_el = entry.find("{http://www.w3.org/2005/Atom}published")
            if title_el is None or title_el.text is None:
                continue
            title = title_el.text.strip()
            url = link_el.get("href", "") if link_el is not None else ""
            published = pub_el.text.strip() if pub_el is not None and pub_el.text else ""
            articles.append(NewsArticle(title=title, source=source, url=url, published=published))

        return articles

    def _map_article(self, article: NewsArticle) -> List[str]:
        text = (article.title + " " + article.url).upper()
        found: List[str] = []
        for name, pid in KNOWN_CRYPTO_SYMBOLS.items():
            if name in text:
                if pid not in found:
                    found.append(pid)
        return found[:3]

    def _score_sentiment(self, text: str) -> float:
        words = set(text.lower().split())
        pos_count = len(words & POSITIVE_WORDS)
        neg_count = len(words & NEGATIVE_WORDS)
        total = pos_count + neg_count
        if total == 0:
            return 0.0
        return (pos_count - neg_count) / max(1, total)
