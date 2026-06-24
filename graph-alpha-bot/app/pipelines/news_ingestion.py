#!/usr/bin/env python3
"""News ingestion pipeline - fetches RSS feeds, classifies topics, tracks breaking news."""

import sys, os, json, time, logging, hashlib, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from collections import defaultdict


def setup_logging(log_file: str = None):
    if log_file is None:
        home_dir = os.path.expanduser("~")
        log_file = f"{home_dir}/.hermes/log/graphalphabot/news.log"
    Path(os.path.dirname(log_file)).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
    )


RSS_FEEDS = {
    "coindesk": {
        "url": "https://www.coindesk.com/arc/rss/articles?topic=Cryptocurrency",
        "source_name": "CoinDesk",
        "weight": 1.0,
    },
    "cryptoslate": {
        "url": "https://cryptoslate.com/feed/",
        "source_name": "CryptoSlate",
        "weight": 0.8,
    },
    "cointelegraph": {
        "url": "https://cointelegraph.com/news/feed",
        "source_name": "Cointelegraph",
        "weight": 0.9,
    },
    "theblock": {
        "url": "https://www.theblock.co/rss.xml",
        "source_name": "The Block",
        "weight": 1.0,
    },
    "decrypt": {
        "url": "https://decrypt.co/feed",
        "source_name": "Decrypt",
        "weight": 0.8,
    },
    "bitcoinmagazine": {
        "url": "https://bitcoinmagazine.com/feed",
        "source_name": "Bitcoin Magazine",
        "weight": 0.7,
    },
    "bloombergcrypto": {
        "url": "https://feeds.bloomberg.com/markets/news.rss",
        "source_name": "Bloomberg",
        "weight": 1.2,
    },
}

KNOWN_CRYPTO_SYMBOLS = {
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

TOPIC_KEYWORDS: Dict[str, List[str]] = {
    "regulation": [
        "sec", "regulation", "regulatory", "compliance", "lawsuit", "legal", "cftc",
        "ban", "sanction", "license", "approval", "etf", "bill", "law", "congress",
        "treasury", "federal reserve", "cbdc", "money laundering", "kyc", "aml",
    ],
    "adoption": [
        "adoption", "institutional", "mainstream", "main street", "payment",
        "merchant", "integration", "partnership", "enterprise", "corporate",
        "fortune 500", "bank", "traditional finance", "tradfi",
    ],
    "hacks_security": [
        "hack", "exploit", "breach", "attack", "vulnerability", "bug", "security",
        "phishing", "scam", "rug pull", "stolen", "theft", "malware", "ransomware",
        "bridge hack", "flash loan attack",
    ],
    "technology": [
        "upgrade", "fork", "scaling", "layer 2", "l2", "rollup", "zk", "zero knowledge",
        "sharding", "consensus", "proof of stake", "pos", "proof of work", "pow",
        "smart contract", "defi", "dao", "nft", "protocol", "mainnet", "testnet",
    ],
    "partnerships": [
        "partner", "partnership", "collaborate", "collaboration", "alliance",
        "integration", "joins forces", "strategic", "memorandum of understanding",
        "mou", "joint venture",
    ],
    "price_analysis": [
        "price", "rally", "surge", "dump", "pump", "correction", "bull run",
        "bear market", "all-time high", "ath", "support", "resistance", "breakout",
        "breakdown", "volatility", "market cap", "trading volume", "liquidation",
    ],
    "mining": [
        "mining", "miner", "hashrate", "hash rate", "block reward", "halving",
        "difficulty", "asic", "pool", "proof of work", "energy", "power consumption",
    ],
    "macro": [
        "inflation", "fed", "interest rate", "cpi", "gdp", "recession", "economy",
        "economic", "dollar", "treasury yield", "monetary policy", "quantitative easing",
        "qe", "quantitative tightening", "qt",
    ],
}

REGULATORY_WORDS = {"regulation", "regulatory", "compliance", "lawsuit", "legal", "ban", "sanction", "license", "sec", "cftc"}
HACK_WORDS = {"hack", "exploit", "breach", "attack", "scam", "stolen", "theft"}


class NewsIngestionPipeline:
    """Fetches and processes crypto news from RSS feeds with topic classification."""

    def __init__(self, symbols: List[str] = None):
        self.symbols = symbols or ['bitcoin', 'btc', 'ethereum', 'eth', 'solana', 'sol']
        self.kg_file = Path("graph-alpha-bot/app/data/knowledge_graph.json")
        self.processed_hashes: set = set()
        self.cache_dir = Path(".news_cache")
        self.cache_dir.mkdir(exist_ok=True)
        # Volume tracking for breaking news detection
        self._volume_window: Dict[str, List[float]] = defaultdict(list)
        self._volume_window_hours = 4
        logging.info(f"News ingestion initialized with {len(RSS_FEEDS)} feeds")

    def classify_topic(self, title: str, summary: str = "") -> Optional[str]:
        text = (title + " " + summary).lower()
        scores: Dict[str, int] = {}
        for topic, keywords in TOPIC_KEYWORDS.items():
            count = sum(1 for kw in keywords if kw in text)
            if count > 0:
                scores[topic] = count
        if not scores:
            return None
        return max(scores, key=scores.get)

    def extract_tickers(self, title: str, summary: str = "") -> List[str]:
        text = (title + " " + summary).upper()
        found: List[str] = []
        for name, product in KNOWN_CRYPTO_SYMBOLS.items():
            if name in text and product not in found:
                found.append(product)
        return found

    def compute_sentiment(self, title: str, summary: str = "") -> float:
        text = (title + " " + summary).lower()
        positive = sum(1 for w in ["gain", "rise", "surge", "bullish", "profit", "breakthrough",
                                    "success", "upgrade", "adoption", "launch", "positive", "growth",
                                    "optimistic", "momentum", "recovery", "outperform", "innovation",
                                    "record", "soar", "jump", "rally"] if w in text)
        negative = sum(1 for w in ["fall", "drop", "crash", "bearish", "loss", "downgrade",
                                    "scandal", "lawsuit", "hack", "exploit", "ban", "restrict",
                                    "decline", "slump", "plunge", "fear", "panic", "sell-off",
                                    "liquidation", "bankruptcy", "fraud", "investigation"] if w in text)
        total = positive + negative
        if total == 0:
            return 0.5
        return round((positive - negative) / total, 3)

    def compute_freshness_weight(self, pub_date_str: str, half_life_hours: float = 6) -> float:
        try:
            pub = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00").replace("GMT", "").strip())
        except (ValueError, TypeError):
            return 0.5
        age_hours = (datetime.now(timezone.utc) - pub).total_seconds() / 3600
        if age_hours < 0:
            return 1.0
        return 2 ** (-age_hours / half_life_hours)

    def track_volume(self, topic: Optional[str]) -> None:
        now = time.time()
        cutoff = now - self._volume_window_hours * 3600
        if topic:
            self._volume_window[topic].append(now)
            self._volume_window[topic] = [t for t in self._volume_window[topic] if t > cutoff]
        # Prune old topics
        for t in list(self._volume_window.keys()):
            self._volume_window[t] = [ts for ts in self._volume_window[t] if ts > cutoff]
            if not self._volume_window[t]:
                del self._volume_window[t]

    def detect_breaking_news(self, topic: Optional[str], threshold: float = 3.0) -> bool:
        if not topic:
            return False
        recent = self._volume_window.get(topic, [])
        now = time.time()
        window = now - timedelta(hours=1).total_seconds()
        last_hour = sum(1 for t in recent if t > window)
        prior_cutoff = window - timedelta(hours=2).total_seconds()
        prior_hour = sum(1 for t in recent if prior_cutoff < t <= window)
        if prior_hour == 0:
            return last_hour >= threshold
        return (last_hour / prior_hour) >= 2.0 and last_hour >= threshold

    def fetch_rss_feed(self, url: str, source_name: str, max_articles: int = 20) -> List[Dict[str, Any]]:
        cache_file = self.cache_dir / (hashlib.md5(url.encode()).hexdigest()[:16] + ".json")
        if cache_file.exists():
            try:
                with open(cache_file) as f:
                    data = json.load(f)
                last_fetch = datetime.fromisoformat(data.get("last_fetch", "1970-01-01").replace('Z', '+00:00'))
                if datetime.now(timezone.utc) - last_fetch < timedelta(minutes=15):
                    return data.get("articles", [])
            except Exception:
                pass
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (GraphAlphaBot/2.0; +https://github.com)"}
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                xml_data = response.read().decode('utf-8')
            articles = self._parse_rss(xml_data, source_name, max_articles)
            cache_entry = {
                "last_fetch": datetime.now(timezone.utc).isoformat(),
                "articles": articles
            }
            with open(cache_file, 'w') as f:
                json.dump(cache_entry, f)
            return articles
        except Exception as e:
            logging.error(f"RSS fetch failed {source_name}: {e}")
            return []

    def _parse_rss(self, xml_data: str, source: str, max_articles: int) -> List[Dict[str, Any]]:
        articles = []
        try:
            root = ET.fromstring(xml_data)
            items = root.findall(".//item")
            for item in items[:max_articles]:
                title_elem = item.find("title")
                link_elem = item.find("link")
                pubdate_elem = item.find("pubDate")
                if not all([title_elem is not None, link_elem is not None]):
                    continue
                desc_elem = item.find("description")
                summary = (desc_elem.text or "")[:500] if desc_elem is not None else ""
                article = {
                    "id": hashlib.md5(link_elem.text.encode()).hexdigest()[:12],
                    "title": title_elem.text or "",
                    "url": link_elem.text or "",
                    "published_at": pubdate_elem.text if pubdate_elem is not None else datetime.now(timezone.utc).isoformat(),
                    "source": source,
                    "summary": summary,
                    "tickers": [],
                    "sentiment_score": 0.5,
                    "topic": None,
                    "freshness": 1.0,
                    "is_breaking": False,
                }
                articles.append(article)
            return articles
        except Exception as e:
            logging.error(f"RSS parse failed for {source}: {e}")
            return []

    def run_once(self) -> Dict[str, Any]:
        all_articles = []
        fetch_times = {}

        for feed_name, feed_config in RSS_FEEDS.items():
            start = time.time()
            articles = self.fetch_rss_feed(feed_config["url"], feed_config["source_name"])
            fetch_times[feed_name] = round(time.time() - start, 3)

            for article in articles:
                if article["id"] in self.processed_hashes:
                    continue
                self.processed_hashes.add(article["id"])
                sentiment = self.compute_sentiment(article["title"], article["summary"])
                topic = self.classify_topic(article["title"], article["summary"])
                tickers = self.extract_tickers(article["title"], article["summary"])
                freshness = self.compute_freshness_weight(article["published_at"])
                self.track_volume(topic)
                is_breaking = self.detect_breaking_news(topic)

                all_articles.append({
                    **article,
                    "sentiment_score": sentiment,
                    "tickers": tickers,
                    "topic": topic,
                    "freshness": round(freshness, 3),
                    "is_breaking": is_breaking,
                })

            logging.info(f"{feed_name}: {len(articles)} articles ({fetch_times[feed_name]}s)")

        source_weights = {name: cfg["weight"] for name, cfg in RSS_FEEDS.items()}
        kg = {
            "articles": all_articles,
            "tickers": self._extract_ticker_stats(all_articles),
            "topic_volume": dict(self._volume_window),
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
                "total_articles": len(all_articles),
                "sources": list(RSS_FEEDS.keys()),
                "source_weights": source_weights,
                "fetch_times": fetch_times,
                "topics_active": [t for t, v in self._volume_window.items() if len(v) > 0],
                "breaking_topics": [t for t in self._volume_window if self.detect_breaking_news(t)],
            }
        }

        Path("graph-alpha-bot/app/data").mkdir(parents=True, exist_ok=True)
        with open(self.kg_file, 'w') as f:
            json.dump(kg, f, indent=2)

        return {
            "status": "success",
            "articles_collected": len(all_articles),
            "sources_used": list(RSS_FEEDS.keys()),
            "fetch_times": fetch_times,
            "topics_found": list(set(a.get("topic") for a in all_articles if a.get("topic"))),
            "breaking_topics": kg["metadata"]["breaking_topics"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _extract_ticker_stats(self, articles: List[Dict]) -> Dict[str, Any]:
        tickers: Dict[str, dict] = {}
        for article in articles:
            topic = article.get("topic")
            sentiment = article.get("sentiment_score", 0.5)
            is_breaking = article.get("is_breaking", False)
            for ticker in article.get("tickers", []):
                if ticker not in tickers:
                    tickers[ticker] = {"count": 0, "topics": set(), "sentiments": [], "breaking_count": 0}
                tickers[ticker]["count"] += 1
                if topic:
                    tickers[ticker]["topics"].add(topic)
                tickers[ticker]["sentiments"].append(sentiment)
                if is_breaking:
                    tickers[ticker]["breaking_count"] += 1
        result = {}
        for sym, info in tickers.items():
            avg_s = sum(info["sentiments"]) / len(info["sentiments"]) if info["sentiments"] else 0.5
            result[sym] = {
                "count": info["count"],
                "topics": list(info["topics"]),
                "avg_sentiment": round(avg_s, 3),
                "breaking_count": info["breaking_count"],
            }
        return result


def main():
    setup_logging()
    pipeline = NewsIngestionPipeline(symbols=['bitcoin', 'btc', 'ethereum', 'eth', 'solana', 'sol'])
    result = pipeline.run_once()
    print("\n" + "=" * 60)
    print("NEWS INGESTION RESULTS")
    print("=" * 60)
    print(f"Articles collected: {result['articles_collected']}")
    print(f"Sources: {result['sources_used']}")
    print(f"Topics found: {result['topics_found']}")
    print(f"Breaking topics: {result['breaking_topics']}")
    print(f"Fetch times: {json.dumps(result['fetch_times'], indent=2)}")


if __name__ == "__main__":
    main()
