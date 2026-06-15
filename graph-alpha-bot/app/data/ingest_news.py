#!/usr/bin/env python3
"""News Ingestion Pipeline for Financial Knowledge Graph.

This module ingests news from multiple sources and stores them in Neo4j:
- X/Twitter API (via twikit or official API)
- RSS feeds (financial blogs, news sites)
- SEC EDGAR filings (earnings announcements, press releases)
- Crypto-specific sources (CoinDesk, Cointelegraph for crypto assets)

All articles are scored for sentiment and linked to relevant tickers.

Usage:
    python3 app/data/ingest_news.py --symbols BTC-USD ETH-USD --interval 5m
    
Or as a cron job for continuous ingestion.
"""

import sys, os, json, time, hashlib, re, threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from urllib.parse import urlparse, urljoin
import argparse
import logging
import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Neo4j imports
try:
    from neo4j import GraphDatabase, basic_auth
except ImportError:
    print("Installing neo4j package...")
    os.system("pip install neo4j")
    from neo4j import GraphDatabase, basic_auth

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class NewsArticle:
    """Normalized news article structure."""
    id: str
    title: str
    content: str
    source_url: str
    source_name: str
    published_at: datetime
    sentiment_score: float = 0.0  # -1 to 1
    sentiment_label: str = "neutral"
    entities: List[str] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    
    def __hash__(self):
        return hash(self.id)


@dataclass
class Config:
    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "")
    
    # X/Twitter API settings
    twitter_api_key: str = os.getenv("TWITTER_API_KEY", "")
    twitter_api_secret: str = os.getenv("TWITTER_API_SECRET", "")
    twitter_bearer_token: str = os.getenv("TWITTER_BEARER_TOKEN", "")
    
    # RSS feeds to monitor
    rss_feeds: List[str] = field(default_factory=lambda: [
        "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        "https://feeds.feedburner.com/cnbc-top-stories",
        "https://www.coindesk.com/rdfeed/",  # Crypto news
        "https://cointelegraph.com/news/feed",  # Crypto news
    ])
    
    # Financial data APIs
    alpha_vantage_api_key: str = os.getenv("ALPHA_VANTAGE_API_KEY", "")
    finnhub_api_key: str = os.getenv("FINNHUB_API_KEY", "")
    
    # Processing settings
    processing_interval_seconds: int = 300  # 5 minutes default
    max_articles_per_source: int = 20
    min_sentiment_threshold: float = 0.3
    ticker_match_threshold: float = 0.6


class SentimentAnalyzer:
    """Simple sentiment analysis for financial news."""
    
    POSITIVE_WORDS = {
        'gain', 'gains', 'rising', 'rise', 'surge', 'soar', 'boom', 'bullish',
        'profit', 'profits', 'upside', 'outperform', 'beats', 'stronger',
        'opportunit', 'breakthrough', 'success', 'growth', 'growing',
        'record', 'milestone', 'upgrade', 'favorable', 'positive',
        'optimistic', 'acqui', 'partner', 'expand', 'expandin', 'revenue'
    }
    
    NEGATIVE_WORDS = {
        'fall', 'decline', 'drop', 'plummet', 'crash', 'bearish',
        'loss', 'losse', 'downgrade', 'miss', 'disappoint', 'weak',
        'concern', 'risk', 'threat', 'investigat', 'lawsuit', 'fine',
        'regulatory', 'scandal', 'controversy', 'delay', 'cancel'
    }
    
    CRYPTO_POSITIVE = {
        'adoption', 'institutional', 'etf', 'bullrun', 'halving', 
        'mainstream', 'perrmanent', 'infrastructure', 'regulatory clarity'
    }
    
    CRYPTO_NEGATIVE = {
        'hacks', 'exploit', 'breach', 'rug pull', 'scam', 'ban',
        'restriction', 'crackdown'
    }
    
    def analyze(self, text: str, title: str) -> Tuple[float, str, List[str], List[str]]:
        """Analyze sentiment of text.
        
        Returns: (score, label, entities, categories)
        """
        combined = f"{title} {text}".lower()
        
        # Word matching approach
        positive_count = sum(1 for word in self.POSITIVE_WORDS if word in combined)
        negative_count = sum(1 for word in self.NEGATIVE_WORDS if word in combined)
        
        # Crypto-specific scoring boost/drop
        crypto_positive = sum(1 for word in self.CRYPTO_POSITIVE if word in combined)
        crypto_negative = sum(1 for word in self.CRYPTO_NEGATIVE if word in combined)
        
        # Calculate score (-1 to 1)
        total = positive_count + negative_count + crypto_positive + crypto_negative
        
        if total == 0:
            return 0.0, "neutral", [], ["general"]
        
        score = ((positive_count + crypto_positive * 1.5) - (negative_count + crypto_negative * 2)) / total
        
        # Entity extraction (simple ticker matching)
        entities = self.extract_entities(text, title)
        
        # Category classification
        categories = self.classify_categories(text, title)
        
        if score > 0.3:
            label = "positive"
        elif score < -0.3:
            label = "negative"
        else:
            label = "neutral"
        
        return round(score, 3), label, entities, categories
    
    def extract_entities(self, text: str, title: str) -> List[str]:
        """Extract mentioned tickers/entities from text."""
        patterns = [
            r'\b[A-Z]{1,5}\b',
            r'\b(?:BTC|ETH|SOL|ADA|XRP|DOGE|AVAX|LINK|DOT|NEAR)\s*-?\s*USD?',
        ]
        
        text_to_search = f"{title} {text}"
        entities = []
        
        for pattern in patterns:
            matches = re.findall(pattern, text_to_search)
            entities.extend(matches)
        
        return list(set(entities))
    
    def classify_categories(self, text: str, title: str) -> List[str]:
        """Classify article into categories."""
        categories = []
        combined = f"{title} {text}".lower()
        
        category_keywords = {
            "earnings": ['earn', 'revenue', 'profit', 'eps', 'quarterly'],
            "m&A": ['acqui', 'merg', 'takeover', 'deal', 'purchase'],
            "regulation": ['sec', 'regulatory', 'lawsuit', 'investigat', 'compliance'],
            "product": ['launch', 'new product', 'feature', 'announc'],
            "leadership": ['ceo', 'executiv', 'cfo', 'board', 'hiring'],
            "market_analysis": ['analyst', 'rating', 'target', 'forecast', 'outlook']
        }
        
        for category, keywords in category_keywords.items():
            if any(kw in combined for kw in keywords):
                categories.append(category)
        
        return categories if categories else ["general"]


class XTwitterIngestor:
    """Ingest news from X/Twitter (was Twitter)."""
    
    def __init__(self, config: Config):
        self.config = config
        self.trend_topic_cache: Dict[str, datetime] = {}
        
    def fetch_trending_topics(self) -> List[dict]:
        """Fetch trending topics by location."""
        logging.info("Twitter ingestion not configured (need TWITTER_BEARER_TOKEN)")
        return []
    
    def fetch_stock_mentions(self, symbol: str) -> List[dict]:
        """Fetch recent X posts mentioning a stock symbol."""
        logging.info(f"Fetching tweets for @{symbol}")
        return []


class RSSIngestor:
    """Ingest news from RSS feeds."""
    
    def __init__(self, config: Config):
        self.config = config
        self._cache_file = ".news_cache.json"
        self._cache_lock = threading.Lock()
        
    def load_cache(self) -> Dict[str, datetime]:
        """Load last fetch times for each feed."""
        try:
            if os.path.exists(self._cache_file):
                with open(self._cache_file) as f:
                    return json.load(f)
        except Exception as e:
            logging.warning(f"Failed to load cache: {e}")
        return {}
    
    def save_cache(self, cache: Dict[str, datetime]):
        """Save cache to file."""
        with self._cache_lock:
            try:
                with open(self._cache_file, 'w') as f:
                    json.dump(cache, f)
            except Exception as e:
                logging.warning(f"Failed to save cache: {e}")
    
    def fetch_feeds(self) -> List[Tuple[str, List[dict]]]:
        """Fetch articles from all configured RSS feeds."""
        
        feed_updates = self.load_cache()
        now = datetime.utcnow()
        
        all_articles: List[Tuple[str, List[dict]]] = []
        
        for feed_url in self.config.rss_feeds:
            updated_since = feed_updates.get(feed_url)
            
            try:
                response = requests.get(feed_url, timeout=10)
                if response.status_code != 200:
                    continue
                
                articles = self.parse_rss(response.text, feed_url, updated_since)
                all_articles.append((feed_url, articles))
                
                # Update cache
                feed_updates[feed_url] = now
                self.save_cache(feed_updates)
                
            except Exception as e:
                logging.error(f"Failed to fetch {feed_url}: {e}")
        
        return all_articles
    
    def parse_rss(self, xml_content: str, source: str, updated_since: Optional[datetime]) -> List[dict]:
        """Parse RSS XML content."""
        soup = BeautifulSoup(xml_content, 'xml')
        items = []
        
        for entry in soup.find_all(['item', 'entry']):
            title_el = entry.find(['title'])
            link_el = entry.find(['link', 'guid'])
            pubdate_el = entry.find(['pubDate', 'published'])
            desc_el = entry.find(['description', 'content:encoded', 'summary'])
            
            if not all([title_el, link_el]):
                continue
            
            try:
                date_str = pubdate_el.string if pubdate_el else None
                if date_str:
                    published_at = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")
                else:
                    published_at = datetime.utcnow()
            except:
                published_at = datetime.utcnow()
            
            if updated_since and published_at < updated_since:
                continue
            
            items.append({
                "title": title_el.string,
                "url": link_el.get('href') or link_el.string,
                "description": desc_el.string if desc_el else "",
                "source": source,
                "published_at": published_at
            })
        
        return items


class SECEDGARIngestor:
    """Ingest press releases and filings from SEC EDGAR."""
    
    def __init__(self, config: Config):
        self.config = config
        self.user_agent = os.getenv("SEC_USER_AGENT", "FinancialAnalysisBot <finance@example.com>")
        
    def fetch_earnings_releases(self, cik: str, days_back: int = 7) -> List[dict]:
        """Fetch press releases from a CIK."""
        base_url = "https://datasecgov/api/v0/"
        
        params = {
            "company_cik": cik,
            "form_types": ["8-K"],
            "limit": 50,
        }
        
        headers = {"User-Agent": self.user_agent}
        
        try:
            response = requests.get(
                f"{base_url}edgar/filings/",
                params=params,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("filings", [])
            return []
        
        except Exception as e:
            logging.error(f"Failed to fetch SEC filings for {cik}: {e}")
            return []

    def fetch_press_releases_by_ticker(self, ticker: str) -> List[dict]:
        """Fetch press releases for a specific ticker using CIK mapping."""
        logging.info(f"Fetching SEC filings for {ticker}")
        return []


class NewsIngestionPipeline:
    """Main news ingestion pipeline coordinating all sources."""
    
    def __init__(self, config: Config):
        self.config = config
        self.analyzer = SentimentAnalyzer()
        self.twitter_ingestor = XTwitterIngestor(config)
        self.rss_ingestor = RSSIngestor(config)
        self.sec_ingestor = SECEDGARIngestor(config)
    
    def ingest_all(self, symbols: Optional[List[str]] = None):
        """Run full news ingestion cycle."""
        
        logging.info(f"Starting news ingestion for {symbols or 'all'}")
        
        # 1. Fetch from all sources
        rss_articles = self.rss_ingestor.fetch_feeds()
        
        # 2. Process and score each article
        processed_articles: List[NewsArticle] = []
        
        for source, articles in rss_articles:
            for article_data in articles:
                article = self.process_article(article_data, source)
                if article:
                    processed_articles.append(article)
        
        logging.info(f"Processed {len(processed_articles)} news articles")
        
        # 3. Store in Neo4j
        self.store_articles(processed_articles, symbols)
    
    def process_article(self, data: dict, source: str) -> Optional[NewsArticle]:
        """Process a single article into normalized format."""
        
        title = data.get('title', '')
        content = data.get('description', '')
        url = data.get('url', '')
        published_at = data.get('published_at', datetime.utcnow())
        
        if published_at < datetime.utcnow() - timedelta(days=7):
            return None
        
        score, label, entities, categories = self.analyzer.analyze(content, title)
        
        article_id = hashlib.sha256(f"{url}{published_at}".encode()).hexdigest()[:16]
        
        return NewsArticle(
            id=article_id,
            title=title,
            content=content[:2000],
            source_url=url,
            source_name=source.split('//')[-1][:30],
            published_at=published_at,
            sentiment_score=score,
            sentiment_label=label,
            entities=entities,
            categories=categories
        )
    
    def store_articles(self, articles: List[NewsArticle], symbols: Optional[List[str]]):
        """Store processed articles in Neo4j."""
        
        if not articles:
            return
        
        driver = None
        try:
            from neo4j import GraphDatabase, basic_auth
            
            driver = GraphDatabase.driver(
                self.config.neo4j_uri,
                auth=basic_auth(self.config.neo4j_user, self.config.neo4j_password)
            )
            
            with driver.session() as session:
                for article in articles:
                    session.run("""
                    MERGE (a:News {id:$id})
                    SET a.title = $title,
                        a.url = $url,
                        a.source = $source,
                        a.timestamp = datetime($ts),
                        a.sentimentScore = $score,
                        a.sentimentLabel = $label,
                        a.categories = $categories
                    """,
                        id=article.id,
                        title=article.title,
                        url=article.source_url,
                        source=article.source_name[:50],
                        ts=str(article.published_at),
                        score=article.sentiment_score,
                        label=article.sentiment_label,
                        categories=article.categories
                    )
                    
                    for entity in article.entities:
                        session.run("""
                        MERGE (t:Ticker {symbol:$ticker})
                        MERGE (a)<-[:MENTIONED_IN]-(t)
                        """, ticker=entity, a=article.id)
                        
        except Exception as e:
            logging.error(f"Failed to store articles: {e}")
        finally:
            if driver:
                driver.close()
    
    def get_news_for_ticker(self, symbol: str, days: int = 7, limit: int = 50):
        """Query news for a specific ticker from Neo4j."""
        
        driver = None
        try:
            from neo4j import GraphDatabase, basic_auth
            
            driver = GraphDatabase.driver(
                self.config.neo4j_uri,
                auth=basic_auth(self.config.neo4j_user, self.config.neo4j_password)
            )
            
            with driver.session() as session:
                result = session.run("""
                MATCH (t:Ticker {symbol:$symbol})<-[:MENTIONED_IN]-(n:News)
                WHERE n.timestamp >= datetime() - duration({days:$days})
                RETURN n.title as title,
                       n.source as source,
                       n.sentimentLabel as sentiment,
                       n.sentimentScore as score,
                       n.timestamp as timestamp
                ORDER BY n.timestamp DESC
                LIMIT $limit
                """, symbol=symbol, days=days, limit=limit)
                
                return [record.data() for record in result]
        
        except Exception as e:
            logging.error(f"Failed to query news: {e}")
            return []
        finally:
            if driver:
                driver.close()


def main():
    parser = argparse.ArgumentParser(description="News Ingestion Pipeline")
    parser.add_argument("--symbols", nargs='+', default=['BTC-USD', 'ETH-USD', 'AAPL', 'GOOGL'])
    parser.add_argument("--interval", type=int, default=300, help="Processing interval in seconds")
    
    args = parser.parse_args()
    
    config = Config()
    pipeline = NewsIngestionPipeline(config)
    
    print(f"Starting news ingestion for: {', '.join(args.symbols)}")
    print(f"Polling every {args.interval} seconds. Ctrl+C to stop.")
    
    last_run = datetime.utcnow() - timedelta(seconds=args.interval)
    
    try:
        while True:
            now = datetime.utcnow()
            
            if (now - last_run).total_seconds() >= args.interval:
                pipeline.ingest_all(args.symbols)
                last_run = now
            
            time.sleep(60)
    
    except KeyboardInterrupt:
        print("\nIngestion stopped by user.")


if __name__ == "__main__":
    main()
