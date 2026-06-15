#!/usr/bin/env python3
"""News ingestion pipeline - fetches RSS feeds from CoinDesk, Cointelegraph."""

import sys, os, json, time, logging, hashlib, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path


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


# RSS feed URLs - public feeds that don't require authentication
RSS_FEEDS = {
    "coindesk": {
        "url": "https://www.coindesk.com/arc/rss/articles?topic=Cryptocurrency",
        "source_name": "CoinDesk"
    },
    "cryptoslate": {
        "url": "https://cryptoslate.com/feed/",
        "source_name": "CryptoSlate"
    }
}


class NewsIngestionPipeline:
    """Fetches and processes crypto news from RSS feeds."""
    
    def __init__(self, symbols: List[str] = None):
        self.symbols = symbols or ['bitcoin', 'btc', 'ethereum', 'eth', 'solana', 'sol']
        self.kg_file = Path("graph-alpha-bot/app/data/knowledge_graph.json")
        
        # Cache for deduplication
        self.processed_hashes: set = set()
        self.cache_dir = Path(".news_cache")
        self.cache_dir.mkdir(exist_ok=True)
        
        logging.info(f"News ingestion initialized with {len(RSS_FEEDS)} feeds")
    
    def fetch_rss_feed(self, url: str, source_name: str, max_articles: int = 20) -> List[Dict[str, Any]]:
        """Fetch RSS feed and parse articles."""
        import urllib.request
        import xml.etree.ElementTree as ET
        
        cache_file = self.cache_dir / (hashlib.md5(url.encode()).hexdigest()[:16] + ".json")
        
        # Try cached version first (refresh every 30 minutes)
        if cache_file.exists():
            try:
                with open(cache_file) as f:
                    data = json.load(f)
                
                last_fetch = datetime.fromisoformat(data.get("last_fetch", "1970-01-01").replace('Z', '+00:00'))
                if datetime.now(timezone.utc) - last_fetch < timedelta(minutes=30):
                    logging.debug(f"Using cached RSS feed from {source_name}")
                    return data.get("articles", [])
            except Exception as e:
                pass  # Fall through to fetch
        
        try:
            req = urllib.request.Request(
                url, 
                headers={"User-Agent": "Mozilla/5.0 (GraphAlphaBot/1.0)"}
            )
            
            with urllib.request.urlopen(req, timeout=10) as response:
                xml_data = response.read().decode('utf-8')
            
            articles = self._parse_rss(xml_data, source_name, max_articles)
            
            # Cache result
            cache_entry = {
                "last_fetch": datetime.now(timezone.utc).isoformat(),
                "articles": articles
            }
            with open(cache_file, 'w') as f:
                json.dump(cache_entry, f)
            
            return articles
            
        except Exception as e:
            logging.error(f"Failed to fetch RSS feed {source_name}: {e}")
            return []
    
    def _parse_rss(self, xml_data: str, source: str, max_articles: int) -> List[Dict[str, Any]]:
        """Parse RSS XML into article objects."""
        import xml.etree.ElementTree as ET
        
        articles = []
        
        try:
            root = ET.fromstring(xml_data)
            
            # Find all item elements (standard RSS format)
            items = root.findall(".//item")
            
            for item in items[:max_articles]:
                title_elem = item.find("title")
                link_elem = item.find("link")
                pubdate_elem = item.find("pubDate")
                
                if not all([title_elem is not None, link_elem is not None]):
                    continue
                
                # Extract content (try enclosure or description)
                enclosure = item.find("enclosure")
                summary = ""
                if enclosure is not None:
                    summary = enclosure.get("url", "")[:200]
                
                article = {
                    "id": hashlib.md5(link_elem.text.encode()).hexdigest()[:12],
                    "title": title_elem.text or "",
                    "url": link_elem.text or "",
                    "published_at": pubdate_elem.text if pubdate_elem is not None else datetime.now(timezone.utc).isoformat(),
                    "source": source,
                    "summary": summary,
                    "tickers": [],  # Will be filled by signal generator
                    "sentiment_score": 0.0  # Placeholder - will be scored by signal gen
                }
                
                articles.append(article)
            
            return articles
            
        except Exception as e:
            logging.error(f"Failed to parse RSS XML: {e}")
            return []
    
    def run_once(self) -> Dict[str, Any]:
        """Run one cycle of news ingestion."""
        
        all_articles = []
        fetch_times = {}
        
        for feed_name, feed_config in RSS_FEEDS.items():
            start_time = time.time()
            
            articles = self.fetch_rss_feed(feed_config["url"], feed_config["source_name"])
            fetch_times[feed_name] = round(time.time() - start_time, 3)
            
            # Update cache
            for article in articles:
                if article["id"] not in self.processed_hashes:
                    self.processed_hashes.add(article["id"])
                    all_articles.append({
                        **article,
                        "sentiment_score": 0.5,  # Default positive until scored
                        "tickers": [t.replace(" ", "-") for t in article.get("title", "").split() if len(t) > 2 and not t[0].isdigit()]
                    })
            
            logging.info(f"📰 {feed_name}: fetched {len(articles)} articles ({fetch_times[feed_name]}s)")
        
        # Build knowledge graph
        kg = {
            "articles": all_articles,
            "tickers": self._extract_ticker_stats(all_articles),
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
                "total_articles": len(all_articles),
                "sources": list(RSS_FEEDS.keys()),
                "fetch_times": fetch_times
            }
        }
        
        # Save knowledge graph
        Path("graph-alpha-bot/app/data").mkdir(parents=True, exist_ok=True)
        with open(self.kg_file, 'w') as f:
            json.dump(kg, f, indent=2)
        
        return {
            "status": "success",
            "articles_collected": len(all_articles),
            "fetch_times": fetch_times,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def _extract_ticker_stats(self, articles: List[Dict]) -> Dict[str, Any]:
        """Extract ticker statistics from articles."""
        tickers = {}
        
        for article in articles:
            for ticker in article.get("tickers", []):
                if ticker not in tickers:
                    tickers[ticker] = {"count": 0}
                tickers[ticker]["count"] += 1
        
        return tickers


def main():
    """Run news ingestion pipeline."""
    setup_logging()
    
    pipeline = NewsIngestionPipeline(symbols=['bitcoin', 'btc', 'ethereum', 'eth'])
    
    result = pipeline.run_once()
    
    print("\n" + "=" * 60)
    print("NEWS INGESTION RESULTS")
    print("=" * 60)
    print(f"Articles collected: {result['articles_collected']}")
    sources = result.get('metadata', {}).get('sources', [])
    print(f"Sources: {sources}")
    print(f"Fetch times: {json.dumps(result['fetch_times'], indent=2)}")


if __name__ == "__main__":
    main()
