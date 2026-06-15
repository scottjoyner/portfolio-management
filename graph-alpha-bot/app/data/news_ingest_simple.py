#!/usr/bin/env python3
"""Simplified News Ingestion via MCP Server.

This module provides a simple news ingestion pipeline that stores results
locally (for development) or pushes to the Neo4j knowledge graph via MCP.

Usage:
    python3 app/data/news_ingest_simple.py --symbols BTC-USD ETH-USD --interval 5m
"""

import sys, os, json, time, hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import logging
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class Config:
    graph_server_url: str = os.getenv("GRAPH_SERVER_URL", "http://localhost:8080")
    processing_interval_seconds: int = 300
    max_articles_per_source: int = 50


class SimpleNewsIngestor:
    """Simple news ingestion without Neo4j dependency."""
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.cache_file = ".news_ingest_cache.json"
        
    def load_feeds(self) -> List[Tuple[str, str]]:
        """Return list of (feed_url, source_name) tuples."""
        return [
            ("https://www.coindesk.com/rdfeed/", "CoinDesk"),
            ("https://cointelegraph.com/news/feed", "Cointelegraph"),
            ("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "NYT Business"),
        ]
    
    def fetch_articles(self) -> List[dict]:
        """Fetch articles from all configured RSS feeds."""
        all_articles: List[dict] = []
        
        for feed_url, source_name in self.load_feeds():
            try:
                response = requests.get(feed_url, timeout=10)
                if response.status_code == 200:
                    articles = self.parse_rss(response.text, source_name)
                    all_articles.extend(articles)
            except Exception as e:
                logging.warning(f"Failed to fetch {feed_url}: {e}")
        
        return all_articles
    
    def parse_rss(self, xml_content: str, source: str) -> List[dict]:
        """Parse RSS XML content (simplified - no BeautifulSoup required)."""
        articles = []
        
        try:
            # Simple XML parsing without BeautifulSoup
            from xml.etree import ElementTree as ET
            
            root = ET.fromstring(xml_content)
            
            for item in root.findall(".//item"):
                title_el = item.find("title")
                link_el = item.find("link")
                
                if title_el is None or link_el is None:
                    continue
                
                pubdate_el = item.find("pubDate")
                published_at_str = pubdate_el.text if pubdate_el is not None else ""
                
                try:
                    published_at = datetime.strptime(
                        published_at_str, "%a, %d %b %Y %H:%M:%S %Z"
                    )
                except:
                    published_at = datetime.utcnow()
                
                articles.append({
                    "title": title_el.text or "",
                    "url": link_el.text or "",
                    "source": source,
                    "published_at": published_at,
                    "description": ""
                })
        except Exception as e:
            logging.warning(f"Failed to parse RSS: {e}")
        
        return articles
    
    def analyze_sentiment(self, title: str, content: str) -> Tuple[float, str]:
        """Simple sentiment analysis."""
        text = f"{title} {content}".lower()
        
        positive = sum(1 for word in ['gain', 'rise', 'surge', 'bullish', 'profit', 'breakthrough', 'success', 'upgrade'] if word in text)
        negative = sum(1 for word in ['fall', 'drop', 'crash', 'bearish', 'loss', 'downgrade', 'scandal', 'lawsuit'] if word in text)
        
        score = (positive - negative) / max(1, positive + negative)
        label = "positive" if score > 0.2 else "negative" if score < -0.2 else "neutral"
        
        return round(score, 3), label
    
    def extract_tickers(self, text: str) -> List[str]:
        """Extract crypto ticker symbols from text."""
        tickers = []
        for ticker in ['BTC', 'ETH', 'SOL', 'ADA', 'XRP', 'DOGE', 'AVAX', 'LINK', 'DOT']:
            if f"${ticker}" in text or f"{ticker}-USD" in text:
                tickers.append(f"{ticker}-USD")
        return list(set(tickers))
    
    def process_article(self, article: dict) -> Optional[dict]:
        """Process a single article."""
        title = article.get('title', '')
        
        if not title:
            return None
        
        score, label = self.analyze_sentiment(title, article.get('description', ''))
        tickers = self.extract_tickers(f"{title} {article.get('url', '')}")
        
        return {
            "id": hashlib.sha256(article['url'].encode()).hexdigest()[:16],
            "title": title,
            "url": article['url'],
            "source": article['source'],
            "published_at": article['published_at'].isoformat(),
            "sentiment_score": score,
            "sentiment_label": label,
            "tickers": tickers
        }
    
    def save_graph(self, articles: List[dict], path: str = "knowledge_graph.json"):
        """Save processed articles to a local JSON file."""
        try:
            if os.path.exists(path):
                with open(path) as f:
                    current_graph = json.load(f)
            else:
                current_graph = {"articles": [], "tickers": {}}
            
            # Add new articles
            existing_ids = {a['id'] for a in current_graph.get('articles', [])}
            new_articles = [a for a in articles if a['id'] not in existing_ids]
            current_graph['articles'].extend(new_articles)
            
            # Build ticker index
            for article in new_articles:
                for ticker in article['tickers']:
                    if ticker not in current_graph['tickers']:
                        current_graph['tickers'][ticker] = []
                    current_graph['tickers'][ticker].append(article['id'])
            
            with open(path, 'w') as f:
                json.dump(current_graph, f, indent=2)
            
            return len(new_articles)
        except Exception as e:
            logging.error(f"Failed to save graph: {e}")
            return 0
    
    def push_to_graph_server(
        self, 
        articles: List[dict],
        symbols: Optional[List[str]] = None
    ) -> dict:
        """Push processed articles to the Neo4j MCP server."""
        
        if not articles:
            return {"status": "ok", "count": 0}
        
        # First, create ticker nodes for all mentioned tickers
        unique_tickers = list(set(ticker for article in articles for ticker in article['tickers']))
        
        result = {"created_articles": 0, "created_tickers": 0, "errors": []}
        
        try:
            # Create ticker nodes first (they'll be referenced by news articles)
            for ticker in unique_tickers:
                try:
                    response = requests.post(
                        f"{self.config.graph_server_url}/signal/create",
                        json={"type": "ticker", "symbol": ticker},
                        timeout=5
                    )
                    if response.status_code == 201:
                        result["created_tickers"] += 1
                except Exception as e:
                    result["errors"].append(f"Failed to create {ticker}: {e}")
            
            # Create news article nodes with sentiment data
            for article in articles:
                try:
                    response = requests.post(
                        f"{self.config.graph_server_url}/signal/create",
                        json={
                            "type": "news",
                            "title": article['title'],
                            "url": article['url'],
                            "source": article['source'],
                            "sentiment_score": article['sentiment_score'],
                            "sentiment_label": article['sentiment_label'],
                            "timestamp": article['published_at'],
                            "tickers": article['tickers']
                        },
                        timeout=5
                    )
                    if response.status_code == 201:
                        result["created_articles"] += 1
                except Exception as e:
                    result["errors"].append(f"Failed to create article {article['id']}: {e}")
            
            return result
        
        except Exception as e:
            logging.error(f"Graph server error: {e}")
            return {"status": "error", "errors": [str(e)]}
    
    def run(
        self, 
        symbols: Optional[List[str]] = None,
        interval_seconds: Optional[int] = None
    ):
        """Run the news ingestion pipeline."""
        
        interval = interval_seconds or self.config.processing_interval_seconds
        
        print(f"Starting news ingestion for: {', '.join(symbols) if symbols else 'all feeds'}")
        print(f"Graph server: {self.config.graph_server_url}")
        print(f"Polling every {interval} seconds. Ctrl+C to stop.")
        
        last_run = datetime.utcnow() - timedelta(seconds=interval)
        
        try:
            while True:
                now = datetime.utcnow()
                
                if (now - last_run).total_seconds() >= interval:
                    # Fetch articles
                    articles = self.fetch_articles()
                    processed = [self.process_article(a) for a in articles]
                    processed = [a for a in processed if a is not None]
                    
                    logging.info(f"Processed {len(processed)} articles")
                    
                    # Save locally (for development/testing)
                    saved = self.save_graph(processed, "knowledge_graph.json")
                    
                    # Push to graph server if available
                    push_result = self.push_to_graph_server(processed, symbols)
                    
                    print(f"Saved {saved} new articles locally; pushed to graph: {push_result['created_articles']} articles, {push_result['created_tickers']} tickers")
                    
                    last_run = now
                
                time.sleep(60)  # Check every minute
        
        except KeyboardInterrupt:
            print("\nIngestion stopped by user.")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Simple News Ingestion Pipeline")
    parser.add_argument("--symbols", nargs='+', default=['BTC-USD', 'ETH-USD', 'SOL-USD'])
    parser.add_argument("--interval", type=int, default=300, help="Processing interval in seconds")
    
    args = parser.parse_args()
    
    config = Config()
    ingestor = SimpleNewsIngestor(config)
    ingestor.run(symbols=args.symbols, interval_seconds=args.interval)


if __name__ == "__main__":
    main()
