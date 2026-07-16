#!/usr/bin/env python3
"""
Parallel Pipeline Runner - Launches news ingestion and trade execution side-by-side.

This script provides the foundation for running multiple pipeline components in parallel
with proper resource isolation, health monitoring, and graceful shutdown.

Usage:
    python3 parallel_pipeline_runner.py [--config config.yaml]
"""

import sys, os, signal, time, logging, json, threading
from datetime import datetime, timedelta
from typing import Dict, Optional, Callable, List
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, Future


# Minimal logging setup (no external dependencies)
def setup_logging(log_file: str = None):
    # Use home directory for logs to avoid permission issues
    if log_file is None:
        home_dir = os.path.expanduser("~")
        log_dir = os.path.join(home_dir, ".cache", "graphalphabot", "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "pipeline.log")
    
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    logger = logging.getLogger('pipeline')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    
    return logger


logger = setup_logging()


class PipelineState(Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    RECOVERING = "recovering"
    ERROR = "error"


@dataclass
class PipelineConfig:
    """Configuration for pipeline components."""
    
    # Neo4j connection (x1-370)
    neo4j_uri: str = os.getenv("NEO4J_URI", "")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "")
    
    # News sources - all public RSS, no API keys needed
    news_sources: list = field(default_factory=lambda: [
        ("https://www.coindesk.com/rdfeed/", "CoinDesk"),
        ("https://cointelegraph.com/news/feed", "Cointelegraph"),
        ("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "NYT Business"),
    ])
    
    # Processing settings
    processing_interval_seconds: int = 300  # 5 minutes
    max_workers: int = 4
    
    # Symbols to track
    symbols: list = field(default_factory=lambda: [
        'BTC-USD', 'ETH-USD', 'SOL-USD', 'DOGE-USD', 'ADA-USD', 
        'XRP-USD', 'AVAX-USD', 'LINK-USD'
    ])


@dataclass
class HealthStatus:
    """Health status for a pipeline component."""
    
    name: str
    healthy: bool = True
    message: str = ""
    last_check: Optional[datetime] = None
    error_count: int = 0
    
    def record_error(self, message: str):
        self.error_count += 1
        self.message = message


class ComponentManager:
    """Manages lifecycle of pipeline components with health tracking."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.state = PipelineState.STOPPED
        self.health_checks: Dict[str, HealthStatus] = {}
        self.metrics = {
            'articles_processed': 0,
            'signals_generated': 0,
            'errors_total': 0
        }
    
    def register_component(self, name: str):
        """Register a new pipeline component."""
        self.health_checks[name] = HealthStatus(name=name)
    
    def get_health_status(self) -> Dict[str, HealthStatus]:
        return self.health_checks.copy()
    
    def increment_error_count(self, source: str):
        self.metrics['errors_total'] += 1
        if source in self.health_checks:
            self.health_checks[source].record_error(
                f"Error count: {self.health_checks[source].error_count}"
            )


class NewsIngestionPipeline:
    """News RSS feed ingestion with caching and rate limiting."""
    
    def __init__(self, config: PipelineConfig, component_manager: ComponentManager):
        self.config = config
        self.cm = component_manager
        
        # Cache for processed article IDs
        self.cache_file = ".news_cache.json"
        self.processed_ids: set = self._load_cache()
    
    def _load_cache(self) -> set:
        """Load previously processed article IDs."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file) as f:
                    data = json.load(f)
                    return set(data.get('processed_ids', []))
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
        return set()
    
    def _save_cache(self, new_ids: set):
        """Save processed article IDs."""
        try:
            data = {
                'processed_ids': list(set(self.processed_ids | new_ids)),
                'timestamp': datetime.utcnow().isoformat()
            }
            with open(self.cache_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
    
    def fetch_articles(self) -> List[dict]:
        """Fetch articles from RSS feeds."""
        import requests
        
        all_articles = []
        
        for feed_url, source_name in self.config.news_sources:
            try:
                response = requests.get(feed_url, timeout=10)
                if response.status_code == 200:
                    # Parse RSS (minimal implementation using xml.etree)
                    articles = self._parse_rss(response.text, source_name)
                    all_articles.extend(articles)
            except Exception as e:
                logger.warning(f"Failed to fetch {feed_url}: {e}")
                self.cm.increment_error_count("news_ingestion")
        
        return all_articles
    
    def _parse_rss(self, xml_content: str, source: str) -> List[dict]:
        """Parse RSS XML content."""
        from xml.etree import ElementTree as ET
        
        articles = []
        
        try:
            root = ET.fromstring(xml_content)
            
            for item in root.findall(".//item"):
                title_el = item.find("title")
                link_el = item.find("link")
                
                if title_el is None or link_el is None:
                    continue
                
                # Parse publication date
                pubdate_el = item.find("pubDate")
                try:
                    published_at_str = pubdate_el.text if pubdate_el is not None else ""
                    published_at = datetime.strptime(
                        published_at_str, "%a, %d %b %Y %H:%M:%S %Z"
                    )
                except:
                    published_at = datetime.utcnow()
                
                articles.append({
                    'title': title_el.text or "",
                    'url': link_el.text or "",
                    'source': source,
                    'published_at': published_at,
                    'description': ""
                })
        
        except Exception as e:
            logger.warning(f"Failed to parse RSS: {e}")
        
        return articles
    
    def run(self) -> int:
        """Run one cycle of news ingestion. Returns number of new articles."""
        
        # Fetch articles
        raw_articles = self.fetch_articles()
        
        # Filter by recency (last 7 days)
        cutoff = datetime.utcnow() - timedelta(days=7)
        recent_articles = [a for a in raw_articles if a['published_at'] > cutoff]
        
        logger.info(f"Found {len(recent_articles)} articles from last 7 days")
        
        self._save_cache(set())  # Store metadata, actual processing happens elsewhere
        return len(recent_articles)


class SignalGenerator:
    """Generates trading signals from processed data."""
    
    def __init__(self, config: PipelineConfig, component_manager: ComponentManager):
        self.config = config
        self.cm = component_manager
        
        self.metrics = {
            'signals_generated': 0
        }
    
    def generate_signals(self) -> List[dict]:
        """Generate trading signals.
        
        This is a placeholder - in production it would query Neo4j for:
        1. Sentiment scores from news articles mentioning each ticker
        2. Price momentum indicators
        3. Correlation patterns across tickers
        """
        import uuid
        
        signals = []
        
        # Placeholder signal generation based on ticker hash
        for symbol in self.config.symbols:
            score = (hash(symbol) % 1000) / 1000 - 0.5
            
            if abs(score) > 0.2:  # Only significant signals
                signals.append({
                    'id': str(uuid.uuid4())[:8],
                    'ticker': symbol,
                    'score': round(score, 3),
                    'timestamp': datetime.utcnow().isoformat(),
                    'source': 'signal_generator',
                    'action': 'buy' if score > 0 else 'sell'
                })
        
        self.metrics['signals_generated'] += len(signals)
        return signals


class PipelineRunner:
    """Main pipeline runner that coordinates all components."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = PipelineConfig()
        if config_path and os.path.exists(config_path):
            # Load YAML config (would use yaml.safe_load)
            logger.info(f"Loading config from {config_path}")
        
        self.cm = ComponentManager(self.config)
        
        # Initialize components
        self.news_pipeline = NewsIngestionPipeline(self.config, self.cm)
        self.signal_generator = SignalGenerator(self.config, self.cm)
        
        # State management
        self.state = PipelineState.STOPPED
        self.shutdown_event = threading.Event()
        
        # Worker pool
        self.max_workers = self.config.max_workers
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # Metrics collection interval
        self.metrics_interval = 60  # seconds
    
    def start(self):
        """Start the pipeline runner."""
        logger.info("Starting PipelineRunner")
        self.state = PipelineState.STARTING
        
        try:
            # Register components for health monitoring
            self.cm.register_component("news_ingestion")
            self.cm.register_component("signal_generator")
            
            self.state = PipelineState.RUNNING
            
            # Set up signal handlers for graceful shutdown
            signal.signal(signal.SIGTERM, self._handle_shutdown)
            signal.signal(signal.SIGINT, self._handle_shutdown)
            
        except Exception as e:
            logger.error(f"Failed to start: {e}")
            self.state = PipelineState.ERROR
    
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Received shutdown signal")
        self.state = PipelineState.PAUSING
        self.shutdown_event.set()
    
    def run_cycle(self) -> Dict[str, any]:
        """Run one complete pipeline cycle. Returns metrics dict."""
        
        cycle_start = time.time()
        metrics = {
            'articles_fetched': 0,
            'signals_generated': 0,
            'errors': 0,
            'duration_seconds': 0.0
        }
        
        try:
            # Step 1: Fetch news articles
            if self.state == PipelineState.RUNNING:
                articles = self.news_pipeline.fetch_articles()
                metrics['articles_fetched'] = len(articles)
                
            # Step 2: Generate trading signals
            signals = self.signal_generator.generate_signals()
            metrics['signals_generated'] = len(signals)
            
        except Exception as e:
            logger.error(f"Cycle failed: {e}")
            self.cm.increment_error_count("pipeline")
            metrics['errors'] += 1
            
            if self.state != PipelineState.ERROR:
                self.state = PipelineState.RECOVERING
        
        metrics['duration_seconds'] = time.time() - cycle_start
        return metrics
    
    def run(self):
        """Main pipeline loop."""
        
        self.start()
        
        while not self.shutdown_event.is_set():
            try:
                # Run one cycle
                metrics = self.run_cycle()
                
                log_msg = (
                    f"Cycle complete | Articles: {metrics['articles_fetched']} | "
                    f"Signals: {metrics['signals_generated']} | "
                    f"Errors: {metrics['errors']} | Duration: {metrics['duration_seconds']:.1f}s"
                )
                
                if metrics['errors'] > 0:
                    logger.warning(log_msg)
                else:
                    logger.info(log_msg)
                
                # Update state after recovery
                if self.state == PipelineState.RECOVERING and metrics['errors'] == 0:
                    logger.info("Recovery successful, resuming normal operation")
                    self.state = PipelineState.RUNNING
                
                # Sleep until next cycle
                sleep_time = max(0, self.config.processing_interval_seconds - 
                               metrics['duration_seconds'])
                
                # Check for shutdown every minute
                if not self.shutdown_event.is_set():
                    time.sleep(sleep_time)
            
            except Exception as e:
                logger.error(f"Pipeline error: {e}", exc_info=True)
                self.state = PipelineState.ERROR
                time.sleep(10)  # Graceful backoff
    
    def shutdown(self):
        """Gracefully shut down the pipeline."""
        logger.info("Shutting down PipelineRunner...")
        
        self.shutdown_event.set()
        
        # Shutdown worker pool
        self.executor.shutdown(wait=True)
        
        logger.info("PipelineRunner shut down complete")


def main():
    """Entry point for parallel pipeline runner."""
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run news ingestion and trade execution pipelines in parallel"
    )
    parser.add_argument(
        "--config", 
        default=None,
        help="Path to configuration YAML file"
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Run a single cycle and exit for testing"
    )
    
    args = parser.parse_args()
    
    runner = PipelineRunner(config_path=args.config)
    
    try:
        if args.test_mode:
            # Run one cycle for testing
            logger.info("Running in test mode (single cycle)")
            metrics = runner.run_cycle()
            print(json.dumps(metrics, indent=2))
        else:
            # Run continuously
            runner.run()
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        runner.shutdown()


if __name__ == "__main__":
    main()
