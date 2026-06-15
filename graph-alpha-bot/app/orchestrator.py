#!/usr/bin/env python3
"""
GraphAlphaBot Pipeline Orchestrator

Main orchestrator that coordinates news ingestion, signal generation, and paper trading.
"""

import sys, os, json, time, logging, threading, signal
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


def setup_logging(log_file: str = None):
    if log_file is None:
        home_dir = os.path.expanduser("~")
        log_file = f"{home_dir}/.hermes/log/graphalphabot/pipeline.log"
    
    Path(os.path.dirname(log_file)).mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
    )


class ComponentState(Enum):
    IDLE = "idle"
    RUNNING = "running"
    FAILED = "failed"


@dataclass
class PipelineConfig:
    name: str = "GraphAlphaBot"
    news_symbols: List[str] = field(default_factory=lambda: ['BTC-USD', 'ETH-USD', 'SOL-USD'])
    news_interval_minutes: int = 5
    sentiment_threshold_long: float = 0.3
    sentiment_threshold_short: float = -0.3
    signal_cooldown_seconds: int = 900
    max_position_size_pct: float = 0.10
    failure_threshold: int = 5


class PipelineOrchestrator:
    """Main pipeline orchestrator."""
    
    def __init__(self):
        self.config = PipelineConfig()
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Component states
        self.component_states: Dict[str, dict] = {
            "news_ingestion": {"state": ComponentState.IDLE, "last_run": None, "error_count": 0},
            "signal_generation": {"state": ComponentState.IDLE, "last_run": None, "error_count": 0},
            "trading_executor": {"state": ComponentState.IDLE, "last_run": None, "error_count": 0}
        }
        
        # Circuit breaker for trading
        self.circuit_open = False
        self.circuit_opened_at: Optional[datetime] = None
        
        # Signal cache
        self.signal_cache_file = ".signal_cache.json"
        self.cached_signals: List[dict] = []
        self.last_signal_times: Dict[str, datetime] = {}
        
        # Price cache
        self.prices = {'BTC-USD': 68500.0, 'ETH-USD': 3450.0, 'SOL-USD': 175.0}
        
        # Simulated portfolio
        self.portfolio = {'USD': 100000.0, 'BTC': 0.5, 'ETH': 2.0}
        self.positions: Dict[str, dict] = {}
        
        logging.info("Pipeline orchestrator initialized")
    
    def run_signal_generation(self) -> List[dict]:
        """Generate trading signals from news sentiment."""
        
        state = self.component_states["signal_generation"]
        state["state"] = ComponentState.RUNNING
        
        new_signals = []
        
        for symbol in self.config.news_symbols:
            # Get recent news sentiment from knowledge graph
            kg_file = Path("graph-alpha-bot/app/data/knowledge_graph.json")
            
            avg_sentiment = 0.0
            news_count = 0
            
            if kg_file.exists():
                try:
                    with open(kg_file) as f:
                        kg = json.load(f)
                    
                    cutoff = datetime.utcnow() - timedelta(hours=6)
                    for article in kg.get('articles', []):
                        pub_time = datetime.fromisoformat(
                            article['published_at'].replace('Z', '+00:00')
                        ).replace(tzinfo=None)
                        
                        if pub_time >= cutoff and symbol.replace('-USD', '') in article.get('tickers', []):
                            avg_sentiment += article.get('sentiment_score', 0.0)
                            news_count += 1
                    
                    if news_count > 0:
                        avg_sentiment /= news_count
                        
                except Exception as e:
                    logging.warning(f"Failed to read knowledge graph for {symbol}: {e}")
            
            # Check cooldown
            if symbol in self.last_signal_times:
                if datetime.utcnow() - self.last_signal_times[symbol] < timedelta(seconds=self.config.signal_cooldown_seconds):
                    continue
            
            # Generate signal
            if news_count >= 2 and avg_sentiment >= self.config.sentiment_threshold_long:
                signal = {
                    'symbol': symbol,
                    'direction': 'LONG',
                    'confidence': min(0.2 + abs(avg_sentiment) * 0.5, 0.9),
                    'sentiment_score': round(avg_sentiment, 3),
                    'news_count': news_count,
                    'signal_reason': f"Positive sentiment: {avg_sentiment:.2f} from {news_count} articles",
                    'timestamp': datetime.utcnow().isoformat()
                }
                new_signals.append(signal)
                self.last_signal_times[symbol] = datetime.utcnow()
            
            logging.info(f"{symbol}: sentiment={avg_sentiment:.3f}, news={news_count}")
        
        # Save signals
        self.cached_signals.extend(new_signals)
        try:
            with open(self.signal_cache_file, 'w') as f:
                json.dump({'signals': self.cached_signals}, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save signal cache: {e}")
        
        state["last_run"] = datetime.utcnow()
        return new_signals
    
    def execute_signal(self, signal: dict) -> dict:
        """Execute a single trading signal."""
        
        # Check circuit breaker
        if self.circuit_open:
            if (datetime.utcnow() - self.circuit_opened_at) < timedelta(seconds=600):
                return {"status": "blocked", "error": "Circuit breaker open"}
            else:
                self.circuit_open = False
        
        symbol = signal['symbol']
        
        try:
            price = self.prices.get(symbol)
            if not price:
                raise ValueError(f"No price for {symbol}")
            
            qty = min(0.10 * self.portfolio['USD'] / price, 2.0)
            
            # Execute buy order
            order_value = price * qty
            self.portfolio['USD'] -= order_value
            ticker = symbol.replace('-USD', '')
            self.portfolio[ticker] = self.portfolio.get(ticker, 0) + qty
            
            self.positions[symbol] = {
                'side': 'BUY',
                'quantity': round(qty, 6),
                'entry_price': price,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return {
                "status": "filled",
                "order_id": f"buy_{symbol}_{int(time.time())}",
                "symbol": symbol,
                "side": "BUY",
                "quantity": round(qty, 6),
                "price": price,
                "value_usd": round(order_value, 2),
                "timestamp": datetime.utcnow().isoformat()
            }
        
        except Exception as e:
            self.circuit_open = True
            self.circuit_opened_at = datetime.utcnow()
            return {"status": "failed", "error": str(e)}


def main():
    """Main pipeline loop."""
    
    orchestrator = PipelineOrchestrator()
    
    def shutdown_handler(signum, frame):
        print("\nShutting down...")
        orchestrator.running = False
    
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    
    print("🚀 GraphAlphaBot starting...")
    
    while orchestrator.running:
        try:
            # Generate signals
            signals = orchestrator.run_signal_generation()
            
            if signals:
                print(f"\n{len(signals)} new signal(s) generated:")
                
                for sig in signals:
                    print(json.dumps(sig, indent=2))
                    
                    # Execute signal (paper trade)
                    order = orchestrator.execute_signal(sig)
                    print(f"Order: {order.get('status')}")
            else:
                logging.debug("No new signals generated this cycle")
            
            # Wait for next cycle
            time.sleep(300)  # 5 minute intervals
            
        except Exception as e:
            logging.error(f"Pipeline error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
