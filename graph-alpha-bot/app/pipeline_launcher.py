#!/usr/bin/env python3
"""
Production Pipeline Launcher for GraphAlphaBot

Coordinates news ingestion, signal generation, and paper trading execution.
Runs in continuous loop with proper error handling and auto-recovery.
"""

import sys, os, json, time, logging, threading, signal
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List, Any
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


class PipelineConfig:
    """Configuration for the pipeline."""
    
    def __init__(self):
        self.news_symbols = ['bitcoin', 'btc', 'ethereum', 'eth', 'solana', 'sol']
        self.sentiment_threshold_long = 0.3
        self.signal_cooldown_seconds = 900  # 15 minutes
        self.max_position_size_pct = 0.10


class SignalGenerator:
    """Generates trading signals from news sentiment."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.kg_file = Path("graph-alpha-bot/app/data/knowledge_graph.json")
        self.signal_cache_file = ".signal_cache.json"
        self.cached_signals: List[dict] = []
        self.last_signal_times: Dict[str, datetime] = {}
        
        try:
            if os.path.exists(self.signal_cache_file):
                with open(self.signal_cache_file) as f:
                    data = json.load(f)
                    self.cached_signals = data.get('signals', [])
        except Exception as e:
            logging.error(f"Failed to load signal cache: {e}")
    
    def generate_signals(self) -> List[dict]:
        """Generate trading signals from news sentiment."""
        
        new_signals = []
        
        if not self.kg_file.exists():
            logging.warning("No knowledge graph found - no signals generated")
            return []
        
        with open(self.kg_file) as f:
            kg = json.load(f)
        
        for symbol in ['BTC-USD', 'ETH-USD']:
            ticker_key = symbol.split('-')[0].lower()
            
            matching = [a for a in kg.get('articles', []) if any(t in a.get('tickers', []) for t in [ticker_key, ticker_key.upper()])]
            
            avg_sentiment = 0.0
            if len(matching) >= 2:
                avg_sentiment = sum(a.get('sentiment_score', 0.5) for a in matching) / len(matching)
                
                # Check cooldown
                if symbol in self.last_signal_times:
                    if datetime.now(timezone.utc) - self.last_signal_times[symbol] < timedelta(seconds=self.config.signal_cooldown_seconds):
                        continue
                
                if avg_sentiment >= self.config.sentiment_threshold_long:
                    signal = {
                        'symbol': symbol,
                        'direction': 'LONG',
                        'confidence': min(0.2 + abs(avg_sentiment) * 0.5, 0.9),
                        'sentiment_score': round(avg_sentiment, 3),
                        'news_count': len(matching),
                        'signal_reason': f"Positive news sentiment ({avg_sentiment:.2f}) from {len(matching)} recent articles",
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
                    new_signals.append(signal)
                    self.last_signal_times[symbol] = datetime.now(timezone.utc)
            
            logging.info(f"{symbol}: sentiment={avg_sentiment:.3f}, news={len(matching)}")
        
        # Save signals
        self.cached_signals.extend(new_signals)
        try:
            with open(self.signal_cache_file, 'w') as f:
                json.dump({'signals': self.cached_signals}, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save signal cache: {e}")
        
        return new_signals


class TradingExecutor:
    """Executes paper trades with circuit breaker protection."""
    
    def __init__(self):
        self.circuit_open = False
        self.circuit_opened_at: Optional[datetime] = None
        self.failure_count = 0
        
        # Simulated portfolio
        self.portfolio = {'USD': 100000.0, 'BTC': 0.5, 'ETH': 2.0}
        self.positions: Dict[str, dict] = {}
        
        self.prices = {
            'BTC-USD': 68500.0,
            'ETH-USD': 3450.0,
            'SOL-USD': 175.0,
        }
    
    def get_price(self, symbol: str) -> Optional[float]:
        return self.prices.get(symbol)
    
    def execute_signal(self, signal: dict) -> dict:
        """Execute a single trading signal."""
        
        # Check circuit breaker
        if self.circuit_open:
            if (datetime.now(timezone.utc) - self.circuit_opened_at) < timedelta(seconds=600):
                return {"status": "blocked", "error": "Circuit breaker open"}
            else:
                self.circuit_open = False
        
        symbol = signal['symbol']
        
        try:
            price = self.get_price(symbol)
            if not price:
                raise ValueError(f"No price for {symbol}")
            
            qty = min(0.10 * self.portfolio['USD'] / price, 2.0)
            order_value = price * qty
            
            # Execute buy order
            self.portfolio['USD'] -= order_value
            ticker = symbol.replace('-USD', '')
            self.portfolio[ticker] = self.portfolio.get(ticker, 0) + qty
            
            self.positions[symbol] = {
                'side': 'BUY',
                'quantity': round(qty, 6),
                'entry_price': price,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            return {
                "status": "filled",
                "order_id": f"buy_{symbol}_{int(time.time())}",
                "symbol": symbol,
                "side": "BUY",
                "quantity": round(qty, 6),
                "price": price,
                "value_usd": round(order_value, 2),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        
        except Exception as e:
            self.circuit_open = True
            self.circuit_opened_at = datetime.now(timezone.utc)
            return {"status": "failed", "error": str(e)}


class PipelineOrchestrator:
    """Main pipeline orchestrator."""
    
    def __init__(self):
        self.config = PipelineConfig()
        self.running = False
        
        self.signal_generator = SignalGenerator(self.config)
        self.trading_executor = TradingExecutor()
        
        logging.info("Pipeline orchestrator initialized")
    
    def run_cycle(self) -> Dict[str, Any]:
        """Run one complete pipeline cycle."""
        
        result = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "signals_generated": 0,
            "orders_executed": 0,
        }
        
        # Generate signals
        logging.info("Generating trading signals...")
        signals = self.signal_generator.generate_signals()
        result['signals_generated'] = len(signals)
        
        for sig in signals:
            print(f"\n📡 Signal: {sig['direction']} {sig['symbol']}")
            print(f"   Sentiment: {sig['sentiment_score']:.3f}")
            print(f"   Confidence: {sig['confidence']:.2f}")
            
            # Execute paper trade
            order = self.trading_executor.execute_signal(sig)
            print(f"   Order: {order.get('status')} - ${order.get('value_usd', 0):,.2f}")
            
            result['orders_executed'] += 1
        
        return result


def main():
    """Main pipeline loop."""
    
    orchestrator = PipelineOrchestrator()
    
    def shutdown_handler(signum, frame):
        print("\nShutting down...")
        orchestrator.running = False
    
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    
    print("🚀 GraphAlphaBot Pipeline starting...")
    print("=" * 60)
    
    while orchestrator.running:
        try:
            result = orchestrator.run_cycle()
            
            if result['signals_generated'] > 0:
                print(f"\n✅ Generated {result['signals_generated']} signal(s), executed {result['orders_executed']} order(s)")
            else:
                logging.debug("No new signals generated this cycle")
            
            # Wait for next cycle (5 minutes)
            time.sleep(300)
            
        except Exception as e:
            logging.error(f"Pipeline error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
