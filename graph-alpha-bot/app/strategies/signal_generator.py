#!/usr/bin/env python3
"""Signal generation module using news sentiment + price data."""

import sys, os, json, time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field


@dataclass
class SignalConfig:
    """Configuration for signal generation."""
    symbols: List[str] = field(default_factory=lambda: ['BTC-USD', 'ETH-USD', 'SOL-USD'])
    sentiment_threshold_long: float = 0.3
    sentiment_threshold_short: float = -0.3
    price_momentum_window: int = 20
    max_signals_per_symbol: int = 5
    signal_cooldown_minutes: int = 15


@dataclass
class TradingSignal:
    """Represents a trading signal."""
    symbol: str
    direction: str  # 'LONG', 'SHORT', or 'CLOSE'
    confidence: float  # 0.0 to 1.0
    sentiment_score: float  # -1.0 to +1.0
    price_change_pct: float
    signal_reason: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> dict:
        return {
            'symbol': self.symbol,
            'direction': self.direction,
            'confidence': round(self.confidence, 3),
            'sentiment_score': round(self.sentiment_score, 3),
            'price_change_pct': round(self.price_change_pct, 4),
            'signal_reason': self.signal_reason,
            'timestamp': self.timestamp.isoformat()
        }


class SignalGenerator:
    """Generates trading signals from news sentiment and price data."""
    
    def __init__(self, config: Optional[SignalConfig] = None):
        self.config = config or SignalConfig()
        self.signal_cache_file = '.signal_cache.json'
        self.last_signal_times: Dict[str, datetime] = {}
        
        # Load cached signals
        self._load_cache()
    
    def _load_cache(self):
        """Load previously generated signals."""
        try:
            if os.path.exists(self.signal_cache_file):
                with open(self.signal_cache_file) as f:
                    self.cached_signals = json.load(f)
            else:
                self.cached_signals = {'signals': []}
        except Exception as e:
            print(f"Failed to load cache: {e}")
            self.cached_signals = {"signals": []}
    
    def _save_cache(self):
        """Save signal cache."""
        try:
            with open(self.signal_cache_file, 'w') as f:
                json.dump(self.cached_signals, f, indent=2)
        except Exception as e:
            print(f"Failed to save cache: {e}")
    
    def _get_recent_news_sentiment(
        self, 
        symbol: str, 
        hours: int = 6
    ) -> Tuple[float, List[dict]]:
        """Get average sentiment from recent news articles about a symbol."""
        
        # Try Neo4j first if available
        try:
            sys.path.insert(0, 'graph-alpha-bot')
            from app.db.neo4j_connection import get_connection
            
            conn = get_connection()
            if conn.is_healthy():
                query = """
                MATCH (a:NewsArticle)-[:MENTIONS]->(t:Ticker {symbol: $ticker})
                WHERE a.published_at >= datetime() - duration({hours: $hours})
                RETURN avg(a.sentiment_score) as avg_sentiment, collect(a) as articles
                """
                result = conn.execute_query(
                    query, 
                    {"ticker": symbol, "hours": hours},
                    limit=100
                )
                if result and len(result) > 0:
                    score = float(result[0][0])
                    articles_list = []
                    for r in result[1]:
                        article_dict = {'title': r[1], 'sentiment_score': r[2]} if len(r) > 2 else {}
                        articles_list.append(article_dict)
                    return (score, articles_list[:5])
        except Exception as e:
            print(f"Neo4j query failed, falling back to local: {e}")
        
        # Fallback to local JSON graph
        try:
            if os.path.exists('knowledge_graph.json'):
                with open('knowledge_graph.json') as f:
                    kg = json.load(f)
                
                # Find articles mentioning this symbol within the time window
                cutoff = datetime.utcnow() - timedelta(hours=hours)
                matching_articles = []
                
                for article in kg.get('articles', []):
                    pub_time = datetime.fromisoformat(article['published_at'].replace('Z', '+00:00')).replace(tzinfo=None)
                    if pub_time >= cutoff and symbol in article.get('tickers', []):
                        matching_articles.append(article)
                
                if matching_articles:
                    avg_sentiment = sum(a['sentiment_score'] for a in matching_articles) / len(matching_articles)
                    return (avg_sentiment, matching_articles[:5])
        except Exception as e:
            print(f"Local graph read failed: {e}")
        
        # No news available - return neutral sentiment
        return (0.0, [])
    
    def _get_price_data(self, symbol: str) -> Tuple[float, float]:
        """Get current price and 24h change for a symbol."""
        
        try:
            import yfinance as yf
            
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # Get current and historical prices
            history = ticker.history(period="1d")
            
            if not history.empty:
                current_price = float(history['Close'].iloc[-1])
                prev_close = float(history['Close'].iloc[0])
                change_pct = (current_price - prev_close) / prev_close * 100
                
                return (current_price, round(change_pct, 4))
            else:
                # Fallback to info
                current = info.get('currentPrice', 0.0) if info else 0.0
                day_change = info.get('regularMarketChangePercent', 0.0) if info else 0.0
                
                return (round(current, 2), round(day_change, 4))
        except Exception as e:
            print(f"Price fetch failed for {symbol}: {e}")
            # Return hardcoded fallback prices
            price_map = {
                'BTC-USD': 68500.0, 'ETH-USD': 3450.0, 'SOL-USD': 175.0
            }
            return (price_map.get(symbol, 100.0), 0.0)
    
    def _analyze_sentiment_for_signal(
        self, 
        sentiment_score: float, 
        price_change: float,
        news_count: int
    ) -> Optional[TradingSignal]:
        """Analyze whether to generate a signal based on sentiment and price movement."""
        
        # Require some news coverage for confidence
        if news_count < 2:
            return None
        
        # Calculate composite signal score
        # News sentiment contributes up to 0.5, price momentum up to 0.3, base confidence 0.2
        
        sentiment_contribution = abs(sentiment_score) * 0.5
        price_momentum_contribution = min(abs(price_change) / 10, 0.1)  # Cap at 10% change
        
        if sentiment_score >= self.config.sentiment_threshold_long:
            direction = 'LONG'
            base_confidence = 0.2 + sentiment_contribution + price_momentum_contribution
            
            # Boost confidence for strong alignment
            if price_change > 2:  # Price already moving up
                base_confidence += 0.15
            
            signal_reason = (
                f"Positive news sentiment ({sentiment_score:.2f}) with "
                f"{abs(price_change):.1f}% price change over 24h"
            )
        
        elif sentiment_score <= self.config.sentiment_threshold_short:
            direction = 'SHORT'
            base_confidence = 0.2 + sentiment_contribution + price_momentum_contribution
            
            # Boost confidence if price also declining
            if price_change < -2:
                base_confidence += 0.15
            
            signal_reason = (
                f"Negative news sentiment ({sentiment_score:.2f}) with "
                f"{abs(price_change):.1f}% price decline over 24h"
            )
        
        else:
            return None
        
        # Add sentiment detail to reason
        news_sources = set()
        
        return TradingSignal(
            symbol=self._get_target_symbol(),
            direction=direction,
            confidence=min(round(base_confidence, 3), 0.95),
            sentiment_score=sentiment_score,
            price_change_pct=price_change,
            signal_reason=f"{signal_reason} ({news_sources} news sources)" if news_sources else f"Sentiment-driven {direction.lower()}: {sentiment_score:.2f}",
            timestamp=datetime.utcnow()
        )
    
    def _get_target_symbol(self) -> str:
        """Get the next symbol to generate a signal for."""
        # Round-robin through symbols
        symbols = self.config.symbols
        if not self.last_signal_times or max(self.last_signal_times.values()) < datetime.utcnow() - timedelta(minutes=30):
            return symbols[0]
        
        # Find least recently queried symbol
        sorted_symbols = sorted(
            symbols, 
            key=lambda s: self.last_signal_times.get(s, datetime.min)
        )
        return sorted_symbols[0]
    
    def generate_signals(self) -> List[TradingSignal]:
        """Generate trading signals for all configured symbols."""
        
        print(f"Generating signals for {len(self.config.symbols)} symbols...")
        new_signals = []
        
        for symbol in self.config.symbols:
            # Get sentiment score from news
            avg_sentiment, articles = self._get_recent_news_sentiment(symbol)
            
            # Get price data
            current_price, price_change_pct = self._get_price_data(symbol)
            
            print(f"  {symbol}: sentiment={avg_sentiment:.2f}, "
                  f"news_articles={len(articles)}, price_change={price_change_pct}%")
            
            # Check if we should generate a signal
            signal = self._analyze_sentiment_for_signal(avg_sentiment, price_change_pct, len(articles))
            
            if signal:
                new_signals.append(signal)
        
        # Save new signals
        for sig in new_signals:
            self.cached_signals['signals'].append(sig.to_dict())
            self.last_signal_times[sig.symbol] = sig.timestamp
        
        self._save_cache()
        
        return new_signals


def main():
    """Run signal generation."""
    
    config = SignalConfig(
        symbols=['BTC-USD', 'ETH-USD', 'SOL-USD'],
        sentiment_threshold_long=0.3,
        sentiment_threshold_short=-0.3,
        max_signals_per_symbol=5,
        signal_cooldown_minutes=15
    )
    
    generator = SignalGenerator(config)
    signals = generator.generate_signals()
    
    print(f"\nGenerated {len(signals)} signals:")
    for sig in signals:
        print(json.dumps(sig.to_dict(), indent=2))
    
    return signals


if __name__ == "__main__":
    main()
