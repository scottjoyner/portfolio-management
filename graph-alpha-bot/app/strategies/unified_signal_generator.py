#!/usr/bin/env python3
"""
Unified Signal Generator - Combines News Sentiment + BTC-XXX Volatility Strategies

This module integrates:
1. News sentiment analysis (existing signal_generator.py functionality)
2. BTC-XXX volatility trading strategies (newly implemented)
3. Enhanced symbol support for all BTC market pairs

This provides a comprehensive signal generation system for the backtesting framework.
"""

import sys, os, json, time, logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum

# Import our enhanced backtesting strategies from the backtesting framework
sys.path.insert(0, '/home/scott/git/portfolio-management')
from backtester import (
    BTCVolatilityStacking,
    BTCVolatilityBreakout,
    BTCVolatilityMeanReversion,
    BTCVolatilityMomentum,
    CoinbaseMomentumStrategy,
    CoinbaseMeanReversionStrategy,
    VolatilityBreakoutStrategy,
    RegimeAwareAdaptiveStrategy,
    VolumeProfileStrategy,
    MultiTimeframeConfluenceStrategy,
    OrderFlowPressureStrategy,
    VolatilityContractionExpansionStrategy,
    StatisticalArbitrageZScorePairStrategy,
    LiquidationHeatmapStrategy,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SignalDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    CLOSE = "CLOSE"


@dataclass
class UnifiedTradingSignal:
    """A unified trading signal combining multiple sources."""

    symbol: str
    direction: str  # 'LONG', 'SHORT', or 'CLOSE'
    confidence: float  # 0.0 to 1.0
    sentiment_score: float  # -1.0 to +1.0 (news analysis)
    technical_score: float  # -1.0 to +1.0 (strategy-based)
    price_change_pct: float  # 24h change
    news_count: int  # Number of relevant articles
    signal_reason: str
    strategy_name: str  # Name of the strategy that generated the signal
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "confidence": round(self.confidence, 3),
            "sentiment_score": round(self.sentiment_score, 3),
            "technical_score": round(self.technical_score, 3),
            "price_change_pct": round(self.price_change_pct, 4),
            "news_count": self.news_count,
            "signal_reason": self.signal_reason,
            "strategy_name": self.strategy_name,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class UnifiedSignalConfig:
    """Configuration for unified signal generation."""

    symbols: List[str] = field(
        default_factory=lambda: [
            "BTC-USD", "BTC-ETH", "BTC-SOL", "BTC-DOGE", "BTC-XRP",
            "BTC-ADA", "BTC-DOT", "BTC-MATIC", "BTC-SHIB", "BTC-AVAX",
            "BTC-UNI", "BTC-SNX", "BTC-YFI", "BTC-AAVE", "BTC-MKR",
            "BTC-COMP", "BTC-LINK", "BTC-BAT", "BTC-ZRX"
        ]
    )
    news_hours: int = 6  # Hours of news to analyze
    sentiment_threshold: float = 0.25  # Min abs(sentiment) for signal
    max_position_pct: float = 0.10  # Max 10% per position
    cooldown_minutes: int = 15  # Minimum time between signals per symbol
    enable_strategy_signals: bool = True  # Enable strategy-based signals
    enable_news_signals: bool = True  # Enable news sentiment signals


class NewsSentimentAnalyzer:
    """Analyzes news sentiment for trading signals with topic & freshness."""

    def __init__(self, symbols: List[str]):
        self.symbols = [s.replace("-USD", "") for s in symbols]

    def analyze_sentiment(self) -> Dict[str, Tuple[float, int]]:
        """Legacy method — returns {symbol: (avg_sentiment, count)}."""
        results = {}
        for sym, info in self.analyze_full().items():
            results[sym] = (info["avg_sentiment"], info["count"])
        return results

    def analyze_full(self) -> Dict[str, dict]:
        """Rich analysis including topic, freshness, and breaking news.

        Returns {symbol: {
            "avg_sentiment": float, "count": int,
            "freshness_weighted_sentiment": float,
            "topics": list[str], "breaking_ratio": float,
            "regulation_count": int, "hack_count": int,
            "technology_count": int, "adoption_count": int,
        }}
        """
        try:
            kg_file = "app/data/knowledge_graph.json"
            if not os.path.exists(kg_file):
                kg_file = "graph-alpha-bot/app/data/knowledge_graph.json"
            if not os.path.exists(kg_file):
                return {}

            with open(kg_file) as f:
                data = json.load(f)

            articles = data.get("articles", [])
            logger.info(f"Loaded {len(articles)} articles for rich sentiment analysis")
            topic_volume = data.get("topic_volume", {})

            TOPIC_SENTIMENT_MAP = {
                "regulation": -0.2, "hacks_security": -0.3,
                "technology": 0.15, "adoption": 0.25,
                "partnerships": 0.2, "price_analysis": 0.0,
                "mining": 0.0, "macro": 0.0,
            }

            results = {}
            for symbol in self.symbols:
                symbol_upper = symbol.upper()
                symbol_usd = f"{symbol_upper}-USD"
                matching = [
                    a for a in articles
                    if symbol.lower() in str(a.get("title", "")).lower()
                    or symbol_upper in str(a.get("title", "")).upper()
                    or symbol_usd in a.get("tickers", [])
                ]
                if not matching:
                    continue

                sentiments = []
                fresh_weighted = 0
                total_fresh = 0
                topics = set()
                regulation_count = 0
                hack_count = 0
                technology_count = 0
                adoption_count = 0
                breaking_count = 0

                for a in matching:
                    score = a.get("sentiment_score", 0.5)
                    freshness = a.get("freshness", 0.5)
                    topic = a.get("topic")
                    is_breaking = a.get("is_breaking", False)

                    sentiments.append(score)
                    fresh_weighted += score * freshness
                    total_fresh += freshness
                    if topic:
                        topics.add(topic)
                    if topic == "regulation":
                        regulation_count += 1
                    elif topic == "hacks_security":
                        hack_count += 1
                    elif topic == "technology":
                        technology_count += 1
                    elif topic == "adoption":
                        adoption_count += 1
                    if is_breaking:
                        breaking_count += 1

                avg_s = sum(sentiments) / len(sentiments)
                fresh_s = fresh_weighted / total_fresh if total_fresh > 0 else avg_s
                breaking_ratio = breaking_count / len(matching)

                # Apply topic sentiment bias
                topic_bias = 0
                for t in topics:
                    topic_bias += TOPIC_SENTIMENT_MAP.get(t, 0)
                topic_bias = max(-0.3, min(0.3, topic_bias))

                results[symbol] = {
                    "avg_sentiment": round(avg_s, 3),
                    "freshness_weighted_sentiment": round(fresh_s, 3),
                    "topic_adjusted_sentiment": round(max(-1, min(1, fresh_s + topic_bias)), 3),
                    "count": len(matching),
                    "topics": list(topics),
                    "breaking_ratio": round(breaking_ratio, 3),
                    "regulation_count": regulation_count,
                    "hack_count": hack_count,
                    "technology_count": technology_count,
                    "adoption_count": adoption_count,
                }

            return results
        except Exception as e:
            logger.error(f"Rich sentiment analysis failed: {e}")
            return {}


class StrategySignalGenerator:
    """Generates signals from BTC-XXX volatility trading strategies."""

    def __init__(self):
        # Initialize all our BTC-XXX volatility strategies
        self.strategies = {
            "BTCVolatilityStacking": BTCVolatilityStacking(20, 14, 14, 0.02, 0.04),
            "BTCVolatilityBreakout": BTCVolatilityBreakout(20, 10, 14, 0.02),
            "BTCVolatilityMeanReversion": BTCVolatilityMeanReversion(30, 20, 2.0, 0.5, 14),
            "BTCVolatilityMomentum": BTCVolatilityMomentum(20, 10, 14, 14, 0.02),
            "CoinbaseMomentum": CoinbaseMomentumStrategy(),
            "CoinbaseMeanReversion": CoinbaseMeanReversionStrategy(),
            "VolatilityBreakout": VolatilityBreakoutStrategy(),
            "RegimeAwareAdaptive": RegimeAwareAdaptiveStrategy(),
            "VolumeProfile": VolumeProfileStrategy(),
            "MultiTFConfluence": MultiTimeframeConfluenceStrategy(),
            "OrderFlowPressure": OrderFlowPressureStrategy(),
            "VolContractionExpansion": VolatilityContractionExpansionStrategy(),
            "ZScorePairArb": StatisticalArbitrageZScorePairStrategy(),
            "LiquidationHeatmap": LiquidationHeatmapStrategy(),
        }

    def generate_strategy_signals(self, symbol: str, data: List[Dict]) -> List[UnifiedTradingSignal]:
        """Generate signals from strategies for a specific symbol."""
        signals = []
        closes = [d['close'] for d in data] if data else []

        for strategy_name, strategy in self.strategies.items():
            try:
                # Generate signals using the strategy
                raw_signals = strategy.generate_signals(data)

                if raw_signals:
                    # Convert raw signals to unified format
                    for action, price in raw_signals:
                        direction = "LONG" if action == "BUY" else "SHORT" if action == "SELL" else "CLOSE"

                        # Calculate confidence based on strategy type
                        confidence = self._calculate_strategy_confidence(strategy_name, raw_signals)

                        # Calculate technical score
                        technical_score = self._calculate_technical_score(strategy_name, raw_signals)

                        # Get approximate recent price change
                        price_change = ((closes[-1] - closes[0]) / closes[0] * 100) if len(closes) > 1 else 0

                        signal = UnifiedTradingSignal(
                            symbol=symbol,
                            direction=direction,
                            confidence=confidence,
                            sentiment_score=0.0,  # No sentiment for strategy signals
                            technical_score=technical_score,
                            price_change_pct=price_change,
                            news_count=0,  # No news for strategy signals
                            signal_reason=f"{strategy_name}: {direction} signal at ${price:.2f}",
                            strategy_name=strategy_name,
                            timestamp=datetime.now()
                        )

                        signals.append(signal)

            except Exception as e:
                logger.error(f"Strategy {strategy_name} failed for {symbol}: {e}")
                continue

        return signals

    def _calculate_strategy_confidence(self, strategy_name: str, signals: List[Tuple[str, float]]) -> float:
        """Calculate confidence score for strategy signals."""
        if not signals:
            return 0.0

        # Different confidence calculation for different strategies
        if strategy_name in ["BTCVolatilityStacking", "CoinbaseMomentum", "MultiTFConfluence"]:
            # Higher confidence for momentum / multi-TF confluence
            return min(0.95, 0.5 + (len([s for s in signals if s[0] == "BUY"]) / len(signals)) * 0.4)
        elif strategy_name in ["BTCVolatilityBreakout", "BTCVolatilityMeanReversion", "VolatilityBreakout", "VolContractionExpansion"]:
            # Balanced confidence for volatility/breakout strategies
            return min(0.90, 0.6 + (len([s for s in signals if s[0] == "BUY"]) / len(signals)) * 0.3)
        elif strategy_name in ["BTCVolatilityMomentum", "RegimeAwareAdaptive", "LiquidationHeatmap"]:
            # Moderate confidence for regime-aware / liquidation detection
            return min(0.85, 0.4 + (len([s for s in signals if s[0] == "BUY"]) / len(signals)) * 0.35)
        elif strategy_name in ["VolumeProfile", "OrderFlowPressure"]:
            # Higher base — these are microstructure/inventory strategies
            return min(0.88, 0.55 + (len([s for s in signals if s[0] == "BUY"]) / len(signals)) * 0.3)
        elif strategy_name in ["ZScorePairArb"]:
            # Statistical arbitrage — mean-reversion base
            return min(0.80, 0.5 + (len([s for s in signals if s[0] == "BUY"]) / len(signals)) * 0.25)
        else:
            return 0.7

    def _calculate_technical_score(self, strategy_name: str, signals: List[Tuple[str, float]]) -> float:
        """Calculate technical score for strategy signals."""
        if not signals:
            return 0.0

        # Calculate score based on signal type and count
        buy_signals = len([s for s in signals if s[0] == "BUY"])
        sell_signals = len([s for s in signals if s[0] == "SELL"])

        if buy_signals > sell_signals:
            return min(1.0, buy_signals / (buy_signals + sell_signals) * 0.8 + 0.2)
        elif sell_signals > buy_signals:
            return max(-1.0, -(sell_signals / (buy_signals + sell_signals) * 0.8 + 0.2))
        else:
            return 0.0


class UnifiedSignalGenerator:
    """Unified signal generation combining news sentiment and technical strategies."""

    def __init__(self, config: Optional[UnifiedSignalConfig] = None):
        self.config = config or UnifiedSignalConfig()
        self.signal_cache_file = '.unified_signal_cache.json'
        self.last_signal_times: Dict[str, datetime] = {}

        # Initialize components
        self.news_analyzer = NewsSentimentAnalyzer(self.config.symbols)
        self.strategy_generator = StrategySignalGenerator()

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
            logger.error(f"Failed to load cache: {e}")
            self.cached_signals = {"signals": []}

    def _save_cache(self):
        """Save signal cache."""
        try:
            with open(self.signal_cache_file, 'w') as f:
                json.dump(self.cached_signals, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")

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
            logger.error(f"Price fetch failed for {symbol}: {e}")
            # Return hardcoded fallback prices
            price_map = {
                'BTC-USD': 68500.0, 'BTC-ETH': 2.5, 'BTC-SOL': 175.0,
                'BTC-DOGE': 0.15, 'BTC-XRP': 0.5, 'BTC-ADA': 0.35,
                'BTC-DOT': 7.5, 'BTC-MATIC': 0.9, 'BTC-SHIB': 0.000015,
                'BTC-AVAX': 42.0, 'BTC-UNI': 35.0, 'BTC-SNX': 3.5,
                'BTC-YFI': 12000.0, 'BTC-AAVE': 85.0, 'BTC-MKR': 1500.0,
                'BTC-COMP': 45.0, 'BTC-LINK': 6.5, 'BTC-BAT': 0.25,
                'BTC-ZRX': 0.4
            }
            return (price_map.get(symbol, 100.0), 0.0)

    def generate_signals(self) -> List[UnifiedTradingSignal]:
        """Generate unified trading signals from all sources."""

        print(f"Generating unified signals for {len(self.config.symbols)} symbols...")
        new_signals = []

        # Fetch rich sentiment once for all symbols
        rich_sentiment = self.news_analyzer.analyze_full()
        legacy_sentiment = {s: (d["avg_sentiment"], d["count"]) for s, d in rich_sentiment.items()}

        for symbol in self.config.symbols:
            sym_base = symbol.replace("-USD", "")
            avg_sentiment, news_count = legacy_sentiment.get(sym_base, (0.0, 0))
            rich_info = rich_sentiment.get(sym_base, {})

            # Get price data
            current_price, price_change_pct = self._get_price_data(symbol)

            print(f"  {symbol}: sentiment={avg_sentiment:.2f}, "
                  f"news_articles={news_count}, price_change={price_change_pct}%")

            # Generate strategy-based signals
            strategy_signals = []
            if self.config.enable_strategy_signals:
                try:
                    synthetic_data = self._generate_synthetic_data(symbol, 100)
                    strategy_signals = self.strategy_generator.generate_strategy_signals(symbol, synthetic_data)
                except Exception as e:
                    logger.error(f"Strategy signal generation failed for {symbol}: {e}")

            # Generate news-based signals from rich analysis
            news_signals = []
            if self.config.enable_news_signals:
                base_signal = self._analyze_news_sentiment_for_signal(
                    avg_sentiment, price_change_pct, news_count, symbol
                )
                if base_signal:
                    # Boost confidence from breaking news
                    breaking = rich_info.get("breaking_ratio", 0)
                    if breaking > 0.3:
                        base_signal.confidence = min(base_signal.confidence * 1.3, 0.95)
                        base_signal.signal_reason += " [BREAKING]"

                    news_signals.append(base_signal)

                # Topic-specific signals
                topics = rich_info.get("topics", [])
                if "hacks_security" in topics:
                    news_signals.append(UnifiedTradingSignal(
                        symbol=symbol, direction="SHORT",
                        confidence=min(0.3 + rich_info.get("hack_count", 0) * 0.05, 0.7),
                        sentiment_score=-0.3, technical_score=0.0,
                        price_change_pct=price_change_pct,
                        news_count=rich_info.get("hack_count", 0),
                        signal_reason=f"Security incident articles ({rich_info.get('hack_count', 0)})",
                        strategy_name="NewsHackAlert",
                        timestamp=datetime.now(),
                    ))

                if "regulation" in topics:
                    news_signals.append(UnifiedTradingSignal(
                        symbol=symbol, direction="SHORT",
                        confidence=min(0.2 + rich_info.get("regulation_count", 0) * 0.04, 0.65),
                        sentiment_score=-0.2, technical_score=0.0,
                        price_change_pct=price_change_pct,
                        news_count=rich_info.get("regulation_count", 0),
                        signal_reason=f"Regulatory coverage ({rich_info.get('regulation_count', 0)} articles)",
                        strategy_name="NewsRegulationWatch",
                        timestamp=datetime.now(),
                    ))

                if "adoption" in topics or "partnerships" in topics:
                    adopt_count = rich_info.get("adoption_count", 0)
                    news_signals.append(UnifiedTradingSignal(
                        symbol=symbol, direction="LONG",
                        confidence=min(0.2 + adopt_count * 0.05, 0.6),
                        sentiment_score=0.2, technical_score=0.0,
                        price_change_pct=price_change_pct,
                        news_count=adopt_count,
                        signal_reason=f"Adoption/partnership news ({adopt_count} articles)",
                        strategy_name="NewsAdoptionSignal",
                        timestamp=datetime.now(),
                    ))

                if "technology" in topics:
                    tech_count = rich_info.get("technology_count", 0)
                    news_signals.append(UnifiedTradingSignal(
                        symbol=symbol, direction="LONG",
                        confidence=min(0.15 + tech_count * 0.03, 0.5),
                        sentiment_score=0.15, technical_score=0.0,
                        price_change_pct=price_change_pct,
                        news_count=tech_count,
                        signal_reason=f"Technology upgrade news ({tech_count} articles)",
                        strategy_name="NewsTechSignal",
                        timestamp=datetime.now(),
                    ))

            # Combine signals
            all_signals = strategy_signals + news_signals

            # Filter by cooldown and add to results
            for signal in all_signals:
                if self._should_generate_signal(signal):
                    new_signals.append(signal)
                    self.last_signal_times[signal.symbol] = signal.timestamp

        # Save new signals
        for sig in new_signals:
            self.cached_signals['signals'].append(sig.to_dict())

        self._save_cache()

        return new_signals

    def _generate_synthetic_data(self, symbol: str, num_bars: int) -> List[Dict]:
        """Generate synthetic OHLCV data for strategy testing."""
        import random

        data = []
        base_price = 50000 if symbol == "BTC-USD" else 100 if symbol == "BTC-ETH" else 200

        for i in range(num_bars):
            volatility = random.uniform(0.005, 0.02)
            change = random.uniform(-volatility, volatility)

            current_price = base_price * (1 + change)

            high = current_price * (1 + random.uniform(0, 0.01))
            low = current_price * (1 - random.uniform(0, 0.01))
            open_price = current_price * (1 + random.uniform(-0.005, 0.005))
            volume = random.uniform(100, 1000)

            high = max(high, open_price, current_price)
            low = min(low, open_price, current_price)

            timestamp = int((datetime.now(timezone.utc).timestamp() - (num_bars - i) * 3600))

            data.append({
                'time': datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
                'ts': timestamp,
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(current_price, 2),
                'volume': round(volume, 2)
            })

        return data

    def _analyze_news_sentiment_for_signal(
        self,
        sentiment_score: float,
        price_change: float,
        news_count: int,
        symbol: str
    ) -> Optional[UnifiedTradingSignal]:
        """Analyze whether to generate a signal based on sentiment and price movement."""

        # Require some news coverage for confidence
        if news_count < 2:
            return None

        # Calculate composite signal score
        sentiment_contribution = abs(sentiment_score) * 0.5
        price_momentum_contribution = min(abs(price_change) / 10, 0.1)

        if sentiment_score >= self.config.sentiment_threshold:
            direction = 'LONG'
            base_confidence = 0.2 + sentiment_contribution + price_momentum_contribution

            # Boost confidence for strong alignment
            if price_change > 2:
                base_confidence += 0.15

            signal_reason = (
                f"Positive news sentiment ({sentiment_score:.2f}) with "
                f"{abs(price_change):.1f}% price change over 24h"
            )

        elif sentiment_score <= -self.config.sentiment_threshold:
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

        return UnifiedTradingSignal(
            symbol=symbol,
            direction=direction,
            confidence=min(round(base_confidence, 3), 0.95),
            sentiment_score=sentiment_score,
            technical_score=0.0,  # No technical for news signals
            price_change_pct=price_change,
            news_count=news_count,
            signal_reason=signal_reason,
            strategy_name="NewsSentimentAnalyzer",
            timestamp=datetime.now()
        )

    def _should_generate_signal(self, signal: UnifiedTradingSignal) -> bool:
        """Check if a signal should be generated based on cooldown and thresholds."""

        # Check cooldown
        last_time = self.last_signal_times.get(signal.symbol)
        if last_time:
            time_since = (datetime.now() - last_time).total_seconds()
            if time_since < self.config.cooldown_minutes * 60:
                return False

        # Check confidence threshold
        if signal.confidence < 0.3:
            return False

        # Check sentiment threshold for news signals
        if signal.strategy_name == "NewsSentimentAnalyzer" and abs(signal.sentiment_score) < self.config.sentiment_threshold:
            return False

        return True

    def get_strategy_signals(self) -> Dict[str, Any]:
        """Get information about all available strategies."""
        return {
            "available_strategies": list(self.strategy_generator.strategies.keys()),
            "supported_symbols": self.config.symbols,
            "strategy_details": {
                name: {
                    "class": strategy.__class__.__name__,
                    "description": self._get_strategy_description(name)
                }
                for name, strategy in self.strategy_generator.strategies.items()
            }
        }

    def _get_strategy_description(self, strategy_name: str) -> str:
        """Get description for a strategy."""
        descriptions = {
            "BTCVolatilityStacking": "BTC volatility stacking with position sizing and tax loss harvesting",
            "BTCVolatilityBreakout": "Volatility breakout trading with dynamic stops",
            "BTCVolatilityMeanReversion": "Statistical arbitrage with mean reversion",
            "BTCVolatilityMomentum": "Volatility and momentum combination",
            "CoinbaseMomentum": "RSI-based momentum with adaptive timeframes",
            "CoinbaseMeanReversion": "Bollinger Band mean reversion with volatility breakout",
            "VolatilityBreakout": "ATR-based volatility breakout with squeeze detection",
            "RegimeAwareAdaptive": "Machine learning-inspired adaptive parameter tuning",
            "VolumeProfile": "Volume-at-price profiling — HVN support/resistance, LVN breakout targets",
            "MultiTFConfluence": "Multi-timeframe rate-of-change alignment across short/medium/long horizons",
            "OrderFlowPressure": "Candle microstructure analysis — wick/body ratios inferring directional pressure",
            "VolContractionExpansion": "Volatility contraction-expansion cycle — trades breakouts from consolidation",
            "ZScorePairArb": "Statistical arbitrage via z-score of BTC-XXX pair ratio mean reversion",
            "LiquidationHeatmap": "Stop-loss run detection via acceleration, volume spikes, and wick analysis",
        }
        return descriptions.get(strategy_name, "Unknown strategy")


def main():
    """Run unified signal generation."""

    config = UnifiedSignalConfig(
        symbols=[
            "BTC-USD", "BTC-ETH", "BTC-SOL", "BTC-DOGE", "BTC-XRP",
            "BTC-ADA", "BTC-DOT", "BTC-MATIC", "BTC-SHIB", "BTC-AVAX",
            "BTC-UNI", "BTC-SNX", "BTC-YFI", "BTC-AAVE", "BTC-MKR",
            "BTC-COMP", "BTC-LINK", "BTC-BAT", "BTC-ZRX"
        ],
        sentiment_threshold=0.25,
        cooldown_minutes=15,
        enable_strategy_signals=True,
        enable_news_signals=True
    )

    generator = UnifiedSignalGenerator(config)
    signals = generator.generate_signals()

    print(f"\nGenerated {len(signals)} unified signals:")
    for sig in signals:
        print(json.dumps(sig.to_dict(), indent=2))

    # Print strategy information
    print(f"\n{'='*70}")
    print("AVAILABLE STRATEGIES:")
    print(f"{'='*70}")
    strategy_info = generator.get_strategy_signals()
    print(f"Total Strategies: {len(strategy_info['available_strategies'])}")
    print(f"Supported Symbols: {len(strategy_info['supported_symbols'])}")
    print("\nStrategy Details:")
    for name, details in strategy_info['strategy_details'].items():
        print(f"  • {name}: {details['description']}")

    return signals


if __name__ == "__main__":
    main()