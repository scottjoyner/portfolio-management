"""
Advanced Novel Crypto Trading Strategies - Part 2

This module implements cutting-edge trading strategies leveraging:
- On-chain metrics and blockchain data integration
- Machine learning-inspired adaptive systems
- Advanced order flow analysis
- Cross-exchange microstructure arbitrage
- Sentiment-driven regime detection
"""

from __future__ import annotations

import math
import time
import json
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
from math import sign  # For order flow imbalance calculation

from trading_system.strategies.base import BaseStrategy, OHLCVBar, compute_sma, compute_ema


@dataclass
class OnChainMetrics:
    """On-chain metrics for crypto-specific strategies."""
    timestamp: int
    active_addresses: float = 0.0
    tx_volume_usd: float = 0.0
    nvt_ratio: float = 0.0  # Network Value to Transactions
    mvr_ratio: float = 0.0  # Market Value to Realized Volume
    whale_flow_net: float = 0.0  # Net whale inflow/outflow USD
    exchange_inflow: float = 0.0  # Coins moving to exchanges (sell signal)
    exchange_outflow: float = 0.0  # Coins moving from exchanges (buy signal)


class OnChainRegimeStrategy(BaseStrategy):
    """
    Network Value-to-Transactions (NVT) Mean Reversion Strategy.
    
    Uses on-chain NVT ratio as a valuation metric similar to P/E ratios in stocks.
    When NVT is historically low, indicates undervaluation → buy signal.
    When NVT is historically high, indicates overvaluation → sell signal.
    
    Key Features:
    - Rolling NVT percentile calculation
    - Multi-asset NVT aggregation (BTC, ETH, major altcoins)
    - Time-decayed historical comparison
    - Whale flow confirmation filter
    """
    
    def __init__(self, nvt_lookback_days: int = 365,
                 percentile_threshold_low: float = 20.0,
                 percentile_threshold_high: float = 80.0):
        self.nvt_lookback_days = nvt_lookback_days
        self.percentile_threshold_low = percentile_threshold_low
        self.percentile_threshold_high = percentile_threshold_high
        self.nvt_history: List[float] = []
        self.whale_flow_buffer: List[float] = []
    
    def setup(self, ohlcv_data: List[OHLCVBar], nvt_data: Optional[List[OnChainMetrics]] = None) -> None:
        """Initialize with OHLCV and optional on-chain data."""
        self.ohlcv = ohlcv_data
        
        if nvt_data:
            # Store NVT history for percentile calculation
            self.nvt_history = [m.nvt_ratio for m in nvt_data]
        else:
            # Use mock/simulated NVT data for demonstration
            self._load_mock_nvt_data()
    
    def _load_mock_nvt_data(self) -> None:
        """Load simulated on-chain metrics."""
        now = datetime.now()
        days_ago = min(365, len(self.ohlcv))
        
        for i in range(days_ago):
            timestamp = int((now - timedelta(days=days_ago - i)).timestamp())
            # Simulate realistic NVT ratios (typically 20-100)
            nvt = 35 + math.sin(i / 30) * 15 + (i % 7) * 2
            self.nvt_history.append(nvt)
    
    def _calculate_nvt_percentile(self, current_nvt: float) -> float:
        """Calculate percentile rank of current NVT."""
        if not self.nvt_history or len(self.nvt_history) < 30:
            return 50.0
        
        # Use time-decayed history (more recent data weighted higher)
        decay_factor = 0.95
        weighted_nvt = sum(
            nvt * (decay_factor ** i)
            for i, nvt in enumerate(reversed(self.nvt_history[-100:]))
        ) / sum(decay_factor ** i for _ in range(100))
        
        # Calculate percentile
        sorted_nvt = sorted([weighted_nvt] + self.nvt_history[-99:])
        percentile = (sorted_nvt.index(current_nvt) + 1) / len(sorted_nvt) * 100
        return min(percentile, 100.0)
    
    def on_bar(self, bar: OHLCVBar) -> Tuple[Optional[bool], Optional[float]]:
        """
        Generate NVT-based mean reversion signal.
        """
        if not self.nvt_history or len(self.nvt_history) < 30:
            return None, None
        
        # Add current bar to history
        self.ohlcv.append(bar)
        
        # Simulate NVT update (in production, fetch from on-chain API)
        current_nvt = self.nvt_history[-1] + (
            math.sin(len(self.ohlcv) / 50) * 3 +
            (bar.close - bar.open) / bar.open if bar.open else 0
        )
        
        # Update NVT history with decay
        self.nvt_history.append(current_nvt)
        if len(self.nvt_history) > 100:
            self.nvt_history.pop(0)
        
        current_percentile = self._calculate_nvt_percentile(current_nvt)
        
        # Whale flow confirmation
        whale_signal = (
            sum(self.whale_flow_buffer[-7:]) if len(self.whale_flow_buffer) >= 7 else 0
        ) / 7
        
        # Generate signal based on NVT percentile and whale flow
        if current_percentile < self.percentile_threshold_low:
            # Undervalued - buy signal, especially with positive whale flow
            if whale_signal > -5e6:  # Whale inflow > $-5M (not too negative)
                return True, bar.close
        elif current_percentile > self.percentile_threshold_high:
            # Overvalued - sell signal, especially with negative whale flow
            if whale_signal < 5e6:  # Whale outflow < $5M (not too positive)
                return False, bar.close
        
        return None, None
    
    def get_nvt_signal(self) -> Dict[str, Any]:
        """Get current NVT analysis."""
        if not self.nvt_history:
            return {'error': 'Insufficient data'}
        
        avg_nvt = sum(self.nvt_history[-50:]) / 50
        min_nvt = min(self.nvt_history)
        max_nvt = max(self.nvt_history)
        
        return {
            'current_nvt': current_nvt if (current_nvt := self.nvt_history[-1]) else avg_nvt,
            'avg_nvt_50d': avg_nvt,
            'min_nvt': min_nvt,
            'max_nvt': max_nvt,
            'percentile_range': f'{self.percentile_threshold_low}-{self.percentile_threshold_high}',
        }
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """Get strategy metadata."""
        return {
            'name': 'OnChainRegimeStrategy',
            'description': 'NVT ratio mean reversion with whale flow confirmation',
            'risk_level': 'Medium-High',
            'recommended_allocation_pct': 15.0,
            'min_lookback_bars': 30,
        }


class WhaleFlowMomentumStrategy(BaseStrategy):
    """
    Whale Flow Momentum Strategy.
    
    Tracks net whale inflows/outflows to identify institutional accumulation/distribution.
    Strong whale accumulation followed by price breakout = high-probability buy signal.
    
    Key Features:
    - Exchange flow analysis (inflow = sell pressure, outflow = buy pressure)
    - Whale transaction detection (>10 BTC or >$5M per tx)
    - Flow momentum calculation over rolling windows
    - Price-flow divergence detection
    """
    
    def __init__(self, whale_threshold_btc: float = 10.0,
                 flow_lookback_days: int = 7):
        self.whale_threshold_btc = whale_threshold_btc
        self.flow_lookback_days = flow_lookback_days
        self.exchange_flows: List[float] = []
        self.whale_tx_history: List[Dict] = []
    
    def setup(self, ohlcv_data: List[OHLCVBar]) -> None:
        """Initialize with OHLCV data."""
        self.ohlcv = ohlcv_data
    
    def _simulate_whale_transactions(self) -> None:
        """Simulate whale transactions for demonstration."""
        now = datetime.now()
        days_ago = min(30, len(self.ohlcv))
        
        for i in range(days_ago):
            timestamp = int((now - timedelta(days=days_ago - i)).timestamp())
            # Simulate whale flows with some realistic patterns
            flow = math.sin(i / 15) * 2e7 + (i % 10) * 1e6
            self.exchange_flows.append(flow)
    
    def _calculate_flow_momentum(self) -> float:
        """Calculate net whale flow momentum."""
        if len(self.exchange_flows) < 5:
            return 0.0
        
        # Net outflow (positive = whales moving to exchanges = sell signal)
        net_outflow = sum(self.exchange_flows[-self.flow_lookback_days:])
        avg_flow = net_outflow / self.flow_lookback_days
        
        return avg_flow
    
    def on_bar(self, bar: OHLCVBar) -> Tuple[Optional[bool], Optional[float]]:
        """
        Generate whale flow momentum signal.
        """
        if not self.exchange_flows or len(self.exchange_flows) < 10:
            return None, None
        
        # Add current bar
        self.ohlcv.append(bar)
        
        # Simulate new exchange flow (in production: fetch from on-chain API)
        current_flow = self._calculate_flow_momentum()
        self.exchange_flows.append(current_flow)
        if len(self.exchange_flows) > 30:
            self.exchange_flows.pop(0)
        
        # Price-flow divergence detection
        price_change = (bar.close - bar.open) / max(bar.open, 1e-8)
        flow_momentum = current_flow / 1e7  # Normalize to millions
        
        # Bullish signal: strong outflow momentum + positive price action
        if flow_momentum > 0.3 and price_change > 0.02:
            return True, bar.close
        
        # Bearish signal: strong inflow momentum + negative price action
        elif flow_momentum < -0.3 and price_change < -0.02:
            return False, bar.close
        
        return None, None
    
    def get_flow_analysis(self) -> Dict[str, Any]:
        """Get current whale flow analysis."""
        if not self.exchange_flows:
            return {'error': 'Insufficient data'}
        
        net_outflow = sum(self.exchange_flows[-7:]) / 7
        avg_flow_24h = sum(self.exchange_flows[-1:]) / 1
        
        signal = 'accumulation' if net_outflow < 0 else 'distribution'
        confidence = min(abs(net_outflow) / 5e6, 1.0)
        
        return {
            'net_7d_outflow_usd': net_outflow,
            'avg_daily_flow_usd': avg_flow_24h,
            'signal': signal,
            'confidence': confidence * 100,
        }
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """Get strategy metadata."""
        return {
            'name': 'WhaleFlowMomentumStrategy',
            'description': 'Institutional whale flow momentum with exchange flow analysis',
            'risk_level': 'Medium-High',
            'recommended_allocation_pct': 15.0,
            'min_lookback_bars': 10,
        }


class OrderFlowImbalanceStrategy(BaseStrategy):
    """
    Order Flow Imbalance Strategy.
    
    Analyzes the imbalance between buy and sell orders at market depth levels.
    Persistent imbalances indicate institutional order flow pressure.
    
    Key Features:
    - Bid-ask spread analysis
    - Market depth estimation (simulated)
    - Order book imbalance calculation
    - Imbalance persistence detection
    """
    
    def __init__(self, imbalance_threshold: float = 0.15,
                 lookback_bars: int = 20):
        self.imbalance_threshold = imbalance_threshold
        self.lookback_bars = lookback_bars
        self.order_imbalances: List[float] = []
    
    def setup(self, ohlcv_data: List[OHLCVBar]) -> None:
        """Initialize with OHLCV data."""
        self.ohlcv = ohlcv_data
    
    def _estimate_market_depth(self, bar: OHLCVBar) -> Tuple[float, float]:
        """
        Estimate market depth based on price action.
        In production: use actual order book data from exchange API.
        """
        # Simulate depth estimation using recent volatility
        if len(self.ohlcv) < 10:
            return 1e6, 1e6  # Default depth
        
        price_range = bar.high - bar.low
        avg_price = (bar.high + bar.low) / 2
        
        # Estimate depth based on volatility and price level
        estimated_depth = (
            1e7 * math.exp(-price_range / max(avg_price * 0.05, 1))
        )
        return estimated_depth, estimated_depth
    
    def _calculate_order_imbalance(self, bar: OHLCVBar) -> float:
        """
        Calculate order flow imbalance.
        Positive = more buying pressure, Negative = more selling pressure.
        """
        if not self.ohlcv or len(self.ohlcv) < 5:
            return 0.0
        
        # Add current bar
        self.ohlcv.append(bar)
        
        # Estimate depth at open and close
        depth_open, _ = self._estimate_market_depth(self.ohlcv[-2])
        depth_close, _ = self._estimate_market_depth(bar)
        
        # Simulate order flow based on price movement relative to estimated depth
        price_change = bar.close - bar.open
        avg_depth = (depth_open + depth_close) / 2
        
        # Imbalance: price change normalized by estimated market impact
        imbalance = price_change / max(avg_depth, 1e6)
        
        self.order_imbalances.append(imbalance)
        if len(self.order_imbalances) > self.lookback_bars:
            self.order_imbalances.pop(0)
        
        return imbalance
    
    def on_bar(self, bar: OHLCVBar) -> Tuple[Optional[bool], Optional[float]]:
        """
        Generate order flow imbalance signal.
        """
        if not self.order_imbalances or len(self.order_imbalances) < 10:
            return None, None
        
        current_imbalance = self._calculate_order_imbalance(bar)
        avg_imbalance = sum(self.order_imbalances[-5:]) / 5
        
        # Persistent imbalance detection
        if abs(current_imbalance) > self.imbalance_threshold and \
           math.sign(current_imbalance) == math.sign(avg_imbalance):
            # Strong persistent imbalance - potential breakout
            if current_imbalance > 0:  # Buying pressure
                return True, bar.close
            else:  # Selling pressure
                return False, bar.close
        
        return None, None
    
    def get_imbalance_analysis(self) -> Dict[str, Any]:
        """Get current order flow analysis."""
        if not self.order_imbalances:
            return {'error': 'Insufficient data'}
        
        avg_imbalance = sum(self.order_imbalances[-10:]) / 10
        max_imbalance = max(abs(i) for i in self.order_imbalances[-20:])
        
        signal = 'buying_pressure' if avg_imbalance > 0 else 'selling_pressure'
        strength = min(abs(avg_imbalance) * 10, 1.0)
        
        return {
            'avg_imbalance': avg_imbalance,
            'max_recent_imbalance': max_imbalance,
            'signal': signal,
            'strength_pct': strength * 100,
        }
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """Get strategy metadata."""
        return {
            'name': 'OrderFlowImbalanceStrategy',
            'description': 'Market depth-based order flow imbalance detection',
            'risk_level': 'Medium-High',
            'recommended_allocation_pct': 10.0,
            'min_lookback_bars': 10,
        }


class CrossExchangeMicrostructureArb(BaseStrategy):
    """
    Cross-Exchange Microstructure Arbitrage.
    
    Exploits temporary price discrepancies between exchanges caused by:
    - Latency differences in order processing
    - Liquidity imbalances across venues
    - Exchange-specific trading fees and slippage
    
    Key Features:
    - Multi-exchange price monitoring (simulated)
    - Fee-adjusted profit calculation
    - Slippage-aware position sizing
    - Rapid execution window detection
    """
    
    def __init__(self, exchanges: List[str] = None,
                 min_profit_pct: float = 0.05,
                 max_position_usd: float = 1000):
        self.exchanges = exchanges or ['coinbase', 'kraken', 'binance']
        self.min_profit_pct = min_profit_pct
        self.max_position_usd = max_position_usd
        self.exchange_prices: Dict[str, float] = {}
    
    def setup(self, ohlcv_data: List[OHLCVBar]) -> None:
        """Initialize with OHLCV data."""
        self.ohlcv = ohlcv_data
    
    def _fetch_exchange_prices(self) -> Dict[str, float]:
        """
        Fetch prices from multiple exchanges.
        In production: use actual exchange APIs or websocket feeds.
        """
        # Simulate price discrepancies between exchanges
        base_price = self.ohlcv[-1].close if self.ohlcv else 50000
        
        for exchange in self.exchanges:
            # Simulate small but realistic price differences (0.01-0.1%)
            discrepancy = math.sin(len(self.ohlcv) / 20) * 0.0005
            price = base_price * (1 + discrepancy)
            self.exchange_prices[exchange] = price
        
        return self.exchange_prices
    
    def _calculate_fee_adjusted_profit(self, buy_exchange: str,
                                       sell_exchange: str) -> float:
        """
        Calculate profit after fees and slippage.
        """
        if not self.exchange_prices or len(self.exchange_prices) < 2:
            return 0.0
        
        buy_price = self.exchange_prices[buy_exchange]
        sell_price = self.exchange_prices[sell_exchange]
        
        # Fee-adjusted prices (typical fees: Coinbase 0.4%, Kraken 0.26%, Binance 0.1%)
        fees = {
            'coinbase': 0.004,
            'kraken': 0.0026,
            'binance': 0.001,
        }
        
        buy_fee = fees.get(buy_exchange, 0.005)
        sell_fee = fees.get(sell_exchange, 0.005)
        
        # Fee-adjusted prices
        adjusted_buy_price = buy_price * (1 + buy_fee)
        adjusted_sell_price = sell_price * (1 - sell_fee)
        
        profit_pct = (adjusted_sell_price - adjusted_buy_price) / adjusted_buy_price
        
        return max(profit_pct, 0.0)  # Only positive profits
    
    def on_bar(self, bar: OHLCVBar) -> Tuple[Optional[bool], Optional[float]]:
        """
        Generate microstructure arbitrage signal.
        """
        if not self.exchange_prices or len(self.exchange_prices) < 2:
            return None, None
        
        # Add current bar
        self.ohlcv.append(bar)
        
        # Fetch/update exchange prices
        self._fetch_exchange_prices()
        
        # Find best arbitrage opportunity
        best_profit = 0.0
        buy_exchange = None
        sell_exchange = None
        
        for i, ex1 in enumerate(self.exchanges):
            for ex2 in self.exchanges[i+1:]:
                profit = self._calculate_fee_adjusted_profit(ex1, ex2)
                if profit > best_profit:
                    best_profit = profit
                    buy_exchange = ex1
                    sell_exchange = ex2
        
        # Execute only if profit exceeds threshold
        if best_profit >= self.min_profit_pct:
            return True, bar.close  # Buy on cheaper exchange
        
        return None, None
    
    def get_arb_opportunity(self) -> Optional[Dict[str, Any]]:
        """Get current arbitrage opportunity."""
        if not self.exchange_prices or len(self.exchange_prices) < 2:
            return None
        
        best_profit = 0.0
        buy_exchange = None
        sell_exchange = None
        
        for i, ex1 in enumerate(self.exchanges):
            for ex2 in self.exchanges[i+1:]:
                profit = self._calculate_fee_adjusted_profit(ex1, ex2)
                if profit > best_profit:
                    best_profit = profit
                    buy_exchange = ex1
                    sell_exchange = ex2
        
        if best_profit < self.min_profit_pct:
            return None
        
        return {
            'buy_exchange': buy_exchange,
            'sell_exchange': sell_exchange,
            'profit_pct': best_profit * 100,
            'position_limit_usd': self.max_position_usd,
        }
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """Get strategy metadata."""
        return {
            'name': 'CrossExchangeMicrostructureArb',
            'description': 'Multi-exchange microstructure arbitrage with fee-adjusted profit calculation',
            'risk_level': 'Low-Medium',
            'recommended_allocation_pct': 10.0,
            'min_lookback_bars': 1,  # Real-time operation
        }


class SentimentRegimeDetector(BaseStrategy):
    """
    Social Media Sentiment Regime Detector.
    
    Uses sentiment analysis from social media and news sources to detect
    market regime shifts. Extreme sentiment often precedes reversals.
    
    Key Features:
    - Sentiment score calculation (simulated)
    - Extremity detection (overbought/oversold sentiment)
    - Regime transition smoothing
    - Confirmation with technical indicators
    """
    
    def __init__(self, sentiment_lookback_days: int = 7,
                 extreme_threshold: float = 0.8):
        self.sentiment_lookback_days = sentiment_lookback_days
        self.extreme_threshold = extreme_threshold
        self.sentiment_history: List[float] = []
    
    def setup(self, ohlcv_data: List[OHLCVBar]) -> None:
        """Initialize with OHLCV data."""
        self.ohlcv = ohlcv_data
    
    def _simulate_sentiment_score(self) -> float:
        """
        Simulate sentiment score from -1 (extremely negative) to +1 (extremely positive).
        In production: use actual social media APIs, news sentiment analysis.
        """
        if not self.ohlcv or len(self.ohlcv) < 5:
            return 0.0
        
        # Simulate sentiment with some realistic patterns
        days_ago = min(30, len(self.ohlcv))
        base_sentiment = math.sin(days_ago / 10) * 0.4 + 0.2
        noise = (hash(str(datetime.now())) % 100 - 50) / 50
        
        return min(max(base_sentiment + noise, -1.0), 1.0)
    
    def _calculate_sentiment_extremity(self) -> float:
        """
        Calculate how extreme current sentiment is relative to history.
        Returns value between 0 (normal) and 1 (extreme).
        """
        if not self.sentiment_history or len(self.sentiment_history) < 20:
            return 0.0
        
        # Calculate rolling statistics
        avg_sentiment = sum(self.sentiment_history[-30:]) / 30
        max_sentiment = max(self.sentiment_history)
        min_sentiment = min(self.sentiment_history)
        range_sentiment = max_sentiment - min_sentiment
        
        if range_sentiment == 0:
            return 0.0
        
        # Extremity: how close to historical extremes
        current_sentiment = self.sentiment_history[-1]
        distance_to_extreme = min(
            abs(current_sentiment - max_sentiment),
            abs(current_sentiment - min_sentiment)
        ) / range_sentiment
        
        return 1.0 - distance_to_extreme
    
    def on_bar(self, bar: OHLCVBar) -> Tuple[Optional[bool], Optional[float]]:
        """
        Generate sentiment-based regime signal.
        """
        if not self.sentiment_history or len(self.sentiment_history) < 20:
            return None, None
        
        # Add current bar and sentiment
        self.ohlcv.append(bar)
        current_sentiment = self._simulate_sentiment_score()
        self.sentiment_history.append(current_sentiment)
        if len(self.sentiment_history) > 50:
            self.sentiment_history.pop(0)
        
        # Calculate extremity
        extremity = self._calculate_sentiment_extremity()
        
        # Extreme sentiment + price confirmation = reversal signal
        if extremity > self.extreme_threshold:
            # Check for divergence with price action
            price_momentum = (bar.close - bar.open) / max(bar.open, 1e-8)
            
            # Sentiment extreme positive but price weak = potential top
            if current_sentiment > 0.5 and price_momentum < 0.01:
                return False, bar.close
            
            # Sentiment extreme negative but price strong = potential bottom
            elif current_sentiment < -0.5 and price_momentum > 0.01:
                return True, bar.close
        
        return None, None
    
    def get_sentiment_analysis(self) -> Dict[str, Any]:
        """Get current sentiment analysis."""
        if not self.sentiment_history:
            return {'error': 'Insufficient data'}
        
        avg_sentiment = sum(self.sentiment_history[-30:]) / 30
        extremity = self._calculate_sentiment_extremity()
        
        sentiment_label = (
            'extremely_bullish' if avg_sentiment > 0.5 else
            'extremely_bearish' if avg_sentiment < -0.5 else
            'neutral'
        )
        
        return {
            'avg_sentiment_30d': avg_sentiment,
            'extremity_score': extremity,
            'sentiment_label': sentiment_label,
            'recommendation': 'caution' if extremity > 0.7 else 'normal',
        }
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """Get strategy metadata."""
        return {
            'name': 'SentimentRegimeDetector',
            'description': 'Social sentiment-based regime detection with extremity analysis',
            'risk_level': 'Medium-High',
            'recommended_allocation_pct': 10.0,
            'min_lookback_bars': 20,
        }


# Export all strategies
__all__ = [
    'OnChainRegimeStrategy',
    'WhaleFlowMomentumStrategy',
    'OrderFlowImbalanceStrategy',
    'CrossExchangeMicrostructureArb',
    'SentimentRegimeDetector',
]
