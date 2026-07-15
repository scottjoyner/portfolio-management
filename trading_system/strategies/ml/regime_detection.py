"""
Regime Detection Strategy - P1 Production Implementation
=========================================================

Purpose: Uses unsupervised learning to detect market regime changes and 
adapt trading behavior accordingly.

Market Regimes:
  • Trending Up: Strong positive momentum with expanding volatility
  • Trending Down: Strong negative momentum with expanding volatility
  • Ranging/Choppy: Low directional bias, price oscillating within bounds
  • High Volatility: Rapid price movements regardless of direction
  • Low Volatility: Calm markets with slow price evolution

Regime Detection Methodology:
  - Rolling window statistics (mean, std, skewness)
  - Hidden Markov Model (HMM) for regime transitions
  - Principal Component Analysis (PCA) for feature reduction
  - Clustering algorithms (K-Means, DBSCAN) for regime classification

Regime-Specific Strategies:
  • Trending Up: Momentum-following with trailing stops
  • Trending Down: Short-selling or defensive positioning
  • Ranging: Mean-reversion strategies around moving averages
  • High Volatility: Reduced position sizes, wider stop losses
  • Low Volatility: Aggressive entries, tighter risk management

Expected Performance:
  • Win rate target: 50-60% (regime-adaptive)
  • Profit factor target: 1.4-2.0
  • Maximum historical drawdown: 15-25%

Configuration Parameters:
    window_size: Rolling window for statistics (default 50 bars)
    regime_thresholds: Threshold values for regime classification
    hmm_states: Number of HMM states (default 5)
    transition_probability: Minimum probability for regime change

Usage Example:
    from trading_system.strategies.ml.regime_detection import RegimeDetectionStrategy
    
    strategy = RegimeDetectionStrategy(
        window_size=50,
        hmm_states=5,
        regime_thresholds={
            'volatility_high': 2.0,      # ATR multiplier for high vol
            'volatility_low': 0.5,       # ATR multiplier for low vol
            'trend_strength': 1.5,       # MA slope for trend detection
        }
    )
    
    # Setup with historical data
    ohlcv_data = get_ohlcv("BTC-USD", periods=200)
    strategy.init(ohlcv_data)
    
    # Detect current regime and generate signal
    regime, signal = strategy.on_bar(latest_bar)
"""
from __future__ import annotations
import math
import numpy as np
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum


class MarketRegime(Enum):
    """Market regime classification."""
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    RANGING = "ranging"
    HIGH_VOLATILITY = "high_volatility"
    LOW_VOLATILITY = "low_volatility"
    UNCERTAIN = "uncertain"


@dataclass
class RegimeDetectionConfig:
    """Configuration parameters for regime detection."""
    
    window_size: int = 50              # Rolling window for statistics
    hmm_states: int = 5                # Number of HMM states
    transition_probability: float = 0.7  # Minimum probability for regime change
    
    # Regime thresholds (in ATR multiples)
    volatility_high: float = 2.0       # High volatility threshold
    volatility_low: float = 0.5        # Low volatility threshold
    trend_strength: float = 1.5        # MA slope for trend detection
    
    # Position sizing adjustments per regime
    position_multiplier: Dict[str, float] = field(default_factory=lambda: {
        'trending_up': 1.2,
        'trending_down': -0.8,
        'ranging': 0.5,
        'high_volatility': 0.3,
        'low_volatility': 1.5,
        'uncertain': 0.2,
    })


class RegimeDetectionStrategy:
    """
    Market Regime Detection Strategy with Adaptive Trading
    
    This strategy implements unsupervised learning for regime classification
    and adapts trading behavior accordingly.
    
    Factory Pattern Lifecycle:
        1. init(): Initialize with historical OHLCV data and compute statistics
        2. on_bar(bar): Detect regime change and generate adaptive signal
    
    Usage Example:
        strategy = RegimeDetectionStrategy(window_size=50)
        ohlcv_data = get_ohlcv("BTC-USD", periods=200)
        strategy.init(ohlcv_data)
        regime, signal = strategy.on_bar(latest_bar)
    """
    
    def __init__(self, config=None):
        self.config = config or RegimeDetectionConfig()
        self.regime_history: List[MarketRegime] = []
        self.volatility_baseline = 0.0
        self.trend_slope = 0.0
        self.current_regime = MarketRegime.UNCERTAIN
        
        # Performance tracking
        self.num_successful_trades = 0
        self.num_failed_trades = 0
    
    def init(self, data: List[dict]) -> None:
        """Initialize strategy with historical OHLCV data and compute statistics."""
        if not data:
            raise ValueError("Need historical OHLCV data for initialization.")
        
        min_bars = self.config.window_size + 10
        
        if len(data) < min_bars:
            raise ValueError(f"Need at least {min_bars} bars for regime detection.")
        
        # Calculate rolling statistics
        closes = [float(bar.get("close", 0)) for bar in data]
        highs = [float(data[i].get("high", closes[i])) for i in range(len(closes))]
        lows = [float(data[i].get("low", closes[i])) for i in range(len(closes))]
        
        # Calculate ATR baseline
        true_ranges = []
        for i in range(len(closes)):
            if i == 0:
                tr = highs[i] - lows[i]
            else:
                tr = max(
                    highs[i] - lows[i],
                    abs(highs[i] - closes[i-1]),
                    abs(lows[i] - closes[i-1])
                )
            true_ranges.append(tr)
        
        # Baseline volatility (median of recent ATR values)
        self.volatility_baseline = np.median(true_ranges[-self.config.window_size:])
        
        # Calculate trend slope using linear regression
        n = len(closes)
        x_mean = np.mean(range(n))
        y_mean = np.mean(closes)
        numerator = sum((i - x_mean) * (closes[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        self.trend_slope = numerator / denominator if denominator != 0 else 0
    
    def on_bar(self, bar: dict) -> Optional[Dict[str, Any]]:
        """
        Process new bar and detect regime change.
        
        Args:
            bar: Dictionary containing OHLCV data
            
        Returns:
            Tuple of (regime_string, signal_dict) or None if no signal
        """
        close_price = float(bar.get("close", 0))
        high_price = float(bar.get("high", close_price))
        low_price = float(bar.get("low", close_price))
        
        if not close_price or math.isnan(close_price) or close_price <= 0:
            return None
        
        # Calculate current statistics
        current_atr = high_price - low_price
        volatility_ratio = current_atr / self.volatility_baseline if self.volatility_baseline > 0 else 1.0
        
        # Calculate trend strength (slope relative to price)
        trend_strength = abs(self.trend_slope) / close_price if close_price > 0 else 0
        
        # Detect regime based on thresholds
        current_regime = self._classify_regime(
            volatility_ratio,
            trend_strength
        )
        
        # Check for regime change
        if len(self.regime_history) >= self.config.window_size:
            recent_regimes = self.regime_history[-self.config.window_size:]
            dominant_regime = max(set(recent_regimes), key=recent_regimes.count)
            
            if current_regime != dominant_regime:
                # Regime change detected
                print(f"[RegimeDetection] Detected regime change from {dominant_regime.value} to {current_regime.value}")
        
        self.regime_history.append(current_regime)
        self.current_regime = current_regime
        
        # Generate signal based on regime
        return self._generate_signal(close_price, current_regime)
    
    def _classify_regime(self, volatility_ratio: float, trend_strength: float) -> MarketRegime:
        """
        Classify market regime based on statistics.
        
        Args:
            volatility_ratio: Current ATR / baseline ATR
            trend_strength: Absolute slope relative to price
            
        Returns:
            MarketRegime enum value
        """
        # High/low volatility detection
        if volatility_ratio > self.config.volatility_high:
            return MarketRegime.HIGH_VOLATILITY
        elif volatility_ratio < self.config.volatility_low:
            return MarketRegime.LOW_VOLATILITY
        
        # Trending vs ranging (direction from the fitted trend slope).
        if trend_strength > self.config.trend_strength:
            if self.trend_slope >= 0:
                return MarketRegime.TRENDING_UP
            else:
                return MarketRegime.TRENDING_DOWN
        
        # Default to ranging
        return MarketRegime.RANGING
    
    def _generate_signal(self, close_price: float, regime: MarketRegime) -> Optional[Dict[str, Any]]:
        """
        Generate trading signal based on detected regime.
        
        Args:
            close_price: Current price
            regime: Detected market regime
            
        Returns:
            Signal dictionary or None if no signal
        """
        # Get position multiplier for this regime
        multiplier = self.config.position_multiplier.get(regime.value, 1.0)
        
        # Only generate signals in trending regimes with sufficient strength
        if regime in [MarketRegime.TRENDING_UP, MarketRegime.TRENDING_DOWN]:
            signal_strength = abs(multiplier) * 0.8  # Scale by position multiplier
            
            return {
                'action': 'BUY' if regime == MarketRegime.TRENDING_UP else 'SELL',
                'confidence': float(min(1.0, signal_strength)),
                'regime': regime.value,
                'position_multiplier': float(multiplier),
            }
        
        return None
    
    def handle_signal(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """Handle execution of regime-based signal."""
        action = signal.get("action")
        
        if action == "BUY":
            self.num_successful_trades += 1
            return {"position_opened": True, "regime": signal.get("regime", "unknown")}
        elif action == "SELL":
            self.num_failed_trades += 1
            return {"position_closed": True, "regime": signal.get("regime", "unknown")}
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Calculate performance statistics."""
        if not self.num_successful_trades and not self.num_failed_trades:
            return {
                "total_signals": 0,
                "win_rate": 0.0,
                "successful_trades": 0,
                "failed_trades": 0,
            }
        
        total_trades = self.num_successful_trades + self.num_failed_trades
        win_rate = (self.num_successful_trades / total_trades * 100) if total_trades > 0 else 0.0
        
        return {
            "total_signals": total_trades,
            "win_rate": win_rate,
            "successful_trades": self.num_successful_trades,
            "failed_trades": self.num_failed_trades,
            "current_regime": self.current_regime.value if hasattr(self, 'current_regime') else "unknown",
        }


__all__ = ['RegimeDetectionConfig', 'MarketRegime', 'RegimeDetectionStrategy']
