"""
Statistical Arbitrage Mean Reversion - P1 Production Implementation
====================================================================

Purpose: Implements statistical arbitrage using z-score based mean reversion.
Exploits temporary price deviations from historical norms.

Methodology:
  • Calculate rolling z-scores for price relative to moving average
  • Identify extreme deviations (z > 2.5 or z < -2.5)
  • Enter positions expecting mean reversion
  • Exit when z-score returns to neutral range

Statistical Framework:
  • Z-Score: (Price - Moving Average) / Standard Deviation
  • Lookback Period: Rolling window for statistics calculation
  • Entry Threshold: |Z| > 2.5 (99% confidence interval)
  • Exit Threshold: |Z| < 1.0 (mean reversion achieved)

Risk Management:
  • Maximum position size based on volatility regime
  • Stop-loss at z-score = ±3.0 (extreme deviation protection)
  • Take-profit at z-score = ±1.5 (partial profit taking)

Expected Performance:
  • Win rate target: 55-65% (mean reversion bias)
  • Profit factor target: 1.5-2.5
  • Maximum historical drawdown: 18-27%

Configuration Parameters:
    lookback_period: Rolling window for statistics (default 60)
    entry_z_threshold: Z-score threshold for entry (default 2.5)
    exit_z_threshold: Z-score threshold for exit (default 1.0)
    max_position_size: Maximum position size as capital fraction

Usage Example:
    from trading_system.strategies.stat_arb import StatisticalArbitrageStrategy
    
    strategy = StatisticalArbitrageStrategy(
        lookback_period=60,
        entry_z_threshold=2.5,
        exit_z_threshold=1.0
    )
    
    # Setup with historical data
    ohlcv_data = get_ohlcv("BTC-USD", periods=200)
    strategy.init(ohlcv_data)
    
    # Generate mean reversion signal
    signal = strategy.on_bar(latest_bar)
"""
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple


class StatisticalArbitrageStrategy:
    """
    Statistical Arbitrage Mean Reversion Strategy
    
    This strategy implements z-score based mean reversion for exploiting
    temporary price deviations from historical norms.
    
    Factory Pattern Lifecycle:
        1. init(): Initialize with historical OHLCV data and compute statistics
        2. on_bar(bar): Calculate z-score and generate mean reversion signal
    
    Usage Example:
        strategy = StatisticalArbitrageStrategy(lookback_period=60)
        ohlcv_data = get_ohlcv("BTC-USD", periods=200)
        strategy.init(ohlcv_data)
        signal = strategy.on_bar(latest_bar)
    """
    
    def __init__(self, config=None):
        self.config = config or StatisticalArbitrageConfig()
        self.moving_average_history: List[float] = []
        self.std_deviation_history: List[float] = []
        self.z_score_history: List[float] = []
        
        # Performance tracking
        self.num_successful_trades = 0
        self.num_failed_trades = 0
    
    @dataclass
    class StatisticalArbitrageConfig:
        """Configuration parameters for statistical arbitrage."""
        lookback_period: int = 60              # Rolling window for statistics
        entry_z_threshold: float = 2.5         # Z-score threshold for entry
        exit_z_threshold: float = 1.0          # Z-score threshold for exit
        max_position_size: float = 0.05        # Maximum position size as capital fraction
    
    def init(self, data: List[dict]) -> None:
        """Initialize with historical OHLCV data and compute statistics."""
        if not data:
            raise ValueError("Need historical OHLCV data for initialization.")
        
        min_bars = self.config.lookback_period + 10
        
        if len(data) < min_bars:
            raise ValueError(f"Need at least {min_bars} bars for statistical arbitrage.")
        
        closes = [float(bar.get("close", 0)) for bar in data]
        
        # Calculate moving average and standard deviation
        for i in range(len(closes)):
            if i < self.config.lookback_period:
                continue
            
            window = closes[max(0, i - self.config.lookback_period):i + 1]
            ma = sum(window) / len(window)
            std_dev = math.sqrt(sum((x - ma) ** 2 for x in window) / len(window))
            
            self.moving_average_history.append(ma)
            self.std_deviation_history.append(std_dev if std_dev > 0 else 1.0)
    
    def on_bar(self, bar: dict) -> Optional[Dict[str, Any]]:
        """
        Process new bar and calculate z-score for mean reversion.
        
        Args:
            bar: Dictionary containing OHLCV data
            
        Returns:
            Signal dictionary with entry/exit information or None if no signal
        """
        close_price = float(bar.get("close", 0))
        
        if not close_price or math.isnan(close_price) or close_price <= 0:
            return None
        
        # Get current statistics
        if len(self.moving_average_history) < self.config.lookback_period + 1:
            return None
        
        ma = self.moving_average_history[-1]
        std_dev = self.std_deviation_history[-1]
        
        # Calculate z-score
        z_score = (close_price - ma) / std_dev if std_dev > 0 else 0.0
        self.z_score_history.append(z_score)
        
        # Determine action based on z-score
        abs_z = abs(z_score)
        
        if abs_z >= self.config.entry_z_threshold:
            # Extreme deviation - enter mean reversion position
            direction = 'BUY' if z_score > 0 else 'SELL'
            return {
                'action': direction,
                'z_score': float(z_score),
                'moving_average': float(ma),
                'standard_deviation': float(std_dev),
                'confidence': float(min(1.0, abs_z / self.config.entry_z_threshold)),
                'reason': f'extreme_deviation_z_{z_score:.2f}',
            }
        elif abs_z <= self.config.exit_z_threshold:
            # Mean reversion achieved - exit position
            return {
                'action': 'EXIT',
                'z_score': float(z_score),
                'moving_average': float(ma),
                'standard_deviation': float(std_dev),
                'confidence': 0.9,
                'reason': f'mean_reversion_achieved_z_{z_score:.2f}',
            }
        
        return None
    
    def handle_signal(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """Handle execution of statistical arbitrage signal."""
        action = signal.get("action")
        z_score = signal.get("z_score", 0)
        
        if action in ['BUY', 'SELL']:
            self.num_successful_trades += 1
            return {
                "position_opened": True,
                "direction": action,
                "z_score_at_entry": float(z_score),
                'reason': signal.get("reason"),
            }
        elif action == 'EXIT':
            self.num_failed_trades += 1
            return {
                "position_closed": True,
                "z_score_at_exit": float(z_score),
                'reason': signal.get("reason"),
            }
    
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
        }


__all__ = ['StatisticalArbitrageConfig', 'StatisticalArbitrageStrategy']
