"""
RSITrendFollowingStrategy - Trend Following

Purpose: Uses RSI for trend identification and momentum-based entries.

Regime Suitability:
  ✅ Strong trending markets with clear overbought/oversold conditions
  ❌ Choppy/ranging markets with frequent false signals

Failure Modes:
  • False signals in choppy markets
  • Whipsaws during trend reversals
  • Late entries due to RSI lag

Expected Performance:
  • Win rate target: 47-52%
  • Profit factor target: 1.3-1.7
  • Maximum historical drawdown: 20-28%
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class RSITrendFollowingConfig:
    """Configuration for RSI Trend Following Strategy."""
    period: int = 14      # Period for RSI calculation
    overbought_threshold: float = 70.0  # Overbought level
    oversold_threshold: float = 30.0   # Oversold level
    
    # Trend filter parameters
    min_trend_strength: float = 0.25
    max_drawdown_bps: float = 280.0  # Maximum drawdown before exit (2.8%)
    
    # Position sizing
    risk_per_trade: float = 0.01
    min_position_size: float = 0.001
    max_position_size: float = 0.05
    
    # Entry/Exit thresholds
    entry_threshold: float = 0.03  # Minimum RSI divergence for entry (3%)
    exit_threshold: float = -0.02  # Maximum RSI divergence for short (-2%)
    stop_loss_bps: float = 70.0  # Stop loss as basis points (0.7%)
    take_profit_bps: float = 200.0  # Take profit as basis points (2.0%)
    trailing_take_profit_bps: float = 100.0  # Trailing stop after profit
    cooldown_bars: int = 5
    warmup_bars: int = 60  # Bars needed for RSI calculation


class RSITrendFollowingStrategy:
    """
    RSI Trend Following Strategy.
    
    Generates buy signals when RSI crosses above oversold level with upward trend.
    Generates sell signals on opposite conditions.
    """
    
    def __init__(self, config: Optional[RSITrendFollowingConfig] = None):
        """
        Initialize strategy with configuration.
        
        Args:
            config: Strategy configuration
        """
        self.config = config or RSITrendFollowingConfig()
        self.warmup_complete = False
        self.current_position = None  # 'long' or 'short'
        self.entry_price = 0.0
        self.stop_loss_price = 0.0
        self.take_profit_price = 0.0
    
    def on_bar(self, market_state: dict) -> Optional[dict]:
        """
        Generate signal on new bar.
        
        Args:
            market_state: Dictionary containing OHLCV data and indicators
            
        Returns:
            Signal dictionary with action, quantity, stop_loss, take_profit, or None if no signal
        """
        # Check warmup period
        if not self.warmup_complete:
            bars_since_start = market_state.get('bars_since_start', 0)
            if bars_since_start >= self.config.warmup_bars:
                self.warmup_complete = True
            return None
        
        # Extract price data
        close_price = float(market_state.get('close', 0))
        
        # Get RSI values (assumed to be pre-calculated in market_state)
        rsi_value = float(market_state.get('rsi', 0))
        rsi_trend = float(market_state.get('rsi_trend', 0))  # 1 for up, -1 for down
        
        # Calculate RSI divergence from price trend
        rsi_divergence = abs(rsi_value - rsi_trend) / rsi_trend if rsi_trend != 0 else 0
        
        # Check if we're in a valid trending regime
        if rsi_divergence < self.config.min_trend_strength:
            return None
        
        # Determine current position state
        is_long_position = self.current_position == 'long'
        is_short_position = self.current_position == 'short'
        
        # Calculate target prices based on current position
        if is_long_position:
            target_price = close_price + (close_price * self.config.take_profit_bps / 10000)
            trailing_stop = max(self.stop_loss_price, close_price - (close_price * self.config.trailing_take_profit_bps / 10000))
        else:
            target_price = close_price - (close_price * self.config.take_profit_bps / 10000)
            trailing_stop = min(self.stop_loss_price, close_price + (close_price * self.config.trailing_take_profit_bps / 10000))
        
        # Check for exit signals
        if is_long_position:
            # Exit on stop loss or take profit
            if close_price <= self.stop_loss_price:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': None,
                    'take_profit': target_price,
                    'reason': 'stop_loss'
                }
            elif close_price >= target_price:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'take_profit'
                }
            # Exit on trend reversal (RSI crosses below overbought or enters oversold)
            elif rsi_value < self.config.oversold_threshold and rsi_divergence < self.config.exit_threshold:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'trend_reversal'
                }
        
        elif is_short_position:
            # Exit on stop loss or take profit
            if close_price >= self.stop_loss_price:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': None,
                    'take_profit': target_price,
                    'reason': 'stop_loss'
                }
            elif close_price <= target_price:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'take_profit'
                }
            # Exit on trend reversal (RSI crosses above oversold or enters overbought)
            elif rsi_value > self.config.overbought_threshold and rsi_divergence > self.config.entry_threshold:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'trend_reversal'
                }
        
        # No position - check for entry signals
        if rsi_divergence > self.config.entry_threshold and rsi_value < self.config.oversold_threshold:
            return {
                'action': 'open',
                'quantity': self.config.risk_per_trade / close_price,
                'stop_loss': close_price * (1 - self.config.stop_loss_bps / 10000),
                'take_profit': close_price * (1 + self.config.take_profit_bps / 10000),
                'reason': 'rsi_oversold_trend'
            }
        elif rsi_divergence > self.config.entry_threshold and rsi_value > self.config.overbought_threshold:
            return {
                'action': 'open',
                'quantity': -self.config.risk_per_trade / close_price,
                'stop_loss': close_price * (1 + self.config.stop_loss_bps / 10000),
                'take_profit': close_price * (1 - self.config.take_profit_bps / 10000),
                'reason': 'rsi_overbought_trend'
            }
        
        return None
    
    def metadata(self) -> dict:
        """
        Return strategy metadata for catalog and documentation.
        """
        return {
            'strategy_id': 'rsi_trend_following',
            'name': 'RSI Trend Following',
            'family': 'Trend Following',
            'purpose': 'Trend identification and momentum-based entries using RSI indicator',
            'regime_suitability': ['Strong trending markets with clear overbought/oversold conditions'],
            'supported_products': ['BTC-USD', 'ETH-USD', 'SOL-USD'],
            'risk_tier': 'TIER_2_MODERATE_RISK',
            'required_data': ['close', 'rsi', 'rsi_trend'],
            'required_indicators': ['RSI (period, overbought_threshold, oversold_threshold)'],
            'warmup_bars': self.config.warmup_bars,
            'required_latency_budget_ms': 10.0,
            'sizing_model': 'fixed_fraction',
            'risk_ceilings': 'TIER_2_MODERATE_RISK',
            'min_size': self.config.min_position_size,
            'max_size': self.config.max_position_size,
            'max_capital_fraction': 0.10,
            'max_exposure_by_asset': 0.20,
            'expected_holding_horizon': 'medium_term',
            'execution_style': 'trend_following',
            'take_profit_model': 'fixed_bps',
            'trailing_exit': True,
            'compound_profits': False,
            'min_net_edge_bps': 3.0,
            'approvals_required': False,
            'failure_modes': ['False signals in choppy markets'],
            'disable_criteria': ['Weak trend strength', 'Maximum drawdown reached'],
            'cooldown_logic': 'fixed_bar_count',
            'explainability_output': 'RSI position relative to overbought/oversold levels and trend direction',
            'live_prerequisites': ['Trend filter active', 'Adequate volatility'],
            'downgrade_conditions': ['Extended ranging period', 'Low volume regime']
        }
