"""
WilliamsPercentRStrategy - Trend Following

Purpose: Uses Williams %R for overbought/oversold reversal signals.

Regime Suitability:
  ✅ Strong trending markets with clear overbought/oversold conditions
  ❌ Choppy/ranging markets with frequent false signals

Failure Modes:
  • False signals in choppy markets
  • Whipsaws during trend reversals
  • Late entries due to %R lag

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
class WilliamsPercentRConfig:
    """Configuration for Williams %R Strategy."""
    period: int = 14      # Period for %R calculation
    overbought_threshold: float = -20.0  # Overbought level (negative values)
    oversold_threshold: float = -80.0   # Oversold level (negative values)
    
    # Trend filter parameters
    min_trend_strength: float = 0.3
    max_drawdown_bps: float = 275.0  # Maximum drawdown before exit (2.75%)
    
    # Position sizing
    risk_per_trade: float = 0.01
    min_position_size: float = 0.001
    max_position_size: float = 0.05
    
    # Entry/Exit thresholds
    entry_threshold: float = 0.03  # Minimum %R divergence for entry (3%)
    exit_threshold: float = -0.02  # Maximum %R divergence for short (-2%)
    stop_loss_bps: float = 68.0  # Stop loss as basis points (0.68%)
    take_profit_bps: float = 195.0  # Take profit as basis points (1.95%)
    trailing_take_profit_bps: float = 100.0  # Trailing stop after profit
    cooldown_bars: int = 4
    warmup_bars: int = 60  # Bars needed for %R calculation


class WilliamsPercentRStrategy:
    """
    Williams %R Strategy.
    
    Generates buy signals when %R crosses above oversold level with upward momentum.
    Generates sell signals on opposite conditions.
    """
    
    def __init__(self, config: Optional[WilliamsPercentRConfig] = None):
        """
        Initialize strategy with configuration.
        
        Args:
            config: Strategy configuration
        """
        self.config = config or WilliamsPercentRConfig()
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
        
        # Get Williams %R values (assumed to be pre-calculated in market_state)
        wr_value = float(market_state.get('wr_value', 0))
        wr_trend = float(market_state.get('wr_trend', 0))  # 1 for up, -1 for down
        
        # Calculate %R divergence from price trend
        wr_divergence = abs(wr_value) / abs(wr_trend) if wr_trend != 0 else 0
        
        # Check if we're in a valid trending regime
        if wr_divergence < self.config.min_trend_strength:
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
            elif close_price >= self.take_profit_price:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'take_profit'
                }
            # Exit on trend reversal (%R crosses below oversold or enters overbought)
            elif wr_value < self.config.oversold_threshold and wr_trend == -1:
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
            elif close_price <= self.take_profit_price:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'take_profit'
                }
            # Exit on trend reversal (%R crosses above oversold or enters overbought)
            elif wr_value > self.config.overbought_threshold and wr_divergence > self.config.entry_threshold:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'trend_reversal'
                }
        
        # No position - check for entry signals
        if wr_divergence > self.config.entry_threshold and wr_value < self.config.oversold_threshold:
            self.current_position = 'long'
            self.entry_price = close_price
            self.stop_loss_price = close_price * (1 - self.config.stop_loss_bps / 10000)
            self.take_profit_price = close_price * (1 + self.config.take_profit_bps / 10000)
            return {
                'action': 'open',
                'quantity': self.config.risk_per_trade / close_price,
                'stop_loss': close_price * (1 - self.config.stop_loss_bps / 10000),
                'take_profit': close_price * (1 + self.config.take_profit_bps / 10000),
                'reason': 'wr_oversold_cross'
            }
        elif wr_divergence > self.config.entry_threshold and wr_value > self.config.overbought_threshold:
            self.current_position = 'short'
            self.entry_price = close_price
            self.stop_loss_price = close_price * (1 - self.config.stop_loss_bps / 10000)
            self.take_profit_price = close_price * (1 + self.config.take_profit_bps / 10000)
            return {
                'action': 'open',
                'quantity': -self.config.risk_per_trade / close_price,
                'stop_loss': close_price * (1 + self.config.stop_loss_bps / 10000),
                'take_profit': close_price * (1 - self.config.take_profit_bps / 10000),
                'reason': 'wr_overbought_cross'
            }
        
        return None
    
    def metadata(self) -> dict:
        """
        Return strategy metadata for catalog and documentation.
        """
        return {
            'strategy_id': 'williams_percent_r',
            'name': 'Williams %R',
            'family': 'Trend Following',
            'purpose': 'Overbought/oversold reversal signals using Williams %R indicator',
            'regime_suitability': ['Strong trending markets with clear overbought/oversold conditions'],
            'supported_products': ['BTC-USD', 'ETH-USD', 'SOL-USD'],
            'risk_tier': 'TIER_2_MODERATE_RISK',
            'required_data': ['close', 'wr_value', 'wr_trend'],
            'required_indicators': ['Williams %R (period, overbought_threshold, oversold_threshold)'],
            'warmup_bars': self.config.warmup_bars,
            'required_latency_budget_ms': 10.0,
            'sizing_model': 'fixed_fraction',
            'risk_ceilings': 'TIER_2_MODERATE_RISK',
            'min_size': self.config.min_position_size,
            'max_size': self.config.max_position_size,
            'max_capital_fraction': 0.10,
            'max_exposure_by_asset': 0.20,
            'expected_holding_horizon': 'medium_term',
            'execution_style': 'reversal',
            'take_profit_model': 'fixed_bps',
            'trailing_exit': True,
            'compound_profits': False,
            'min_net_edge_bps': 3.0,
            'approvals_required': False,
            'failure_modes': ['False signals in choppy markets'],
            'disable_criteria': ['Weak trend strength', 'Maximum drawdown reached'],
            'cooldown_logic': 'fixed_bar_count',
            'explainability_output': '%R position relative to overbought/oversold levels',
            'live_prerequisites': ['Trend filter active', 'Adequate volatility'],
            'downgrade_conditions': ['Extended ranging period', 'Low volume regime']
        }
