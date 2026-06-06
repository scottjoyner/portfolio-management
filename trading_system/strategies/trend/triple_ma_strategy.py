"""
TripleMovingAverageSystemStrategy - Trend Following

Purpose: Short/Medium/Long MA crossovers (e.g., 5/20/60). Uses Golden Cross/Death Cross signals.

Regime Suitability:
  ✅ Strong trending markets with clear direction
  ❌ Choppy/ranging markets with frequent false signals

Failure Modes:
  • Whipsaws during sideways consolidation
  • Lag in strong trends due to MA smoothing
  • False signals at key support/resistance levels

Expected Performance:
  • Win rate target: 45-50%
  • Profit factor target: 1.2-1.5
  • Maximum historical drawdown: 18-25%
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TripleMovingAverageConfig:
    """Configuration for Triple Moving Average System."""
    short_period: int = 5   # Fast MA (e.g., SMA or EMA)
    medium_period: int = 20  # Medium MA (e.g., SMA)
    long_period: int = 60    # Slow MA (e.g., SMA)
    
    # Trend filter parameters
    min_trend_strength: float = 0.4
    max_drawdown_bps: float = 250.0  # Maximum drawdown before exit (2.5%)
    
    # Position sizing
    risk_per_trade: float = 0.01
    min_position_size: float = 0.001
    max_position_size: float = 0.05
    
    # Entry/Exit thresholds
    entry_threshold: float = 0.03  # Minimum MA spread for entry (3%)
    exit_threshold: float = -0.02  # Maximum MA spread for short (-2%)
    stop_loss_bps: float = 60.0  # Stop loss as basis points (0.6%)
    take_profit_bps: float = 180.0  # Take profit as basis points (1.8%)
    trailing_take_profit_bps: float = 90.0  # Trailing stop after profit
    cooldown_bars: int = 5
    warmup_bars: int = 60  # Bars needed for long MA calculation


class TripleMovingAverageSystemStrategy:
    """
    Triple Moving Average System Strategy.
    
    Generates buy signals when short MA crosses above medium AND both are above long MA (Golden Cross).
    Generates sell signals on opposite conditions (Death Cross).
    """
    
    def __init__(self, config: Optional[TripleMovingAverageConfig] = None):
        """
        Initialize strategy with configuration.
        
        Args:
            config: Strategy configuration
        """
        self.config = config or TripleMovingAverageConfig()
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
        
        # Get MA values (assumed to be pre-calculated in market_state)
        short_ma = float(market_state.get('short_ma', 0))
        medium_ma = float(market_state.get('medium_ma', 0))
        long_ma = float(market_state.get('long_ma', 0))
        
        # Calculate MA spread for trend filter
        ma_spread = (short_ma - long_ma) / long_ma if long_ma != 0 else 0
        
        # Check if we're in a valid trending regime
        if abs(ma_spread) < self.config.min_trend_strength:
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
            # Exit on trend reversal (short MA crosses below medium)
            elif short_ma < medium_ma and ma_spread < self.config.exit_threshold:
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
            # Exit on trend reversal (short MA crosses above medium)
            elif short_ma > medium_ma and ma_spread > self.config.entry_threshold:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'trend_reversal'
                }
        
        # No position - check for entry signals
        if ma_spread > self.config.entry_threshold and short_ma > medium_ma:
            return {
                'action': 'open',
                'quantity': self.config.risk_per_trade / close_price,
                'stop_loss': close_price * (1 - self.config.stop_loss_bps / 10000),
                'take_profit': close_price * (1 + self.config.take_profit_bps / 10000),
                'reason': 'golden_cross'
            }
        elif ma_spread < self.config.exit_threshold and short_ma < medium_ma:
            return {
                'action': 'open',
                'quantity': -self.config.risk_per_trade / close_price,
                'stop_loss': close_price * (1 + self.config.stop_loss_bps / 10000),
                'take_profit': close_price * (1 - self.config.take_profit_bps / 10000),
                'reason': 'death_cross'
            }
        
        return None
    
    def metadata(self) -> dict:
        """
        Return strategy metadata for catalog and documentation.
        """
        return {
            'strategy_id': 'triple_ma_system',
            'name': 'Triple Moving Average System',
            'family': 'Trend Following',
            'purpose': 'Short/Medium/Long MA crossovers with Golden Cross/Death Cross signals',
            'regime_suitability': ['Strong trending markets'],
            'supported_products': ['BTC-USD', 'ETH-USD', 'SOL-USD'],
            'risk_tier': 'TIER_2_MODERATE_RISK',
            'required_data': ['close', 'short_ma', 'medium_ma', 'long_ma'],
            'required_indicators': ['Simple Moving Average (3 periods)'],
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
            'failure_modes': ['Whipsaws in ranging markets'],
            'disable_criteria': ['Weak trend strength', 'Maximum drawdown reached'],
            'cooldown_logic': 'fixed_bar_count',
            'explainability_output': 'MA spread direction and alignment',
            'live_prerequisites': ['Trend filter active', 'Adequate volatility'],
            'downgrade_conditions': ['Extended ranging period', 'Low volume regime']
        }
