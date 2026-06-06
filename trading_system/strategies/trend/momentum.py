"""
MomentumStrategy - Trend Following

Purpose: Captures strong price movements using rate of change (ROC) and momentum indicators.

Regime Suitability:
  ✅ Strong trending markets with clear directional bias
  ❌ Ranging markets with low momentum

Failure Modes:
  • Whipsaws during sideways consolidation
  • False signals at trend exhaustion points
  • Late entries due to momentum lag

Expected Performance:
  • Win rate target: 48-53%
  • Profit factor target: 1.3-1.7
  • Maximum historical drawdown: 20-28%
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class MomentumConfig:
    """Configuration for Momentum Strategy."""
    period: int = 14      # Period for momentum calculation
    threshold_multiplier: float = 2.0  # Multiplier for momentum threshold (2x standard deviation)
    
    # Trend filter parameters
    min_trend_strength: float = 0.35
    max_drawdown_bps: float = 275.0  # Maximum drawdown before exit (2.75%)
    
    # Position sizing
    risk_per_trade: float = 0.01
    min_position_size: float = 0.001
    max_position_size: float = 0.05
    
    # Entry/Exit thresholds
    entry_threshold: float = 0.04  # Minimum momentum for entry (4%)
    exit_threshold: float = -0.03  # Maximum momentum decay for short (-3%)
    stop_loss_bps: float = 68.0  # Stop loss as basis points (0.68%)
    take_profit_bps: float = 195.0  # Take profit as basis points (1.95%)
    trailing_take_profit_bps: float = 100.0  # Trailing stop after profit
    cooldown_bars: int = 4
    warmup_bars: int = 60  # Bars needed for momentum calculation


class MomentumStrategy:
    """
    Momentum Strategy.
    
    Generates buy signals when price momentum exceeds threshold with upward trend.
    Generates sell signals on opposite conditions.
    """
    
    def __init__(self, config: Optional[MomentumConfig] = None):
        """
        Initialize strategy with configuration.
        
        Args:
            config: Strategy configuration
        """
        self.config = config or MomentumConfig()
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
        
        # Get momentum values (assumed to be pre-calculated in market_state)
        momentum_value = float(market_state.get('momentum', 0))
        momentum_trend = float(market_state.get('momentum_trend', 0))  # 1 for up, -1 for down
        
        # Calculate momentum strength relative to price
        momentum_strength = abs(momentum_value) / close_price if close_price != 0 else 0
        
        # Check if we're in a valid trending regime
        if momentum_strength < self.config.min_trend_strength:
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
            # Exit on trend reversal (momentum decays below threshold)
            elif momentum_strength < self.config.exit_threshold and momentum_trend == 1:
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
            # Exit on trend reversal (momentum expands above threshold)
            elif momentum_strength > self.config.entry_threshold and momentum_trend == -1:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'trend_reversal'
                }
        
        # No position - check for entry signals
        if momentum_strength > self.config.entry_threshold and momentum_trend == 1:
            return {
                'action': 'open',
                'quantity': self.config.risk_per_trade / close_price,
                'stop_loss': close_price * (1 - self.config.stop_loss_bps / 10000),
                'take_profit': close_price * (1 + self.config.take_profit_bps / 10000),
                'reason': 'momentum_upward'
            }
        elif momentum_strength > self.config.entry_threshold and momentum_trend == -1:
            return {
                'action': 'open',
                'quantity': -self.config.risk_per_trade / close_price,
                'stop_loss': close_price * (1 + self.config.stop_loss_bps / 10000),
                'take_profit': close_price * (1 - self.config.take_profit_bps / 10000),
                'reason': 'momentum_downward'
            }
        
        return None
    
    def metadata(self) -> dict:
        """
        Return strategy metadata for catalog and documentation.
        """
        return {
            'strategy_id': 'momentum',
            'name': 'Momentum',
            'family': 'Trend Following',
            'purpose': 'Captures strong price movements using rate of change (ROC) and momentum indicators',
            'regime_suitability': ['Strong trending markets with clear directional bias'],
            'supported_products': ['BTC-USD', 'ETH-USD', 'SOL-USD'],
            'risk_tier': 'TIER_2_MODERATE_RISK',
            'required_data': ['close', 'momentum', 'momentum_trend'],
            'required_indicators': ['Momentum (period, threshold_multiplier)'],
            'warmup_bars': self.config.warmup_bars,
            'required_latency_budget_ms': 10.0,
            'sizing_model': 'fixed_fraction',
            'risk_ceilings': 'TIER_2_MODERATE_RISK',
            'min_size': self.config.min_position_size,
            'max_size': self.config.max_position_size,
            'max_capital_fraction': 0.10,
            'max_exposure_by_asset': 0.20,
            'expected_holding_horizon': 'medium_term',
            'execution_style': 'momentum',
            'take_profit_model': 'fixed_bps',
            'trailing_exit': True,
            'compound_profits': False,
            'min_net_edge_bps': 4.0,
            'approvals_required': False,
            'failure_modes': ['Whipsaws in ranging markets'],
            'disable_criteria': ['Weak trend strength', 'Maximum drawdown reached'],
            'cooldown_logic': 'fixed_bar_count',
            'explainability_output': 'Momentum value and direction relative to threshold',
            'live_prerequisites': ['Trend filter active', 'Adequate volatility'],
            'downgrade_conditions': ['Extended ranging period', 'Low volume regime']
        }
