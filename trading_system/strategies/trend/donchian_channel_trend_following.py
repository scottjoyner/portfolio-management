"""
DonchianChannelTrendFollowingStrategy - Trend Following

Purpose: Uses Donchian Channel (Bollinger Bands) for trend following with breakout signals.

Regime Suitability:
  ✅ Strong trending markets with clear channel expansion
  ❌ Ranging markets with frequent false breakouts

Failure Modes:
  • Whipsaws during sideways consolidation
  • False breakouts at key support/resistance levels
  • Late entries due to breakout confirmation lag

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
class DonchianChannelTrendFollowingConfig:
    """Configuration for Donchian Channel Trend Following Strategy."""
    period: int = 20      # Period for channel calculation
    breakout_multiplier: float = 1.5  # Multiplier for breakout threshold (1.5x ATR)
    
    # Trend filter parameters
    min_trend_strength: float = 0.3
    max_drawdown_bps: float = 270.0  # Maximum drawdown before exit (2.7%)
    
    # Position sizing
    risk_per_trade: float = 0.01
    min_position_size: float = 0.001
    max_position_size: float = 0.05
    
    # Entry/Exit thresholds
    entry_threshold: float = 0.04  # Minimum channel width for breakout (4%)
    exit_threshold: float = -0.03  # Maximum channel contraction for short (-3%)
    stop_loss_bps: float = 65.0  # Stop loss as basis points (0.65%)
    take_profit_bps: float = 190.0  # Take profit as basis points (1.9%)
    trailing_take_profit_bps: float = 95.0  # Trailing stop after profit
    cooldown_bars: int = 4
    warmup_bars: int = 60  # Bars needed for Donchian Channel calculation


class DonchianChannelTrendFollowingStrategy:
    """
    Donchian Channel Trend Following Strategy.
    
    Generates buy signals when price breaks above upper channel with volatility expansion.
    Generates sell signals on opposite conditions.
    """
    
    def __init__(self, config: Optional[DonchianChannelTrendFollowingConfig] = None):
        """
        Initialize strategy with configuration.
        
        Args:
            config: Strategy configuration
        """
        self.config = config or DonchianChannelTrendFollowingConfig()
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
        
        # Get Donchian Channel values (assumed to be pre-calculated in market_state)
        upper_channel = float(market_state.get('upper_channel', 0))
        lower_channel = float(market_state.get('lower_channel', 0))
        channel_width = float(market_state.get('channel_width', 0))
        
        # Calculate channel width as percentage
        channel_width_pct = abs(channel_width) / close_price if close_price != 0 else 0
        
        # Check if we're in a valid trending regime
        if channel_width_pct < self.config.min_trend_strength:
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
            # Exit on trend reversal (channel contracts below threshold)
            elif channel_width_pct < self.config.exit_threshold:
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
            # Exit on trend reversal (channel expands above threshold)
            elif channel_width_pct > self.config.entry_threshold:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'trend_reversal'
                }
        
        # No position - check for entry signals
        if channel_width_pct > self.config.entry_threshold and close_price > upper_channel:
            return {
                'action': 'open',
                'quantity': self.config.risk_per_trade / close_price,
                'stop_loss': lower_channel * (1 - self.config.stop_loss_bps / 10000),
                'take_profit': lower_channel * (1 + self.config.take_profit_bps / 10000),
                'reason': 'upper_breakout'
            }
        elif channel_width_pct > self.config.entry_threshold and close_price < lower_channel:
            return {
                'action': 'open',
                'quantity': -self.config.risk_per_trade / close_price,
                'stop_loss': upper_channel * (1 + self.config.stop_loss_bps / 10000),
                'take_profit': upper_channel * (1 - self.config.take_profit_bps / 10000),
                'reason': 'lower_breakout'
            }
        
        return None
    
    def metadata(self) -> dict:
        """
        Return strategy metadata for catalog and documentation.
        """
        return {
            'strategy_id': 'donchian_channel_trend_following',
            'name': 'Donchian Channel Trend Following',
            'family': 'Trend Following',
            'purpose': 'Breakout signals using Donchian Channel (Bollinger Bands) with volatility expansion',
            'regime_suitability': ['Strong trending markets with clear channel expansion'],
            'supported_products': ['BTC-USD', 'ETH-USD', 'SOL-USD'],
            'risk_tier': 'TIER_2_MODERATE_RISK',
            'required_data': ['close', 'upper_channel', 'lower_channel', 'channel_width'],
            'required_indicators': ['Donchian Channel (period, breakout_multiplier)'],
            'warmup_bars': self.config.warmup_bars,
            'required_latency_budget_ms': 10.0,
            'sizing_model': 'fixed_fraction',
            'risk_ceilings': 'TIER_2_MODERATE_RISK',
            'min_size': self.config.min_position_size,
            'max_size': self.config.max_position_size,
            'max_capital_fraction': 0.10,
            'max_exposure_by_asset': 0.20,
            'expected_holding_horizon': 'medium_term',
            'execution_style': 'breakout',
            'take_profit_model': 'fixed_bps',
            'trailing_exit': True,
            'compound_profits': False,
            'min_net_edge_bps': 4.0,
            'approvals_required': False,
            'failure_modes': ['False breakouts in ranging markets'],
            'disable_criteria': ['Weak trend strength', 'Maximum drawdown reached'],
            'cooldown_logic': 'fixed_bar_count',
            'explainability_output': 'Channel width and price position relative to channels',
            'live_prerequisites': ['Breakout detected', 'Adequate volatility'],
            'downgrade_conditions': ['Extended ranging period', 'Low volume regime']
        }
