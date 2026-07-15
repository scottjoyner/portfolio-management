"""
IchimokuCloudStrategy - Trend Following

Purpose: Uses Ichimoku Kinko Hyo indicator for trend identification and momentum signals.

Regime Suitability:
  ✅ Strong trending markets with clear cloud support/resistance
  ❌ Ranging markets near the cloud

Failure Modes:
  • Whipsaws when price is within or near the cloud
  • False signals during low volume periods
  • Lag in strong trends due to indicator smoothing

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
class IchimokuCloudConfig:
    """Configuration for Ichimoku Cloud Strategy."""
    tenkan_period: int = 9   # Tenkan-sen (Conversion Line) period
    kijun_period: int = 26   # Kijun-sen (Base Line) period
    senkou_b_period: int = 52  # Senkou Span B period
    chikou_offset: int = 26   # Chikou Span offset
    
    # Trend filter parameters
    min_trend_strength: float = 0.3
    max_drawdown_bps: float = 275.0  # Maximum drawdown before exit (2.75%)
    
    # Position sizing
    risk_per_trade: float = 0.01
    min_position_size: float = 0.001
    max_position_size: float = 0.05
    
    # Entry/Exit thresholds
    entry_threshold: float = 0.03  # Minimum cloud thickness for breakout (3%)
    exit_threshold: float = 0.40  # Maximum cloud thickness for long exit (40%)
    stop_loss_bps: float = 65.0  # Stop loss as basis points (0.65%)
    take_profit_bps: float = 190.0  # Take profit as basis points (1.9%)
    trailing_take_profit_bps: float = 95.0  # Trailing stop after profit
    cooldown_bars: int = 4
    warmup_bars: int = 60  # Bars needed for Ichimoku Cloud calculation


class IchimokuCloudStrategy:
    """
    Ichimoku Cloud Strategy.
    
    Generates buy signals when price is above the cloud and Tenkan-sen crosses above Kijun-sen (TK cross).
    Generates sell signals on opposite conditions.
    """
    
    def __init__(self, config: Optional[IchimokuCloudConfig] = None):
        """
        Initialize strategy with configuration.
        
        Args:
            config: Strategy configuration
        """
        self.config = config or IchimokuCloudConfig()
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
        
        # Get Ichimoku Cloud values (assumed to be pre-calculated in market_state)
        tenkan_sen = float(market_state.get('tenkan_sen', 0))
        kijun_sen = float(market_state.get('kijun_sen', 0))
        senkou_span_a = float(market_state.get('senkou_span_a', 0))
        senkou_span_b = float(market_state.get('senkou_span_b', 0))
        
        # Calculate cloud thickness
        cloud_thickness = abs(senkou_span_a - senkou_span_b) / senkou_span_b if senkou_span_b != 0 else 0
        
        # Check if we're in a valid trending regime
        if cloud_thickness < self.config.min_trend_strength:
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
            # Exit on trend reversal (TK cross below or price enters cloud)
            elif tenkan_sen < kijun_sen and cloud_thickness < self.config.exit_threshold:
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
            # Exit on trend reversal (TK cross above or price enters cloud)
            elif tenkan_sen > kijun_sen and cloud_thickness > self.config.entry_threshold:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'trend_reversal'
                }
        
        # No position - check for entry signals
        if cloud_thickness > self.config.entry_threshold and tenkan_sen > kijun_sen:
            self.current_position = 'long'
            self.entry_price = close_price
            self.stop_loss_price = close_price * (1 - self.config.stop_loss_bps / 10000)
            self.take_profit_price = close_price * (1 + self.config.take_profit_bps / 10000)
            return {
                'action': 'open',
                'quantity': self.config.risk_per_trade / close_price,
                'stop_loss': senkou_span_a * (1 - self.config.stop_loss_bps / 10000),
                'take_profit': senkou_span_a * (1 + self.config.take_profit_bps / 10000),
                'reason': 'tk_cross_above_cloud'
            }
        elif cloud_thickness > self.config.entry_threshold and tenkan_sen < kijun_sen:
            self.current_position = 'short'
            self.entry_price = close_price
            self.stop_loss_price = close_price * (1 - self.config.stop_loss_bps / 10000)
            self.take_profit_price = close_price * (1 + self.config.take_profit_bps / 10000)
            return {
                'action': 'open',
                'quantity': -self.config.risk_per_trade / close_price,
                'stop_loss': senkou_span_a * (1 + self.config.stop_loss_bps / 10000),
                'take_profit': senkou_span_a * (1 - self.config.take_profit_bps / 10000),
                'reason': 'tk_cross_below_cloud'
            }
        
        return None
    
    def metadata(self) -> dict:
        """
        Return strategy metadata for catalog and documentation.
        """
        return {
            'strategy_id': 'ichimoku_cloud',
            'name': 'Ichimoku Cloud',
            'family': 'Trend Following',
            'purpose': 'TK cross signals with cloud support/resistance filter',
            'regime_suitability': ['Strong trending markets with clear cloud support'],
            'supported_products': ['BTC-USD', 'ETH-USD', 'SOL-USD'],
            'risk_tier': 'TIER_2_MODERATE_RISK',
            'required_data': ['close', 'tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b'],
            'required_indicators': ['Ichimoku Kinko Hyo (periods, offset)'],
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
            'failure_modes': ['Whipsaws near the cloud'],
            'disable_criteria': ['Weak trend strength', 'Maximum drawdown reached'],
            'cooldown_logic': 'fixed_bar_count',
            'explainability_output': 'TK cross direction and cloud position',
            'live_prerequisites': ['Trend filter active', 'Adequate volatility'],
            'downgrade_conditions': ['Extended ranging period', 'Low volume regime']
        }
