"""
BollingerBandSqueezeStrategy - Trend Following

Purpose: Buy on Bollinger Band squeeze (volatility contraction), sell on expansion breakout.

Regime Suitability:
  ✅ Volatile trending markets after consolidation
  ❌ Already expanding volatile markets

Failure Modes:
  • False breakouts in choppy markets
  • Whipsaws during sideways volatility expansion
  • Late entries due to squeeze duration

Expected Performance:
  • Win rate target: 46-51%
  • Profit factor target: 1.3-1.7
  • Maximum historical drawdown: 20-28%
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class BollingerBandSqueezeConfig:
    """Configuration for Bollinger Band Squeeze Strategy."""
    period: int = 20      # MA period for middle band
    num_std_devs: float = 2.0  # Number of standard deviations for bands
    squeeze_period: int = 5   # Minimum bars in squeeze before entry
    expansion_threshold: float = 0.15  # Band width threshold for breakout (15%)
    
    # Trend filter parameters
    min_trend_strength: float = 0.3
    max_drawdown_bps: float = 280.0  # Maximum drawdown before exit (2.8%)
    
    # Position sizing
    risk_per_trade: float = 0.01
    min_position_size: float = 0.001
    max_position_size: float = 0.05
    
    # Entry/Exit thresholds
    entry_threshold: float = 0.04  # Minimum band width for breakout (4%)
    exit_threshold: float = -0.03  # Maximum band width for short (-3%)
    stop_loss_bps: float = 70.0  # Stop loss as basis points (0.7%)
    take_profit_bps: float = 200.0  # Take profit as basis points (2.0%)
    trailing_take_profit_bps: float = 100.0  # Trailing stop after profit
    cooldown_bars: int = 5
    warmup_bars: int = 60  # Bars needed for Bollinger Band calculation


class BollingerBandSqueezeStrategy:
    """
    Bollinger Band Squeeze Strategy.
    
    Generates buy signals when bands contract (squeeze) and then expand upward with price breaking upper band.
    Generates sell signals on opposite conditions.
    """
    
    def __init__(self, config: Optional[BollingerBandSqueezeConfig] = None):
        """
        Initialize strategy with configuration.
        
        Args:
            config: Strategy configuration
        """
        self.config = config or BollingerBandSqueezeConfig()
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
        
        # Get Bollinger Band values (assumed to be pre-calculated in market_state)
        middle_band = float(market_state.get('middle_band', 0))
        upper_band = float(market_state.get('upper_band', 0))
        lower_band = float(market_state.get('lower_band', 0))
        
        # Calculate band width
        band_width = (upper_band - lower_band) / middle_band if middle_band != 0 else 0
        
        # Check if we're in a valid trending regime
        if abs(band_width) < self.config.min_trend_strength:
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
            # Exit on trend reversal (price breaks below middle band)
            elif close_price < middle_band and band_width < self.config.exit_threshold:
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
            # Exit on trend reversal (price breaks above middle band)
            elif close_price > middle_band and band_width > self.config.entry_threshold:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'trend_reversal'
                }
        
        # No position - check for entry signals
        if band_width > self.config.entry_threshold and close_price > upper_band:
            return {
                'action': 'open',
                'quantity': self.config.risk_per_trade / close_price,
                'stop_loss': middle_band * (1 - self.config.stop_loss_bps / 10000),
                'take_profit': middle_band * (1 + self.config.take_profit_bps / 10000),
                'reason': 'upper_breakout'
            }
        elif band_width > self.config.entry_threshold and close_price < lower_band:
            return {
                'action': 'open',
                'quantity': -self.config.risk_per_trade / close_price,
                'stop_loss': middle_band * (1 + self.config.stop_loss_bps / 10000),
                'take_profit': middle_band * (1 - self.config.take_profit_bps / 10000),
                'reason': 'lower_breakout'
            }
        
        return None
    
    def metadata(self) -> dict:
        """
        Return strategy metadata for catalog and documentation.
        """
        return {
            'strategy_id': 'bollinger_band_squeeze',
            'name': 'Bollinger Band Squeeze',
            'family': 'Trend Following',
            'purpose': 'Buy on volatility contraction (squeeze) followed by expansion breakout',
            'regime_suitability': ['Volatile trending markets after consolidation'],
            'supported_products': ['BTC-USD', 'ETH-USD', 'SOL-USD'],
            'risk_tier': 'TIER_2_MODERATE_RISK',
            'required_data': ['close', 'middle_band', 'upper_band', 'lower_band'],
            'required_indicators': ['Bollinger Bands (period, num_std_devs)'],
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
            'failure_modes': ['False breakouts in choppy markets'],
            'disable_criteria': ['Weak trend strength', 'Maximum drawdown reached'],
            'cooldown_logic': 'fixed_bar_count',
            'explainability_output': 'Band width and price position relative to bands',
            'live_prerequisites': ['Squeeze detected', 'Adequate volatility'],
            'downgrade_conditions': ['Extended ranging period', 'Low volume regime']
        }
