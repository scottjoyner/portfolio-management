"""
MACDSignalCrossoverStrategy - Trend Following

Purpose: Classic MACD histogram and signal line crossover signals with trend filter.

Regime Suitability:
  ✅ Strong trending markets (BTC/ETH on daily bars)
  ❌ Ranging sideways markets (<5% weekly range)

Failure Modes:
  • Whipsaws near zero-line crossovers in ranging markets
  • False signals during low volume periods

Expected Performance:
  • Win rate target: 48-52%
  • Profit factor target: 1.3-1.6
  • Maximum historical drawdown: 15-20%
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class MACDSignalCrossoverConfig:
    """Configuration for MACD Signal Crossover Strategy."""
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9
    
    # Trend filter parameters
    min_trend_strength: float = 0.5  # Minimum ADX or similar indicator value
    max_drawdown_bps: float = 200.0  # Maximum drawdown before exit (2%)
    
    # Position sizing
    risk_per_trade: float = 0.01  # 1% of capital per trade
    min_position_size: float = 0.001  # Minimum position size as fraction of capital
    max_position_size: float = 0.05  # Maximum position size as fraction of capital
    
    # Entry/Exit thresholds
    entry_threshold: float = 0.02  # Minimum MACD histogram value for long entry (2%)
    exit_threshold: float = -0.01  # Histogram value for short entry (-1%)
    stop_loss_bps: float = 50.0  # Stop loss as basis points (0.5%)
    take_profit_bps: float = 150.0  # Take profit as basis points (1.5%)
    trailing_take_profit_bps: float = 75.0  # Trailing stop after profit
    cooldown_bars: int = 3  # Minimum bars between entries
    warmup_bars: int = 26  # Bars needed for MACD calculation


class MACDSignalCrossoverStrategy:
    """
    MACD Signal Crossover Strategy with Trend Filter.
    
    Generates buy signals when MACD histogram crosses above signal line AND trend is strong.
    Generates sell signals on opposite conditions.
    """
    
    def __init__(self, config: Optional[MACDSignalCrossoverConfig] = None):
        """
        Initialize strategy with configuration.
        
        Args:
            config: Strategy configuration (
        """
        self.config = config or MACDSignalCrossoverConfig()
        self.warmup_complete: bool = False
        self.current_position: Optional[str] = None  # 'long' or 'short'
        self.entry_price: float = 0.0
        self.stop_loss_price: float = 0.0
        self.take_profit_price: float = 0.0
    
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
        
        # Get MACD values (assumed to be pre-calculated in market_state)
        macd_line = float(market_state.get('macd', 0))
        signal_line = float(market_state.get('signal', 0))
        histogram = float(market_state.get('histogram', 0))
        
        # Get trend strength indicator (ADX or similar)
        trend_strength = float(market_state.get('trend_strength', 0.5))
        
        # Check if we're in a valid trending regime
        if trend_strength < self.config.min_trend_strength:
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
            # Exit on trend reversal
            elif histogram < self.config.exit_threshold:
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
            # Exit on trend reversal
            elif histogram > self.config.entry_threshold:
                return {
                    'action': 'close',
                    'quantity': -self.config.risk_per_trade / close_price,
                    'stop_loss': trailing_stop,
                    'take_profit': None,
                    'reason': 'trend_reversal'
                }
        
        # No position - check for entry signals
        if histogram > self.config.entry_threshold and macd_line > signal_line:
            self.current_position = 'long'
            self.entry_price = close_price
            self.stop_loss_price = close_price * (1 - self.config.stop_loss_bps / 10000)
            self.take_profit_price = close_price * (1 + self.config.take_profit_bps / 10000)
            return {
                'action': 'open',
                'quantity': self.config.risk_per_trade / close_price,
                'stop_loss': close_price * (1 - self.config.stop_loss_bps / 10000),
                'take_profit': close_price * (1 + self.config.take_profit_bps / 10000),
                'reason': 'macd_crossover'
            }
        elif histogram < self.config.exit_threshold and macd_line < signal_line:
            self.current_position = 'short'
            self.entry_price = close_price
            self.stop_loss_price = close_price * (1 - self.config.stop_loss_bps / 10000)
            self.take_profit_price = close_price * (1 + self.config.take_profit_bps / 10000)
            return {
                'action': 'open',
                'quantity': -self.config.risk_per_trade / close_price,
                'stop_loss': close_price * (1 + self.config.stop_loss_bps / 10000),
                'take_profit': close_price * (1 - self.config.take_profit_bps / 10000),
                'reason': 'macd_crossover'
            }
        
        return None
    
    def metadata(self) -> dict:
        """
        Return strategy metadata for catalog and documentation.
        """
        return {
            'strategy_id': 'macd_signal_crossover',
            'name': 'MACD Signal Crossover',
            'family': 'Trend Following',
            'purpose': 'Classic MACD histogram crossover with trend filter',
            'regime_suitability': ['Strong trending markets'],
            'supported_products': ['BTC-USD', 'ETH-USD', 'SOL-USD'],
            'risk_tier': 'TIER_2_MODERATE_RISK',
            'required_data': ['close', 'macd', 'signal', 'histogram', 'trend_strength'],
            'required_indicators': ['MACD', 'Signal Line', 'ADX or similar trend indicator'],
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
            'min_net_edge_bps': 2.0,
            'approvals_required': False,
            'failure_modes': ['Whipsaws in ranging markets'],
            'disable_criteria': ['Low trend strength', 'Maximum drawdown reached'],
            'cooldown_logic': 'fixed_bar_count',
            'explainability_output': 'MACD histogram direction and trend strength',
            'live_prerequisites': ['Trend filter active', 'Adequate volatility'],
            'downgrade_conditions': ['Extended ranging period', 'Low volume regime']
        }
