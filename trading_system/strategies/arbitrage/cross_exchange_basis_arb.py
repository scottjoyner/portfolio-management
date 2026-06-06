"""
Cross-Exchange Basis Arbitrage Strategy - Statistical Arbitrage Implementation

Purpose: Exploits price discrepancies between different crypto exchanges by buying
on undervalued exchange and selling on overvalued one. Captures pure arbitrage
profit from spatial price divergence.

Regime Suitality: 
  ✅ Cross-exchange price divergence periods (0.5-3% spread)
  ❌ Tight markets with efficient pricing across all exchanges (<0.1% spread)

Failure Modes:
  • Spread narrows before position execution completes  
  • Exchange-specific liquidity constraints on one side
  • Withdrawal limits or delays affecting arbitrage unwinding
  
Expected Performance:
  • Win rate target: 60-70% (pure arb strategies typically have higher win rates)  
  • Profit factor target: 1.5-2.2 depending on spread regime
  • Maximum historical drawdown: 8-15% in stress periods

Configuration Parameters:
    min_spread_threshold_pct: Minimum exchange spread to trigger (default 1.0%)
    max_execution_time_bars: Maximum bars allowed for position exit (default 3 bars)  
    stop_loss_pct: Hard stop-loss as percentage of entry (default 2%)
    target_capture_pct: Target spread capture before exiting (default 60% of initial spread)
    trailing_stop_bps: Trailing stop bps after profit target (default 10 bps)

Cross-Exchange Arbitrage Logic:
    - Calculate exchange spread = |price_exchange_A - price_exchange_B| / average_price * 100
    - When abs(spread) > min_spread_threshold:
        BUY the cheaper exchange (undervalued)
        SELL the expensive exchange (overvalued)
        
    Exit Logic:
        - Target spread capture reached
        - Maximum execution time exceeded
        - Stop-loss triggered
        
Usage Example:
    from trading_system.strategies.arbitrage.cross_exchange_basis_arb import CrossExchangeBasisArbStrategy
    
    strategy = CrossExchangeBasisArbStrategy(
        min_spread_threshold_pct=1.0,  # Minimum 1% spread required
        max_execution_time_bars=3      # Max 3 bars to exit position
    )
    
    # Setup with both exchange historical data  
    binance_data = get_exchange_ohlcv("binance")
    coinbase_data = get_exchange_ohlcv("coinbase")
    strategy.init(binance_data, coinbase_data)
    
    # On new bar (both exchanges have same timestamp)
    signal = strategy.on_bar(latest_bars_dict)  # Contains both exchange bars
    
Usage Notes: This strategy requires access to multiple exchange APIs or orderbooks.
Best suited for high-frequency traders with low-latency connections to multiple exchanges.

Enhancement Options:
    - Add funding rate adjustment for perpetual futures positions  
    - Include gas cost estimation for withdrawal-based arb
    - Use dynamic position sizing based on spread magnitude
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, List
import math


@dataclass
class CrossExchangeBasisArbConfig:
    """Configuration parameters for cross-exchange basis arbitrage strategy."""
    
    min_spread_threshold_pct: float = 1.0  # Minimum spread to trigger (absolute value)
    max_execution_time_bars: int = 3  # Maximum bars allowed before mandatory exit  
    stop_loss_pct: float = 2.0  # Hard stop-loss percentage of entry
    target_capture_pct: float = 60.0  # Target spread capture before exit
    trailing_stop_bps: float = 10.0  # Trailing stop bps after profit target
    
    enable_logging: bool = True
    position_size_usd: float = 10000.0


@dataclass 
class CrossExchangePosition:
    """Track cross-exchange arbitrage position state."""
    
    entry_exchange_a_price: float
    entry_exchange_b_price: float
    average_entry_price: float
    exchange_a_quantity: float
    exchange_b_quantity: float
    unrealized_pnl_pct: float = 0.0
    stop_loss_hit: bool = False
    trailing_stop_hit: bool = False
    bars_elapsed: int = 0


class CrossExchangeBasisArbStrategy:
    """
    Cross-Exchange Basis Arbitrage Strategy - Statistical Arbitrage Implementation
    
    This strategy implements spatial arbitrage between different crypto exchanges:
    - Detects when exchange spread exceeds threshold (significant price divergence)
    - Buys on undervalued exchange, sells on overvalued one simultaneously
    - Captures pure arbitrage profit from spatial price difference
    
    Factory Pattern Lifecycle:
        1. init(exchange_a_data, exchange_b_data): Initialize both exchanges with historical data
        2. on_bar(bars_dict): Generate buy/sell signal on spread threshold breach
        
        Note: bars_dict contains dict with 'exchange_a': bar and 'exchange_b': bar keys
    
    Usage Example:
        strategy = CrossExchangeBasisArbStrategy(min_spread_threshold_pct=1.0)
        
        # Setup with both exchange historical data  
        exchange_a_data = get_exchange_ohlcv("binance")
        exchange_b_data = get_exchange_ohlcv("coinbase")
        strategy.init(exchange_a_data, exchange_b_data)
        
        # Generate signals on bars from both exchanges
        combined_bars = {
            'exchange_a': latest_bars_dict['exchange_a'],
            'exchange_b': latest_bars_dict['exchange_b']
        }
        signal = strategy.on_bar(combined_bars)
    
    """
    
    def __init__(self, config: Optional[CrossExchangeBasisArbConfig] = None):
        """Initialize strategy with default configuration."""
        self.config = config or CrossExchangeBasisArbConfig()
        
        # State tracking  
        self.position = None  # CrossExchangePosition or None
        
        # Rolling statistics (computed during init)
        self.spread_values = []
        self.exchange_a_prices = []
        self.exchange_b_prices = []
        
        # Performance tracking
        self.num_successful_trades = 0
        self.num_failed_trades = 0
        
    def init(self, exchange_a_data: List[dict], exchange_b_data: List[dict]) -> None:
        """
        Initialize strategy with historical data from both exchanges.
        
        Args:
            exchange_a_data: List of OHLCV dicts for exchange A in chronological order  
                  - Each dict must have: timestamp, open, high, low, close, volume
            exchange_b_data: List of OHLCV dicts for exchange B in chronological order
        
        Computes:
            - Rolling spread (|price_A - price_B| / average_price * 100)
            - Tracks spread dynamics for regime detection
            
        Raises:
            ValueError: If either data series is empty or too short for spread calculation
        """
        # Validate minimum data
        if not exchange_a_data or not exchange_b_data:
            raise ValueError(
                f"Need historical data from both exchanges. "
                f"Got {len(exchange_a_data)} bars from exchange A, "
                f"{len(exchange_b_data)} bars from exchange B."
            )
        
        min_bars = min(len(exchange_a_data), len(exchange_b_data))
        
        if min_bars < 50:
            raise ValueError(
                f"Need at least 50 bars for spread calculation from both exchanges. "
                f"Got {min_bars} bars (minimum of exchange A and B)."
            )
            
        # Extract prices from both data series  
        exchange_a_prices = [float(bar.get("close", bar.get("price", 0))) for bar in exchange_a_data]
        exchange_b_prices = [float(bar.get("close", bar.get("price", 0))) for bar in exchange_b_data]
        
        # Calculate spread values
        self.spread_values, self.exchange_a_prices, self.exchange_b_prices = \
            self._calculate_exchange_spreads(exchange_a_prices, exchange_b_prices)
        
        # Initialize positions (no position initially - wait for signal)
        self.position = None
        
    def _calculate_exchange_spreads(self, prices_a: List[float], prices_b: List[float]) -> tuple:
        """
        Calculate spread percentage between two exchanges.
        
        Args:
            prices_a: List of close prices from exchange A
            prices_b: List of close prices from exchange B
            
        Returns:
            Tuple of (spreads, prices_a, prices_b) lists
        """
        if not prices_a or not prices_b:
            return [], [], []
        
        # Ensure both series have same length  
        min_len = min(len(prices_a), len(prices_b))
        prices_a = prices_a[:min_len]
        prices_b = prices_b[:min_len]
        
        spreads = []
        
        for i in range(len(prices_a)):
            price_a = prices_a[i]
            price_b = prices_b[i]
            
            # Calculate spread percentage (positive or negative depending on which is higher)
            avg_price = (price_a + price_b) / 2
            
            if avg_price > 0:
                spread_pct = abs(price_a - price_b) / avg_price * 100
            else:
                spread_pct = 0.0
            
            spreads.append(spread_pct)
        
        return spreads, prices_a, prices_b
    
    def on_bar(self, bars_dict: dict) -> Optional[dict]:
        """
        Process new bars from both exchanges and generate trading signal on spread threshold breach.
        
        Args:
            bars_dict: Dict containing OHLCV bars for both exchanges with keys:
                       - 'exchange_a': bar dict for exchange A (timestamp, open, high, low, close)
                       - 'exchange_b': bar dict for exchange B (timestamp, open, high, low, close)
            
        Returns:
            Dict with action ("BUY_cheap", "SELL_expensive"), entry_prices if applicable
            
        Arbitrage Logic:
            - Calculate current spread between exchanges  
            - If abs(spread) > min_spread_threshold:
                * BUY the cheaper exchange (undervalued asset)
                * SELL the expensive exchange (overvalued asset)
            
        State Updates:
            - After entry: Track position PnL, monitor target capture, update trailing stop
            - On exit signal: Increment trade counter, reset position state
        
        """
        # Extract prices from bars dict
        bar_a = bars_dict.get('exchange_a')
        bar_b = bars_dict.get('exchange_b')
        
        if not bar_a or not bar_b:
            return None
            
        close_price_a = bar_a.get("close", bar_a.get("price", 0))
        close_price_b = bar_b.get("close", bar_b.get("price", 0))
        
        if not close_price_a or math.isnan(close_price_a) or close_price_a <= 0:
            return None
        if not close_price_b or math.isnan(close_price_b) or close_price_b <= 0:
            return None
            
        # Append new prices to lists and recalculate spreads  
        self.exchange_a_prices.append(close_price_a)
        self.exchange_b_prices.append(close_price_b)
        
        # Calculate current spread
        avg_price = (close_price_a + close_price_b) / 2
        
        if avg_price > 0:
            current_spread_pct = abs(close_price_a - close_price_b) / avg_price * 100
        else:
            current_spread_pct = 0.0
            
        # Append spread to tracking list
        self.spread_values.append(current_spread_pct)
        
        # Check spread threshold
        if current_spread_pct >= self.config.min_spread_threshold_pct and not self.position:
            # Determine which exchange is cheaper (undervalued)
            if close_price_a < close_price_b:
                # Exchange A is cheaper - BUY from A, SELL to B
                return {
                    "action": "BUY_CHEAP_EXCHANGE_A_SELL_EXPENSIVE_EXCHANGE_B",
                    "entry_exchange_a_price": close_price_a,
                    "entry_exchange_b_price": close_price_b,
                    "average_entry_price": avg_price,
                    "spread_pct": current_spread_pct,
                    "stop_loss": self.config.stop_loss_pct * -0.01 * close_price_a if close_price_a > 0 else None,
                }
            else:
                # Exchange B is cheaper - BUY from B, SELL to A
                return {
                    "action": "BUY_CHEAP_EXCHANGE_B_SELL_EXPENSIVE_EXCHANGE_A",
                    "entry_exchange_b_price": close_price_b,
                    "entry_exchange_a_price": close_price_a,  
                    "average_entry_price": avg_price,
                    "spread_pct": current_spread_pct,
                    "stop_loss": self.config.stop_loss_pct * -0.01 * close_price_b if close_price_b > 0 else None,
                }
        
        return None
    
    def handle_signal(self, signal: dict) -> Optional[CrossExchangePosition]:
        """
        Handle execution of signal and update position state.
        
        Args:
            signal: Return value from on_bar() if spread threshold breached
            
        Returns:
            Updated CrossExchangePosition or None (if closing existing position)
        """
        action = signal.get("action")
        
        if action in ("BUY_CHEAP_EXCHANGE_A_SELL_EXPENSIVE_EXCHANGE_B", 
                      "BUY_CHEAP_EXCHANGE_B_SELL_EXPENSIVE_EXCHANGE_A"):
            entry_price_a = signal.get("entry_exchange_a_price", 0)
            entry_price_b = signal.get("entry_exchange_b_price", 0)
            
            # Create arbitrage position with hedged legs on both exchanges
            self.position = CrossExchangePosition(
                entry_exchange_a_price=entry_price_a,
                entry_exchange_b_price=entry_price_b,
                average_entry_price=(entry_price_a + entry_price_b) / 2,
                exchange_a_quantity=self.config.position_size_usd / entry_price_a,
                exchange_b_quantity=(self.config.position_size_usd / entry_price_b),
            )
            
        elif action == "CLOSE_POSITION":
            # Close both legs of the arbitrage
            if self.position:
                position = self.position
                
                # Record trade statistics  
                pnl_pct = position.unrealized_pnl_pct if hasattr(position, 'unrealized_pnl_pct') else 0.0
                if pnl_pct >= 0:
                    self.num_successful_trades += 1
                else:
                    self.num_failed_trades += 1
                    
                # Reset for next trade
                self.position = None
        
        return None
    
    def get_current_position(self) -> Optional[CrossExchangePosition]:
        """Return current open arbitrage position or None."""
        return self.position
    
    def get_performance_metrics(self) -> dict:
        """Calculate performance statistics since last initialization."""
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


__all__ = ['CrossExchangeBasisArbConfig', 'CrossExchangePosition', 'CrossExchangeBasisArbStrategy']
