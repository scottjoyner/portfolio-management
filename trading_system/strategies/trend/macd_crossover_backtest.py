"""
MACD Crossover Backtesting Strategy Module
============================================

Purpose: Specialized backtesting engine for MACD crossover signals with comprehensive
performance metrics including win rate, profit factor, and drawdown analysis.

Features:
- MACD Line/Signal/Histogram crossover detection
- Win rate calculation with configurable P&L thresholds
- Profit factor calculation (Gross Profit / Gross Loss)
- Drawdown tracking and maximum drawdown identification
- Trade-by-trade P&L attribution
- Sharpe ratio estimation from backtest results

Usage Example:
    from trading_system.strategies.trend.macd_crossover_backtest import MACDBacktester
    
    # Initialize with test data
    ohlcv_data = get_ohlcv("BTC-USD", periods=365*24)
    
    # Create and configure backtester
    backtester = MACDBacktester()
    backtester.init(ohlcv_data, config={
        'fast_period': 12,
        'slow_period': 26,
        'signal_period': 9,
        'win_threshold_pct': 0.0,  # Any positive P&L is a win
        'loss_threshold_pct': -5.0  # Trade closes at -5% stop loss
    })
    
    # Run backtest
    results = backtester.run_backtest()
    
    # Get metrics
    print(f"Win Rate: {results['win_rate']:.1f}%")
    print(f"Profit Factor: {results['profit_factor']:.2f}")

Author: Portfolio Management System Team
DATE: June 2026
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import math
from collections import defaultdict


@dataclass
class MACDBacktestConfig:
    """Configuration parameters for MACD crossover backtester."""
    
    # MACD indicator periods
    fast_period: int = 12              # Short-term EMA period for MACD line
    slow_period: int = 26              # Long-term EMA period for signal line
    signal_period: int = 9             # Histogram smoothing period
    
    # Trading parameters
    win_threshold_pct: float = 0.0     # P&L threshold for winning trade (% positive is win)
    loss_threshold_pct: float = -5.0   # Maximum loss before closing (stop loss %)
    
    # Position sizing (simplified models)
    position_size_usd: float = 10000   # Standard position size in USD
    leverage: float = 1.0              # Trading leverage multiplier
    
    # Risk calculations
    risk_free_rate: float = 0.05       # Annual risk-free rate for Sharpe calc
    slippage_bps: float = 10           # Slippage in basis points (0.1%)
    commission_pct: float = 0.001      # Transaction commission percentage
    
    # Performance metrics
    min_trades_for_metrics: int = 5   # Minimum trades to calculate meaningful metrics


@dataclass
class MACDTrade:
    """Record individual MACD crossover trade."""
    
    trade_id: int
    signal_type: str                    # "MACD_BULLISH_CROSSOVER" or "MACD_BEARISH_CROSSOVER"
    entry_price: float
    exit_price: float
    entry_date: int                     # Bar index/timestamp for entry
    exit_date: int                      # Bar index/timestamp for exit
    
    quantity_usd: float                 # Position size in USD
    gross_profit_usd: float             # P&L before fees
    net_profit_usd: float               # P&L after fees/slippage
    pnl_pct: float                      # Return percentage
    
    is_win: bool                        # Trade meets win threshold criteria


@dataclass 
class MACDBacktestResults:
    """Comprehensive backtest results with all performance metrics."""
    
    total_signals: int = 0              # Total crossover signals detected
    trades_completed: int = 0           # Completed trades (exits recorded)
    winning_trades: int = 0             # Trades meeting win criteria
    losing_trades: int = 0              # Trades not meeting win criteria
    
    total_gross_profit_usd: float = 0.0 # Sum of positive trade P&L
    total_gross_loss_usd: float = 0.0   # Sum of negative trade P&L (absolute value)
    
    net_total_return_usd: float = 0.0   # Total net return from all trades
    
    win_rate_pct: float = 0.0           # Win rate percentage
    profit_factor: float = 0.0          # Gross Profit / Gross Loss ratio
    
    average_win_pct: float = 0.0        # Average P&L of winning trades
    average_loss_pct: float = 0.0       # Average P&L of losing trades (absolute)
    
    sharpe_ratio: float = 0.0           # Estimated Sharpe ratio
    max_drawdown_pct: float = 0.0      # Maximum drawdown during backtest
    
    # Trade-by-trade records
    trades: List[MACDTrade] = field(default_factory=list)


class MACDBacktester:
    """
    MACD Crossover Backtesting Strategy Module
    
    This module provides specialized backtesting for MACD crossover signals with
    comprehensive performance metrics including win rate, profit factor, and drawdown analysis.
    
    Factory Pattern Lifecycle:
        1. __init__(config): Create instance with optional configuration
        2. init(data, config): Initialize with OHLCV data and compute MACD indicators
        3. run_backtest(): Execute backtest and generate performance metrics
    
    Usage Example:
        from trading_system.strategies.trend.macd_crossover_backtest import MACDBacktester
        
        # Initialize backtester
        backtester = MACDBacktester()
        ohlcv_data = get_ohlcv("BTC-USD", periods=365*24)
        backtester.init(ohlcv_data)
        
        # Run backtest
        results = backtester.run_backtest()
        
        # Get metrics
        print(f"Win Rate: {results.win_rate_pct:.1f}%")
        print(f"Profit Factor: {results.profit_factor:.2f}")
    """
    
    def __init__(self, config: Optional[MACDBacktestConfig] = None):
        """Initialize backtester with default or custom configuration."""
        self.config = config or MACDBacktestConfig()
        
        # State tracking for MACD indicator computation
        self.macd_line_values: List[float] = []
        self.signal_line_values: List[float] = []
        self.histogram_values: List[float] = []
        
        # Performance tracking variables
        self._trades_completed: int = 0
        self._total_signals: int = 0
        self._winning_trades_count: int = 0
        self._losing_trades_count: int = 0
        
        # Equity curve for drawdown calculation
        self.equity_curve: List[float] = []
        
        # Trade records storage
        self.trades: List[MACDTrade] = []

    def init(self, data: List[dict], config: Optional[Dict] = None) -> None:
        """
        Initialize backtester with historical OHLCV data and compute MACD indicators.
        
        Args:
            data: List of OHLCV dicts in chronological order. Each dict must have:
                  - timestamp: Unix timestamp or bar index
                  - open, high, low, close: Price levels
                  - volume: Trading volume
            config: Optional dictionary to override default MACD parameters
        
        Raises:
            ValueError: If data is empty or too short for calculation
        """
        if not data:
            raise ValueError("Need historical OHLCV data for initialization.")
        
        # Apply config overrides if provided
        if config:
            for key in ['fast_period', 'slow_period', 'signal_period', 
                        'win_threshold_pct', 'loss_threshold_pct']:
                if key in config:
                    setattr(self.config, key, config[key])
        
        min_bars = max(self.config.fast_period, self.config.slow_period, 
                       self.config.signal_period) + 5
        
        if len(data) < min_bars:
            raise ValueError(
                f"Need at least {min_bars} bars for MACD calculation. "
                f"Got {len(data)} bars."
            )
        
        # Compute initial MACD indicators from historical data
        closes = [float(bar.get('close', 0)) for bar in data]
        if len(closes) > 0:
            self.macd_line_values, self.signal_line_values, self.histogram_values = \
                self._compute_macd_indicators(closes)
        
        # Initialize equity curve with starting capital
        self.equity_curve = [self.config.position_size_usd]

    def _compute_macd_indicators(self, closes: List[float]) -> Tuple[List[float], 
                                                                     List[float],
                                                                     List[float]]:
        """
        Compute MACD Line, Signal Line, and Histogram from historical close prices.
        
        Args:
            closes: List of close prices
        
        Returns:
            Tuple of (macd_line_values list, signal_line_values list, histogram_values list)
        
        Logic:
            - EMA(fast): Short-term exponential moving average
            - EMA(slow): Long-term exponential moving average  
            - MACD Line = EMA(fast) - EMA(slow)
            - Signal Line = EMA(MACD Line)
            - Histogram = MACD Line - Signal Line
        """
        if not closes or len(closes) == 0:
            return [], [], []
        
        fast_period = self.config.fast_period
        slow_period = self.config.slow_period
        signal_period = self.config.signal_period
        
        # Compute simple moving averages for initial warm-up period
        fast_ma = []
        slow_ma = []
        
        for i in range(len(closes)):
            window_size = min(fast_period + 1, i + 1)
            ma_value = sum(closes[max(0, i-window_size):i+1]) / window_size
            fast_ma.append(ma_value)
            
            window_size = min(slow_period + 1, i + 1)
            ma_value = sum(closes[max(0, i-window_size):i+1]) / window_size
            slow_ma.append(ma_value)
        
        # Compute MACD Line values (difference between fast and slow MA)
        macd_line_values = []
        for i in range(len(fast_ma)):
            macd_line_values.append(fast_ma[i] - slow_ma[i])
        
        # Compute Signal Line (EMA of MACD Line) using simple moving average
        signal_line_values = []
        for i in range(len(macd_line_values)):
            window_size = min(signal_period + 1, i + 1)
            signal_value = sum(macd_line_values[max(0, i-window_size):i+1]) / window_size
            signal_line_values.append(signal_value)
        
        # Compute Histogram values (MACD Line - Signal Line)
        histogram_values = [macd - signal for macd, signal in 
                           zip(macd_line_values, signal_line_values)]
        
        return macd_line_values, signal_line_values, histogram_values

    def on_bar(self, bar: dict) -> Optional[Dict]:
        """
        Process new bar and detect MACD crossover signals.
        
        Args:
            bar: New OHLCV bar with timestamp, open, high, low, close, volume
        
        Returns:
            Dict with signal information if crossover detected, None otherwise
            - action: "BUY" or "SELL"
            - macd_line: Current MACD line value
            - signal_line: Current signal line value  
            - histogram: Current histogram value
        """
        close_price = float(bar.get('close', bar.get('price', 0)))
        
        if not close_price or math.isnan(close_price) or close_price <= 0:
            return None
        
        # Append new price to compute updated MACD indicators
        closes = [close_price] + self.macd_line_values[:-1]
        macd_line_values, signal_line_values, histogram_values = \
            self._compute_macd_indicators(closes)
        
        current_macd_line = macd_line_values[-1] if macd_line_values else 0.0
        current_signal_line = signal_line_values[-1] if signal_line_values else 0.0
        current_histogram = histogram_values[-1] if histogram_values else 0.0
        
        # Store current values for crossover detection
        self.macd_line_values = [current_macd_line] + self.macd_line_values[:-1]
        self.signal_line_values = [current_signal_line] + self.signal_line_values[:-1]
        self.histogram_values = [current_histogram] + self.histogram_values[:-1]
        
        # Detect crossover signals
        prev_macd_line = self.macd_line_values[0] if len(self.macd_line_values) > 1 else current_macd_line * 1.05
        prev_signal_line = self.signal_line_values[0] if len(self.signal_line_values) > 1 else current_signal_line
        
        # BUY signal: MACD Line crosses above Signal Line (bullish crossover)
        bullish_crossover = (current_macd_line > current_signal_line and 
                           prev_macd_line <= prev_signal_line)
        
        # SELL signal: MACD Line crosses below Signal Line (bearish crossover)
        bearish_crossover = (current_macd_line < current_signal_line and
                           prev_macd_line >= prev_signal_line)
        
        if bullish_crossover:
            self._total_signals += 1
            
            return {
                'action': 'BUY',
                'macd_line': current_macd_line,
                'signal_line': current_signal_line,
                'histogram': current_histogram,
                'entry_price': close_price,
                'entry_date': len(self.equity_curve) - 1
            }
        
        elif bearish_crossover:
            self._total_signals += 1
            
            return {
                'action': 'SELL',
                'macd_line': current_macd_line,
                'signal_line': current_signal_line,
                'histogram': current_histogram,
                'exit_price': close_price,
                'exit_date': len(self.equity_curve) - 1
            }
        
        return None

    def handle_signal(self, signal: dict) -> Optional[MACDTrade]:
        """
        Handle execution of MACD crossover signal and update trade position.
        
        Args:
            signal: Return value from on_bar() if crossover triggered
        
        Returns:
            MACDTrade object with completed trade details, or None if position still open
        """
        action = signal.get('action')
        current_date = len(self.equity_curve) - 1
        
        if action == 'BUY':
            entry_price = signal.get('entry_price', 0)
            
            # Calculate slippage and commission
            slippage_bps = self.config.slippage_bps / 100.0
            effective_entry_price = entry_price * (1 + slippage_bps)
            commission_cost = entry_price * self.config.commission_pct
            
            # Open position with configured size
            position_value = self.config.position_size_usd * self.config.leverage
            quantity = position_value / effective_entry_price
            
            self._trades_completed += 1
            self.equity_curve.append(self.equity_curve[-1] - position_value)
            
        elif action == 'SELL':
            if self._trades_completed > 0:
                exit_price = signal.get('exit_price', 0)
                
                # Calculate trade P&L
                slippage_bps = self.config.slippage_bps / 100.0
                effective_exit_price = exit_price * (1 - slippage_bps)
                
                # Position value at exit
                position_value = self.config.position_size_usd * self.config.leverage
                
                # Gross profit/loss
                gross_profit_usd = (effective_exit_price - self.equity_curve[-2] / position_value 
                                   * self.config.position_size_usd) * self.config.leverage
                net_profit_usd = gross_profit_usd - (exit_price * self.config.commission_pct)
                
                # P&L percentage
                pnl_pct = net_profit_usd / self.config.position_size_usd
                
                # Check if win or loss based on thresholds
                is_win = pnl_pct >= abs(self.config.win_threshold_pct)
                
                # Create trade record
                trade_id = self._trades_completed + 1
                signal_type = 'MACD_BULLISH_CROSSOVER' if self._total_signals % 2 == 0 else 'MACD_BEARISH_CROSSOVER'
                
                trade = MACDTrade(
                    trade_id=trade_id,
                    signal_type=signal_type,
                    entry_price=self.equity_curve[-2] / position_value * self.config.leverage,
                    exit_price=exit_price,
                    entry_date=current_date - 1,  # Entry was previous bar
                    exit_date=current_date,
                    quantity_usd=position_value,
                    gross_profit_usd=gross_profit_usd,
                    net_profit_usd=net_profit_usd,
                    pnl_pct=pnl_pct,
                    is_win=is_win
                )
                
                self.trades.append(trade)
                
                # Update metrics counters
                if is_win:
                    self._winning_trades_count += 1
                    total_gross_profit = sum(t.gross_profit_usd for t in self.trades if t.gross_profit_usd > 0)
                    total_gross_loss = abs(sum(t.gross_profit_usd for t in self.trades if t.gross_profit_usd < 0))
                else:
                    self._losing_trades_count += 1
                
                # Update equity curve
                self.equity_curve.append(self.equity_curve[-1] + net_profit_usd)

    def run_backtest(self) -> MACDBacktestResults:
        """
        Run complete backtest on historical data and generate performance metrics.
        
        Returns:
            MACDBacktestResults with comprehensive performance metrics
        
        Raises:
            ValueError: If not initialized with data first
        """
        if len(self.equity_curve) == 0:
            raise ValueError("Backtester must be initialized with OHLCV data before running backtest.")
        
        # Process all bars and detect signals
        results = MACDBacktestResults()
        
        # Close any open positions at the end of the dataset
        if self._trades_completed > 0:
            current_date = len(self.equity_curve) - 1
            
            exit_price = float(self.equity_curve[-2] / 
                             (self.config.position_size_usd * self.config.leverage))
            
            slippage_bps = self.config.slippage_bps / 100.0
            effective_exit_price = exit_price * (1 - slippage_bps)
            
            position_value = self.config.position_size_usd * self.config.leverage
            
            # Calculate final trade P&L
            gross_profit_usd = (effective_exit_price - 
                               self.equity_curve[-2] / position_value * self.config.leverage) * \
                              self.config.leverage
            net_profit_usd = gross_profit_usd - (exit_price * self.config.commission_pct)
            
            pnl_pct = net_profit_usd / self.config.position_size_usd
            is_win = pnl_pct >= abs(self.config.win_threshold_pct)
            
            # Create final trade record
            trade_id = self._trades_completed + 1
            
            # Determine signal type (simplified for end of dataset)
            last_macd = self.macd_line_values[-1] if self.macd_line_values else 0.0
            last_signal = self.signal_line_values[-1] if self.signal_line_values else 0.0
            
            if last_macd > last_signal:
                signal_type = 'MACD_BULLISH_CROSSOVER'
            else:
                signal_type = 'MACD_BEARISH_CROSSOVER'
            
            trade = MACDTrade(
                trade_id=trade_id,
                signal_type=signal_type,
                entry_price=self.equity_curve[-2] / position_value * self.config.leverage,
                exit_price=effective_exit_price,
                entry_date=current_date - 1,
                exit_date=current_date,
                quantity_usd=position_value,
                gross_profit_usd=gross_profit_usd,
                net_profit_usd=net_profit_usd,
                pnl_pct=pnl_pct,
                is_win=is_win
            )
            
            self.trades.append(trade)
            results.trades_completed += 1
            
            if is_win:
                results.winning_trades += 1
                total_gross_profit = sum(t.gross_profit_usd for t in self.trades if t.gross_profit_usd > 0)
                total_gross_loss = abs(sum(t.gross_profit_usd for t in self.trades if t.gross_profit_usd < 0))
            else:
                results.losing_trades += 1
        
        # Set results from internal state
        results.total_signals = self._total_signals
        results.trades_completed = len(self.trades)
        results.winning_trades = self._winning_trades_count
        results.losing_trades = self._losing_trades_count
        
        # Calculate total gross profit and loss
        results.total_gross_profit_usd = sum(t.gross_profit_usd for t in self.trades if t.gross_profit_usd > 0)
        results.total_gross_loss_usd = abs(sum(t.gross_profit_usd for t in self.trades if t.gross_profit_usd < 0))
        
        # Calculate net return
        results.net_total_return_usd = sum(t.net_profit_usd for t in self.trades)
        
        # Calculate win rate
        total_completed_trades = len(self.trades)
        results.win_rate_pct = (self._winning_trades_count / total_completed_trades * 100 
                               if total_completed_trades > 0 else 0.0)
        
        # Calculate profit factor
        if abs(results.total_gross_loss_usd) > 0:
            results.profit_factor = results.total_gross_profit_usd / abs(results.total_gross_loss_usd)
        else:
            results.profit_factor = float('inf') if results.total_gross_profit_usd > 0 else 1.0
        
        # Calculate average win/loss
        winning_trades = [t for t in self.trades if t.gross_profit_usd > 0]
        losing_trades = [t for t in self.trades if t.gross_profit_usd <= 0]
        
        results.average_win_pct = sum(t.pnl_pct for t in winning_trades) / len(winning_trades) \
                                  if winning_trades else 0.0
        results.average_loss_pct = abs(sum(t.pnl_pct for t in losing_trades) / len(losing_trades)) \
                                   if losing_trades else 0.0
        
        # Calculate Sharpe ratio
        num_bars = len(self.equity_curve) - 1
        equity_values = [max(e, self.equity_curve[0]) for e in self.equity_curve]  # Avoid negative equity
        returns = [(equity_values[i+1] - equity_values[i]) / equity_values[i] 
                   for i in range(len(equity_values)-1)] if len(equity_values) > 1 else []
        
        avg_return = sum(returns) / len(returns) if returns else 0.0
        volatility = math.sqrt(sum((r - avg_return) ** 2 for r in returns) / len(returns)) \
                     if len(returns) > 1 and avg_return != 0 else 0.0
        
        # Annualize returns (assuming daily bars, 365 days)
        if volatility > 0:
            annualized_return = avg_return * math.sqrt(365)
            results.sharpe_ratio = (annualized_return / volatility - self.config.risk_free_rate)
        else:
            results.sharpe_ratio = 0.0
        
        # Calculate maximum drawdown
        peak = self.equity_curve[0]
        max_dd = 0.0
        for equity in self.equity_curve:
            if equity > peak:
                peak = equity
            dd = (peak - equity) / peak * 100 if peak > 0 else 0.0
            max_dd = max(max_dd, dd)
        
        results.max_drawdown_pct = max_dd
        
        return results

    def get_performance_summary(self) -> Dict:
        """
        Get human-readable performance summary of backtest.
        
        Returns:
            Dictionary with formatted performance metrics
        """
        if len(self.trades) == 0:
            return {
                'total_signals': self._total_signals,
                'status': 'No completed trades yet',
                'message': 'Run backtest() first to get performance metrics'
            }
        
        results = self.run_backtest()
        
        summary = {
            'strategy': 'MACD Crossover Strategy',
            'period_covered': f'{len(self.equity_curve)} bars analyzed',
            'total_signals': results.total_signals,
            'completed_trades': results.trades_completed,
            'winning_trades': results.winning_trades,
            'losing_trades': results.losing_trades,
            'win_rate_pct': f"{results.win_rate_pct:.1f}%",
            'profit_factor': f"{results.profit_factor:.2f}" if results.profit_factor != float('inf') else '>5.00',
            'net_return_usd': f"${results.net_total_return_usd:.2f}",
            'average_win_pct': f"{results.average_win_pct:.2f}%",
            'average_loss_pct': f"-{results.average_loss_pct:.2f}%",
            'sharpe_ratio': f"{results.sharpe_ratio:.2f}",
            'max_drawdown_pct': f"{results.max_drawdown_pct:.1f}%"
        }
        
        return summary


def run_macd_crossover_backtest_demo() -> Dict:
    """
    Run demonstration backtest with simulated OHLCV data.
    
    Returns:
        Performance metrics dictionary
    """
    print("=" * 80)
    print("MACD CROSSOVER BACKTESTING MODULE - DEMO")
    print("=" * 80)
    print()
    
    # Create backtester with custom configuration
    config = MACDBacktestConfig(
        fast_period=12,
        slow_period=26,
        signal_period=9,
        position_size_usd=10000,
        leverage=1.0,
        slippage_bps=10,
        commission_pct=0.001,
    )
    
    backtester = MACDBacktestConfig(config)

    # Generate simulated OHLCV data for demonstration
    print("Generating simulated OHLCV test data...")
    test_data = []
    base_price = 42000.0
    price = base_price
    
    for i in range(365):  # 1 year of daily bars
        bar = {
            'timestamp': i,
            'open': price * (1 + math.sin(i/30) * 0.03),
            'high': price * (1 + math.sin(i/30) * 0.05),
            'low': price * (1 - math.sin(i/30) * 0.02),
            'close': price * (1 + math.sin(i/30) * 0.04),
            'volume': 1000000,
        }
        test_data.append(bar)
        
        # Simulate trend
        if i % 7 != 0:  # Add random walk noise
            price = bar['close'] * (1 + 0.002 - abs(math.sin(i/10) * 0.005))
    
    print(f"Created {len(test_data)} bars of simulated OHLCV data")
    print()
    
    # Initialize backtester with test data
    print("Initializing MACD Backtester...")
    ohlcv_config = MACDBacktestConfig(
        fast_period=12,
        slow_period=26,
        signal_period=9,
        position_size_usd=10000,
        leverage=1.0,
        slippage_bps=10,
        commission_pct=0.001,
    )
    
    backtester = MACDBacktester(ohlcv_config)
    backtester.init(test_data)
    
    print("Starting backtest execution...")
    results = backtester.run_backtest()
    
    print()
    print("=" * 80)
    print("BACKTEST RESULTS SUMMARY")
    print("=" * 80)
    print()
    
    summary = backtester.get_performance_summary()
    for key, value in summary.items():
        print(f"{key:25s}: {value}")
    
    return results


if __name__ == '__main__':
    # Run demonstration
    results = run_macd_crossover_backtest_demo()