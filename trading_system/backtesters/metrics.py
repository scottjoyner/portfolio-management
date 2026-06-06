"""
Performance Metrics Calculator for Backtesting System

Calculates comprehensive performance statistics including:
- Total return and cumulative returns
- Annualized Sharpe ratio (risk-adjusted returns)
- Sortino ratio (downside deviation only)
- Maximum drawdown and drawdown duration
- Win rate and profit factor
- Calmar ratio (return / max drawdown)
- Value at Risk (VaR) and Conditional VaR

Usage:
    from backtesters.metrics import PerformanceMetrics
    
    metrics = PerformanceMetrics(
        portfolio_values=[10000, 10500, 9800, 10200],
        trade_results=[{"pnl": 500}, {"pnl": -700}, {"pnl": 400}],
        trading_days=252
    )
    
    print(f"Sharpe Ratio: {metrics.sharpe_ratio:.2f}")
    print(f"Max Drawdown: {metrics.max_drawdown_pct:.2f}%")

"""
from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime
import statistics


@dataclass 
class TradeResult:
    """Single trade result with PnL and timestamps."""
    entry_timestamp: float
    exit_timestamp: float
    pnl_usd: float
    pnl_pct: float
    entry_price: float
    exit_price: float


@dataclass  
class PortfolioValues:
    """Equity curve data for metrics calculation."""
    values: List[float]
    timestamps: List[float] = None  # Optional timestamp sequence


class PerformanceMetrics:
    """
    Comprehensive performance metrics calculator.
    
    Calculates all standard trading metrics plus advanced risk measures.
    Compatible with event-driven backtester output format.
    """
    
    def __init__(self, portfolio_values: List[float],
                 trade_results: Optional[List[TradeResult]] = None,
                 trading_days: int = 252,
                 initial_capital: float = 10000.0):
        """
        Initialize metrics calculator.
        
        Args:
            portfolio_values: Equity curve (portfolio value at each bar)
            trade_results: List of TradeResult objects for realized PnL analysis
            trading_days: Annual trading days (default 252 for daily bars)
            initial_capital: Starting account balance
        """
        self.portfolio_values = portfolio_values
        self.trade_results = trade_results or []
        self.trading_days = trading_days
        self.initial_capital = initial_capital
        
        # Pre-computed values (lazy calculated)
        _self_ = self
        _self_._total_return_cached = None
        _self_._annualized_return_cached = None
        _self_._sharpe_ratio_cached = None
        _self_._sortino_ratio_cached = None
        _self_._max_drawdown_cached = None
        _self_._calmar_ratio_cached = None
        
    @property
    def total_return_pct(self) -> float:
        """Total cumulative return as percentage from start to end."""
        if not self.portfolio_values or len(self.portfolio_values) < 2:
            return 0.0
            
        final_value = self.portfolio_values[-1]
        initial_value = self.portfolio_values[0]
        
        return ((final_value - initial_value) / initial_value) * 100
        
    @property
    def annualized_return_pct(self) -> float:
        """Annualized return using compound annual growth rate (CAGR)."""
        if not self.portfolio_values or len(self.portfolio_values) < 2:
            return 0.0
            
        initial = self.portfolio_values[0]
        final = self.portfolio_values[-1]
        
        # Estimate total period from values (assume daily data)
        if self.trading_days > 0:
            num_years = self.trading_days / 252.0  # Default to 1 year
        else:
            num_years = 1.0
            
        if initial <= 0 or final <= 0:
            return 0.0
            
        cagr = (final / initial) ** (1 / num_years) - 1
        return cagr * 100
        
    @property  
    def sharpe_ratio(self) -> float:
        """
        Annualized Sharpe ratio (risk-adjusted returns).
        
        Uses daily volatility, annualized. Assumes risk-free rate of 0 for simplicity.
        Returns None if not enough data points (<30 bars).
        """
        if len(self.portfolio_values) < 30:
            return None
            
        # Calculate daily returns from equity curve
        returns = []
        for i in range(1, len(self.portfolio_values)):
            prev_value = self.portfolio_values[i-1]
            current_value = self.portfolio_values[i]
            
            if prev_value > 0:
                daily_return = (current_value - prev_value) / prev_value
                returns.append(daily_return)
        
        if not returns or len(returns) < 30:
            return None
            
        # Calculate mean and standard deviation of daily returns
        mean_daily_return = statistics.mean(returns)
        std_daily_return = statistics.stdev(returns) if len(returns) > 1 else 0
        
        if std_daily_return == 0:
            return 0.0
            
        # Annualize (assuming 252 trading days)
        annualized_sharpe = (mean_daily_return / std_daily_return) * (252 ** 0.5)
        
        return annualized_sharpe
        
    @property
    def sortino_ratio(self) -> float:
        """
        Annualized Sortino ratio (risk-adjusted returns using downside deviation).
        
        Only considers downside volatility (returns below mean or zero threshold).
        More sensitive to drawdowns than Sharpe ratio.
        """
        if len(self.portfolio_values) < 30:
            return None
            
        # Calculate daily returns
        returns = []
        for i in range(1, len(self.portfolio_values)):
            prev_value = self.portfolio_values[i-1]
            current_value = self.portfolio_values[i]
            
            if prev_value > 0:
                daily_return = (current_value - prev_value) / prev_value
                returns.append(daily_return)
        
        if not returns or len(returns) < 30:
            return None
            
        # Calculate downside deviation (only negative or below-mean returns)
        mean_return = statistics.mean(returns)
        downside_returns = [r for r in returns if r < mean_return] if mean_return > 0 else \
                         [r for r in returns if r <= 0]
        
        if not downside_returns:
            return None
            
        downside_deviation = statistics.stdev(downside_returns)
        
        if downside_deviation == 0:
            return 0.0
            
        # Annualize
        mean_daily_return = mean_return * len(returns) / self.trading_days
        annualized_sortino = (mean_daily_return / downside_deviation) * (252 ** 0.5)
        
        return annualized_sortino
        
    @property
    def max_drawdown_pct(self) -> float:
        """
        Maximum drawdown percentage from peak to trough.
        
        Tracks rolling peak and calculates largest decline relative to that peak.
        Standard industry metric for risk assessment.
        """
        if not self.portfolio_values:
            return 0.0
            
        peak = self.portfolio_values[0]
        max_dd = 0.0
        
        for value in self.portfolio_values:
            if value > peak:
                peak = value
                
            dd = (peak - value) / peak * 100
            if dd > max_dd:
                max_dd = dd
                
        return max_dd
        
    @property
    def calmar_ratio(self) -> Optional[float]:
        """
        Calmar ratio (annualized return / maximum drawdown).
        
        Measures return per unit of downside risk.
        Higher is better; negative if portfolio underperforms peak.
        """
        max_dd = self.max_drawdown_pct
        
        if abs(max_dd) < 0.01:  # Near zero
            return None
            
        annualized_return = self.annualized_return_pct / 100
        
        return annualized_return / max(dd) if dd != 0 else 0.0
        
    @property
    def win_rate(self) -> Optional[float]:
        """
        Win rate from realized trades (if available).
        
        Returns None if no trade results provided.
        """
        if not self.trade_results:
            return None
            
        winning_trades = sum(1 for t in self.trade_results if t.pnl_usd > 0)
        total_trades = len(self.trade_results)
        
        if total_trades == 0:
            return 0.0
            
        return winning_trades / total_trades * 100
        
    @property
    def profit_factor(self) -> float:
        """
        Profit factor (gross profits / gross losses).
        
        Classic risk-reward metric. >1.0 profitable, <1.0 losing over sample period.
        Only uses realized trade PnLs.
        """
        if not self.trade_results:
            return 1.0  # Default to neutral
            
        gross_profits = sum(t.pnl_usd for t in self.trade_results if t.pnl_usd > 0)
        gross_losses = abs(sum(t.pnl_usd for t in self.trade_results if t.pnl_usd < 0))
        
        if gross_losses == 0:
            return float('inf') if gross_profits > 0 else 1.0
            
        return gross_profits / gross_losses
        
    @property
    def value_at_risk_95(self) -> Optional[float]:
        """
        95% Value at Risk (VaR) from equity curve.
        
        Estimates worst expected loss over single period with 95% confidence.
        Uses historical simulation method on daily returns.
        """
        if len(self.portfolio_values) < 30:
            return None
            
        # Calculate daily returns
        returns = []
        for i in range(1, len(self.portfolio_values)):
            prev_value = self.portfolio_values[i-1]
            current_value = self.portfolio_values[i]
            
            if prev_value > 0:
                daily_return = (current_value - prev_value) / prev_value
                returns.append(daily_return)
        
        if not returns or len(returns) < 30:
            return None
            
        # Sort returns and find 5th percentile (worst 5% of days)
        sorted_returns = sorted(returns)
        var_index = int(len(sorted_returns) * 0.05)
        if var_index >= len(sorted_returns):
            var_index = len(sorted_returns) - 1
            
        daily_var = sorted_returns[var_index]
        
        # Annualize using square root of time scaling
        annualized_var = abs(daily_var) * (self.trading_days ** 0.5)
        
        return annualized_var * self.initial_capital
        
    @property
    def conditional_var_95(self) -> Optional[float]:
        """
        95% Conditional VaR (Expected Shortfall).
        
        Average loss given that loss exceeds VaR threshold.
        More conservative risk measure than standard VaR.
        """
        if len(self.portfolio_values) < 30:
            return None
            
        returns = []
        for i in range(1, len(self.portfolio_values)):
            prev_value = self.portfolio_values[i-1]
            current_value = self.portfolio_values[i]
            
            if prev_value > 0:
                daily_return = (current_value - prev_value) / prev_value
                returns.append(daily_return)
        
        if not returns or len(returns) < 30:
            return None
            
        # Sort and find worst 5%
        sorted_returns = sorted(returns)
        var_index = int(len(sorted_returns) * 0.05)
        if var_index >= len(sorted_returns):
            var_index = len(sorted_returns) - 1
            
        # Average of all returns worse than VaR threshold
        tail_return = sorted_returns[var_index]
        tail_returns = [r for r in sorted_returns if r <= tail_return]
        
        if not tail_returns:
            return None
            
        expected_shortfall = abs(statistics.mean(tail_returns))
        annualized_es = expected_shortfall * (self.trading_days ** 0.5)
        
        return annualized_es * self.initial_capital
        
    def get_summary(self) -> dict:
        """
        Generate comprehensive performance summary.
        
        Returns all metrics in dictionary format for reporting/serialization.
        """
        return {
            "total_return_pct": self.total_return_pct,
            "annualized_return_pct": self.annualized_return_pct,
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "calmar_ratio": self.calmar_ratio,
            "max_drawdown_pct": self.max_drawdown_pct,
            "win_rate": self.win_rate,
            "profit_factor": self.profit_factor if isinstance(self.profit_factor, float) else round(self.profit_factor, 2),
            "var_95_usd": self.value_at_risk_95,
            "conditional_var_95_usd": self.conditional_var_95,
        }


class DrawdownMetrics:
    """
    Extended drawdown analysis (duration metrics).
    
    Tracks when drawdowns occur and how long they last.
    Useful for understanding persistence of losses.
    """
    
    def __init__(self, portfolio_values: List[float]):
        self.portfolio_values = portfolio_values
        
    @property
    def longest_drawdown_duration(self) -> Optional[int]:
        """Number of bars during which max drawdown occurred."""
        if not self.portfolio_values or len(self.portfolio_values) < 2:
            return None
            
        peak_index = 0
        current_dd = 0.0
        max_dd = 0.0
        max_dd_start = 0
        max_dd_end = 0
        
        for i, value in enumerate(self.portfolio_values):
            if value > self.portfolio_values[peak_index]:
                peak_index = i
                
            current_dd = (self.portfolio_values[peak_index] - value) / \
                        self.portfolio_values[peak_index] * 100
            
            if current_dd > max_dd:
                max_dd = current_dd
                max_dd_start = i
        
        # Find end of drawdown (recovery to peak or new peak)
        for i in range(max_dd_start + 1, len(self.portfolio_values)):
            value = self.portfolio_values[i]
            
            if value >= self.portfolio_values[peak_index]:
                return i - max_dd_start
                
        return len(self.portfolio_values) - max_dd_start
        
    def get_drawdown_periods(self) -> List[dict]:
        """
        Identify all significant drawdown periods (>2% drawdown).
        
        Returns list of dicts with: peak_value, trough_value, duration_bars, 
        max_dd_pct, recovery_timestamp (optional if still in drawdown)
        """
        if not self.portfolio_values or len(self.portfolio_values) < 2:
            return []
            
        periods = []
        
        peak_index = 0
        peak_value = self.portfolio_values[0]
        current_dd_start = -1
        
        for i, value in enumerate(self.portfolio_values):
            # Update peak if current price exceeds it
            if value > peak_value:
                # If we were in a drawdown, save it before updating peak
                if current_dd_start >= 0 and (peak_value - self.portfolio_values[current_dd_start]) / \
                    peak_value * 100 >= 2.0:
                    periods.append({
                        "peak_value": peak_value,
                        "trough_value": self.portfolio_values[current_dd_start],
                        "max_drawdown_pct": (peak_value - self.portfolio_values[current_dd_start]) / \
                                          peak_value * 100,
                        "duration_bars": i - current_dd_start,
                    })
                    
                peak_index = i
                peak_value = value
                current_dd_start = -1
            elif (peak_value - value) / peak_value * 100 >= 2.0:
                # Still above 2% drawdown threshold
                if current_dd_start == -1:
                    current_dd_start = i
        
        # Handle final period if still in drawdown
        if current_dd_start >= 0 and (peak_value - self.portfolio_values[-1]) / \
            peak_value * 100 >= 2.0:
            periods.append({
                "peak_value": peak_value,
                "trough_value": self.portfolio_values[-1],
                "max_drawdown_pct": (peak_value - self.portfolio_values[-1]) / \
                                  peak_value * 100,
                "duration_bars": len(self.portfolio_values) - current_dd_start,
            })
            
        return periods


if __name__ == "__main__":
    # Example usage with mock equity curve
    test_equity_curve = [
        10000, 10500, 10200, 10300, 10800, 10600, 
        10900, 10700, 11200, 11000, 11500, 11300,
        11800, 11600, 12000, 11900, 12400, 12200,
    ]
    
    metrics = PerformanceMetrics(
        portfolio_values=test_equity_curve,
        trading_days=252,
        initial_capital=10000.0
    )
    
    summary = metrics.get_summary()
    
    print("Performance Metrics Summary:")
    print(f"  Total Return: {summary['total_return_pct']:.2f}%")
    print(f"  Annualized Return: {summary['annualized_return_pct']:.2f}%")
    if summary['sharpe_ratio'] is not None:
        print(f"  Sharpe Ratio: {summary['sharpe_ratio']:.2f}")
    if summary['max_drawdown_pct'] is not None:
        print(f"  Max Drawdown: {summary['max_drawdown_pct']:.2f}%")
