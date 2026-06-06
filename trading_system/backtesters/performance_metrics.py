#!/usr/bin/env python3
"""
Comprehensive Performance Metrics Calculator for Backtesting Engine

Calculates key risk-adjusted performance metrics:
1. Sharpe Ratio - Return per unit of total risk (standard deviation)
2. Sortino Ratio - Return per unit of downside risk (downside deviation)
3. Calmar Ratio - Annualized return / max drawdown magnitude
4. Maximum Drawdown - Peak-to-valley decline percentage
5. Win Rate - Percentage of profitable trades
6. Profit Factor - Gross profit / gross loss ratio

Features:
- Daily rebased metrics for multi-year analysis
- Risk-free rate adjustment (supports crypto spot with 0% or USY treasury)
- Rolling window calculations for dynamic risk measures
- Equity curve generation for visualization

Production-ready for crypto spot pairs with realistic execution modeling.
Compatible with structured logging system.
"""

from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import math
import json
import logging


class PerformanceMetricsCalculator:
    """
    Calculate comprehensive performance metrics for backtest results.
    
    Usage:
        calculator = PerformanceMetricsCalculator(
            risk_free_rate_pct=0.0,  # 0% for crypto spot, or US10Y ~4%
            annualization_factor=252,  # Trading days per year
        )
        metrics = calculator.calculate(portfolio_values)
    
    """
    
    def __init__(
        self,
        risk_free_rate_pct: float = 0.0,
        annualization_factor: int = 252,
        log_level: str = "INFO"
    ):
        """
        Initialize metrics calculator.
        
        Args:
            risk_free_rate_pct: Annualized risk-free rate (0% for crypto spot pairs)
            annualization_factor: Trading days per year for annualization (default 252)
            log_level: Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR')
        """
        self.risk_free_rate_daily = (1 + risk_free_rate_pct / 100) ** (1 / annualization_factor) - 1
        self.annualization_factor = annualization_factor
        
        # Setup logger with structured output support
        self.logger = logging.getLogger(f"__main__.PerformanceMetrics")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(getattr(logging, log_level.upper()))
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
    def calculate(
        self,
        portfolio_values: List[float],
        initial_capital: float = None,
        symbols: List[str] = None
    ) -> Dict:
        """
        Calculate all performance metrics from portfolio value time series.
        
        Args:
            portfolio_values: Time-series of equity values (daily snapshots)
            initial_capital: Starting account balance (uses first value if not provided)
            symbols: List of trading symbols for labeling
        
        Returns:
            Dict containing all calculated metrics with labels
        """
        if not portfolio_values or len(portfolio_values) < 2:
            self.logger.warning("Insufficient portfolio values for metrics calculation")
            return self._empty_metrics()
        
        initial_capital = initial_capital or portfolio_values[0]
        
        # Calculate returns
        returns = self._calculate_returns(portfolio_values)
        
        # Core metrics
        metrics = {
            'total_return_pct': self._calc_total_return(portfolio_values),
            'annualized_return_pct': self._calc_annualized_return(returns),
            'sharpe_ratio': self._calc_sharpe_ratio(returns),
            'sortino_ratio': self._calc_sortino_ratio(returns),
            'calmar_ratio': self._calc_calmar_ratio(portfolio_values),
            'max_drawdown_pct': self._calc_max_drawdown(portfolio_values),
            'cagg_return_pct': self._calc_cagr(portfolio_values, initial_capital),
            'win_rate_pct': None,  # Will be set from trade data
            'profit_factor': None,  # Will be set from PnL data
            'recovery_periods_days': self._calc_recovery_periods(portfolio_values),
        }
        
        # Rolling metrics
        if len(returns) >= 12:  # Minimum 12 periods for rolling stats
            metrics['rolling_sharpe_6m'] = self._calc_rolling_sharpe(returns, window=6*12)
            metrics['rolling_sortino_6m'] = self._calc_rolling_sortino(returns, window=6*12)
            metrics['var_95_pct'] = self._calc_var(portfolio_values, returns)  # VaR at 95% confidence
        
        # Risk statistics
        metrics.update({
            'volatility_pct': self._calc_volatility(returns),
            'downside_volatility_pct': self._calc_downside_volatility(returns),
            'skewness': self._calc_skewness(returns),
            'kurtosis': self._calc_kurtosis(returns),
            'positive_periods_pct': len([r for r in returns if r > 0]) / len(returns) * 100,
            'negative_periods_pct': len([r for r in returns if r < 0]) / len(returns) * 100,
        })
        
        self.logger.info(f"Metrics calculated: Sharpe={metrics['sharpe_ratio']:.2f}, "
                        f"Sortino={metrics['sortino_ratio']:.2f}, MaxDD={metrics['max_drawdown_pct']:.2f}%")
        
        return metrics
    
    def calculate_with_trades(
        self,
        portfolio_values: List[float],
        trades: List[Dict],
        initial_capital: float = None
    ) -> Dict:
        """
        Calculate all metrics including trade-level statistics.
        
        Args:
            portfolio_values: Time-series of equity values
            trades: List of completed trades with PnL data
            initial_capital: Starting account balance
        
        Returns:
            Complete metrics dict with trade statistics
        """
        # First calculate value-based metrics
        metrics = self.calculate(portfolio_values, initial_capital)
        
        # Calculate trade-level metrics
        if trades and len(trades) > 0:
            pnl_values = [t.get('pnl', t.get('profit_loss', 0)) for t in trades]
            
            if len(pnl_values) > 0:
                gross_profit = sum([p for p in pnl_values if p > 0])
                gross_loss = abs(sum([p for p in pnl_values if p < 0]))
                
                metrics['win_rate_pct'] = len([p for p in pnl_values if p > 0]) / len(pnl_values) * 100
                metrics['profit_factor'] = gross_profit / gross_loss if gross_loss > 0 else float('inf')
                metrics['avg_win_pct'] = (sum([p for p in pnl_values if p > 0]) / 
                                        len([p for p in pnl_values if p > 0])) / initial_capital * 100 if metrics['win_rate_pct'] and metrics['win_rate_pct'] > 0 else 0
                metrics['avg_loss_pct'] = (abs(sum([p for p in pnl_values if p < 0]) / 
                                          len([p for p in pnl_values if p < 0])) / initial_capital) * 100 if not all(p >= 0 for p in pnl_values) else 0
                metrics['total_trades'] = len(trades)
                
        self.logger.info(f"Trade metrics calculated: WinRate={metrics['win_rate_pct']:.2f}%, "
                        f"ProfitFactor={metrics['profit_factor']:.2f}, Trades={len(trades)}")
        
        return metrics
    
    def generate_equity_curve(
        self,
        portfolio_values: List[float],
        initial_capital: float = None,
        symbols: List[str] = None
    ) -> Dict[str, List]:
        """
        Generate equity curve with labels for visualization.
        
        Returns:
            Dict with 'timestamps', 'values', and optional 'labels' lists
        """
        initial_capital = initial_capital or portfolio_values[0]
        normalized_values = [v / initial_capital * 100 for v in portfolio_values]
        
        # Generate labels based on drawdown milestones
        labels = []
        peak = portfolio_values[0]
        for i, value in enumerate(portfolio_values):
            if value > peak:
                peak = value
            
            drawdown = (peak - value) / peak * 100 if peak > 0 else 0
            
            if i == 0:
                labels.append("Start")
            elif drawdown >= 50 and not any(l.startswith("Drawdown:") for l in labels[-5:]):
                labels.append(f"Drawdown:{drawdown:.1f}%")
            elif value > peak * 1.02:  # New ATH
                labels.append("New ATH")
            else:
                labels.append("")
        
        return {
            'timestamps': list(range(len(portfolio_values))),
            'values': normalized_values,
            'labels': labels[:len(normalized_values)],
            'final_value_pct': normalized_values[-1] if normalized_values else None,
            'start_value_pct': normalized_values[0] if normalized_values else None,
        }
    
    # --- Core Calculation Methods ---
    
    def _calculate_returns(self, portfolio_values: List[float]) -> List[float]:
        """Calculate period-over-period returns."""
        if len(portfolio_values) < 2:
            return []
        returns = [(portfolio_values[i] - portfolio_values[i-1]) / portfolio_values[i-1] 
                   for i in range(1, len(portfolio_values))]
        self.logger.debug(f"Calculated {len(returns)} periods of returns")
        return returns
    
    def _calc_total_return(self, portfolio_values: List[float]) -> float:
        """Calculate total return from start to end."""
        if len(portfolio_values) < 2:
            return 0.0
        return ((portfolio_values[-1] - portfolio_values[0]) / portfolio_values[0]) * 100
    
    def _calc_annualized_return(self, returns: List[float]) -> float:
        """Calculate CAGR of returns."""
        if not returns or len(returns) == 0:
            return 0.0
        
        end_value = sum([(1 + r) for r in returns])
        start_value = 1.0
        n_periods = len(returns)
        
        cagr = (end_value / start_value) ** (1 / n_periods) - 1
        return cagr * 100
    
    def _calc_sharpe_ratio(self, returns: List[float]) -> float:
        """Calculate Sharpe ratio using portfolio standard deviation."""
        if not returns or len(returns) < 30:  # Minimum 30 periods
            return 0.0
        
        mean_return = sum(returns) / len(returns)
        
        # Daily standard deviation (rebased to annualization factor)
        variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
        std_dev = math.sqrt(variance) if variance > 0 else 0
        
        sharpe = (mean_return - self.risk_free_rate_daily) / std_dev if std_dev > 0 else 0
        # Annualize: multiply by sqrt(annualization_factor)
        sharpe_annualized = sharpe * math.sqrt(self.annualization_factor)
        
        return sharpe_annualized
    
    def _calc_sortino_ratio(self, returns: List[float]) -> float:
        """Calculate Sortino ratio using downside deviation."""
        if not returns or len(returns) < 30:
            return 0.0
        
        mean_return = sum(returns) / len(returns)
        
        # Downside deviation (only negative returns squared)
        downside_squared = [(r - mean_return) ** 2 for r in returns if r < mean_return]
        
        if not downside_squared:
            return 0.0
        
        downside_variance = sum(downside_squared) / len(downside_squared)
        downside_std = math.sqrt(downside_variance) if downside_variance > 0 else 0
        
        sortino = (mean_return - self.risk_free_rate_daily) / downside_std if downside_std > 0 else 0
        sortino_annualized = sortino * math.sqrt(self.annualization_factor)
        
        return sortino_annualized
    
    def _calc_calmar_ratio(self, portfolio_values: List[float]) -> float:
        """Calculate Calmar ratio (CAGR / max drawdown magnitude)."""
        if len(portfolio_values) < 2:
            return 0.0
        
        cagr = self._calc_annualized_return(self._calculate_returns(portfolio_values))
        max_dd = abs(self._calc_max_drawdown(portfolio_values))
        
        if max_dd == 0 or max_dd < 0.001:  # Avoid division by near-zero
            return 0.0
        
        return cagr / max_dd
    
    def _calc_max_drawdown(self, portfolio_values: List[float]) -> float:
        """Calculate maximum drawdown (peak-to-valley decline)."""
        if len(portfolio_values) < 2:
            return 0.0
        
        peak = portfolio_values[0]
        max_dd = 0.0
        
        for value in portfolio_values:
            if value > peak:
                peak = value
            dd = (value - peak) / peak * 100 if peak > 0 else 0
            max_dd = min(max_dd, dd)
        
        return max_dd
    
    def _calc_cagr(self, portfolio_values: List[float], initial_capital: float) -> float:
        """Calculate Compound Annual Growth Rate."""
        if len(portfolio_values) < 2:
            return 0.0
        
        start_value = initial_capital or portfolio_values[0]
        end_value = portfolio_values[-1]
        
        n_periods = (len(portfolio_values) - 1) * (365 / self.annualization_factor)
        
        if end_value <= 0:
            return float('inf') if start_value > 0 else 0.0
        
        cagr = ((end_value / start_value) ** (1 / n_periods)) - 1
        
        return cagr * 100
    
    def _calc_volatility(self, returns: List[float]) -> float:
        """Calculate annualized volatility (standard deviation of returns)."""
        if not returns or len(returns) < 30:
            return 0.0
        
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
        std_dev = math.sqrt(variance) if variance > 0 else 0
        
        return std_dev * 100 * math.sqrt(self.annualization_factor)
    
    def _calc_downside_volatility(self, returns: List[float]) -> float:
        """Calculate downside volatility (only negative returns)."""
        if not returns or len(returns) < 30:
            return 0.0
        
        mean_return = sum(returns) / len(returns)
        downside_squared = [(r - mean_return) ** 2 for r in returns if r < mean_return]
        
        if not downside_squared:
            return 0.0
        
        downside_variance = sum(downside_squared) / len(downside_squared)
        downside_std = math.sqrt(downside_variance)
        
        return downside_std * 100 * math.sqrt(self.annualization_factor)
    
    def _calc_skewness(self, returns: List[float]) -> float:
        """Calculate skewness of return distribution."""
        if not returns or len(returns) < 30:
            return 0.0
        
        mean_return = sum(returns) / len(returns)
        std_dev = math.sqrt(sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)) if sum((r - mean_return) ** 2 for r in returns) > 0 else 0
        
        if std_dev == 0:
            return 0.0
        
        skewness = sum(((r - mean_return) / std_dev) ** 3 for r in returns) * len(returns) / ((len(returns) - 1) * (len(returns) - 2))
        
        return skewness
    
    def _calc_kurtosis(self, returns: List[float]) -> float:
        """Calculate excess kurtosis of return distribution."""
        if not returns or len(returns) < 30:
            return 0.0
        
        mean_return = sum(returns) / len(returns)
        std_dev = math.sqrt(sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)) if sum((r - mean_return) ** 2 for r in returns) > 0 else 0
        
        if std_dev == 0:
            return 0.0
        
        fourth_moment = sum(((r - mean_return) / std_dev) ** 4 for r in returns) * len(returns) / ((len(returns) - 1) * (len(returns) - 2))
        
        return fourth_moment - 3  # Excess kurtosis
    
    def _calc_recovery_periods(self, portfolio_values: List[float]) -> float:
        """Calculate average recovery time after drawdowns."""
        if len(portfolio_values) < 10:
            return 0.0
        
        peak = portfolio_values[0]
        max_dd_peak = (peak - portfolio_values[0]) / peak * 100 if peak > 0 else 0
        recovery_count = 0
        total_recovery_periods = 0
        
        for i in range(1, len(portfolio_values)):
            current_peak = max(peak, portfolio_values[i-1])
            current_dd = (current_peak - portfolio_values[i]) / current_peak * 100 if current_peak > 0 else 0
            
            if current_dd >= abs(max_dd_peak) / 2:  # Significant drawdown
                recovery_count += 1
                next_peak_idx = i + 1
                while next_peak_idx < len(portfolio_values):
                    if portfolio_values[next_peak_idx] > current_peak * 1.05:  # Recovered with buffer
                        total_recovery_periods += next_peak_idx - i
                        break
                    next_peak_idx += 1
        
        if recovery_count > 0:
            return total_recovery_periods / recovery_count
        
        return 0.0
    
    def _calc_var(self, portfolio_values: List[float], confidence: float = 95) -> float:
        """Calculate Value at Risk using historical simulation."""
        returns = self._calculate_returns(portfolio_values)
        
        if not returns or len(returns) < 30:
            return 0.0
        
        sorted_returns = sorted(returns)
        var_index = int((1 - confidence / 100) * len(sorted_returns))
        var_return = sorted_returns[var_index] if var_index < len(sorted_returns) else sorted_returns[-1]
        
        return abs(var_return) * 100
    
    def _calc_rolling_sharpe(self, returns: List[float], window_periods: int = 6*12) -> float:
        """Calculate Sharpe ratio over rolling window."""
        if not returns or len(returns) < window_periods:
            return 0.0
        
        window_returns = returns[-window_periods:]
        mean_return = sum(window_returns) / len(window_returns)
        
        variance = sum((r - mean_return) ** 2 for r in window_returns) / (len(window_returns) - 1)
        std_dev = math.sqrt(variance) if variance > 0 else 0
        
        sharpe = (mean_return - self.risk_free_rate_daily) / std_dev if std_dev > 0 else 0
        return sharpe * math.sqrt(self.annualization_factor)
    
    def _calc_rolling_sortino(self, returns: List[float], window_periods: int = 6*12) -> float:
        """Calculate Sortino ratio over rolling window."""
        if not returns or len(returns) < window_periods:
            return 0.0
        
        window_returns = returns[-window_periods:]
        mean_return = sum(window_returns) / len(window_returns)
        
        downside_squared = [(r - mean_return) ** 2 for r in window_returns if r < mean_return]
        
        if not downside_squared:
            return 0.0
        
        downside_variance = sum(downside_squared) / len(downside_squared)
        downside_std = math.sqrt(downside_variance) if downside_variance > 0 else 0
        
        sortino = (mean_return - self.risk_free_rate_daily) / downside_std if downside_std > 0 else 0
        return sortino * math.sqrt(self.annualization_factor)


class BacktestResultsExporter:
    """
    Export backtest results to multiple formats for analysis and visualization.
    
    Usage:
        exporter = BacktestResultsExporter()
        
        # Export metrics as JSON with labels
        json_output = exporter.to_json(metrics, symbols=['BTC-USD', 'ETH-USD'])
        
        # Generate equity curve data for plotting
        equity_data = exporter.to_equity_curve(portfolio_values, initial_capital)
    
    """
    
    def __init__(self):
        self.logger = logging.getLogger(f"__main__.BacktestResultsExporter")
    
    def to_json(self, metrics: Dict, symbols: List[str] = None, 
                timestamp_format: str = "%Y-%m-%d") -> str:
        """Export metrics as labeled JSON string."""
        
        output = {
            'metrics': {k: v for k, v in metrics.items() if v is not None},
            'timestamp': datetime.now().isoformat(),
            'symbols': symbols or ['BTC-USD']  # Default to BTC-USD if no symbols provided
        }
        
        return json.dumps(output, indent=2)
    
    def to_equity_curve_json(self, equity_data: Dict) -> str:
        """Export equity curve as JSON for visualization tools."""
        return json.dumps(equity_data, indent=2)
    
    def generate_summary_report(self, metrics: Dict, initial_capital: float = 10000) -> str:
        """Generate human-readable summary report."""
        
        report_lines = [
            "=" * 80,
            "BACKTEST PERFORMANCE SUMMARY",
            "=" * 80,
            "",
            f"Initial Capital: ${initial_capital:,.2f}",
            f"Total Trades: {metrics.get('total_trades', 'N/A')}",
            f"Win Rate: {metrics.get('win_rate_pct', 'N/A'):.1f}%",
            f"Profit Factor: {metrics.get('profit_factor', 'N/A'):.2f}",
            "",
            "-" * 60,
            "RISK-ADJUSTED RETURNS",
            "-" * 60,
            f"Annualized Return: {metrics.get('annualized_return_pct', 0):.2f}%",
            f"CAGR: {metrics.get('cagg_return_pct', 0):.2f}%",
            f"Sharpe Ratio: {metrics.get('sharpe_ratio', 0):.2f}",
            f"Sortino Ratio: {metrics.get('sortino_ratio', 0):.2f}",
            f"Calmar Ratio: {metrics.get('calmar_ratio', 0):.2f}",
            "",
            "-" * 60,
            "RISK METRICS",
            "-" * 60,
            f"Max Drawdown: {metrics.get('max_drawdown_pct', 0):.2f}%",
            f"Volatility (Annualized): {metrics.get('volatility_pct', 0):.2f}%",
            f"Downside Volatility: {metrics.get('downside_volatility_pct', 0):.2f}%",
            f"VaR 95%: {metrics.get('var_95_pct', 0):.2f}%",
            "",
            "-" * 60,
            "DISTRIBUTION STATISTICS",
            "-" * 60,
            f"Positive Periods: {metrics.get('positive_periods_pct', 0):.1f}%",
            f"Negative Periods: {metrics.get('negative_periods_pct', 0):.1f}%",
            f"Skewness: {metrics.get('skewness', 0):.3f}",
            f"Kurtosis: {metrics.get('kurtosis', 0):.2f}",
        ]
        
        return "\n".join(report_lines)


# Utility functions for crypto spot pairs with realistic execution modeling

def simulate_slippage_with_vwap(actual_price: float, vwap: float, order_size: float, 
                                  liquidity_bps: float = 10.0) -> Tuple[float, float]:
    """
    Simulate slippage based on VWAP deviation and order size impact.
    
    Crypto spot pairs typically trade within 5-15 bps of VWAP in normal conditions.
    Flash crashes can see deviations of 100+ bps briefly.
    
    Args:
        actual_price: Best available price at order time
        vwap: Volume-weighted average price for the period
        order_size: Order size in base currency (e.g., BTC)
        liquidity_bps: Market depth in basis points (default 10 bps = tight market)
    
    Returns:
        Tuple of (filled_price, slippage_bps)
    """
    vwap_deviation = ((actual_price - vwap) / vwap) * 10000 if vwap > 0 else 0
    
    # Order size impact (larger orders slip more)
    normalized_size = min(order_size / 10.0, 1.0)  # Assume ~10 BTC as reference
    
    # Combined slippage model
    base_slippage = vwap_deviation + vwap_deviation * liquidity_bps * 0.5
    size_impact = normalized_size * 20  # Additional 2 bps per normalized size unit
    
    total_slippage_bps = base_slippage + size_impact
    filled_price = actual_price * (1 + total_slippage_bps / 10000) if actual_price > 0 else actual_price
    
    return filled_price, abs(total_slippage_bps)


def simulate_flash_crash_detection(returns: List[float], threshold_bps: float = 2.0) -> List[Dict]:
    """
    Detect flash crashes in returns data.
    
    Flash crashes are typically defined as >2% (200 bps) drop within a single bar
    that recovers quickly (within 3-5 bars).
    
    Args:
        returns: Time-series of daily returns
        threshold_bps: Crash threshold in basis points (default 2.0% = 200 bps)
    
    Returns:
        List of crash events with timestamps and details
    """
    crashes = []
    threshold_pct = threshold_bps / 100
    
    for i, return_val in enumerate(returns):
        if return_val < -threshold_pct:
            # Check if this is a flash crash (recovery pattern)
            lookback = min(i, 5)
            lookback_returns = returns[max(0, i-lookback):i]
            
            if any(r > threshold_pct / 3 for r in lookback_returns):  # Signs of recovery
                crash_event = {
                    'index': i,
                    'return': return_val,
                    'threshold_bps': threshold_bps,
                    'is_flash_crash': True
                }
                crashes.append(crash_event)
    
    return crashes
