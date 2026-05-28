"""Performance metrics calculations for backtesting."""

import numpy as np
from typing import List


def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.02) -> float:
    """Calculate annualized Sharpe ratio.
    
    Args:
        returns: List of daily return percentages
        risk_free_rate: Annual risk-free rate (default 2%)
        
    Returns:
        Annualized Sharpe ratio rounded to 2 decimals
    """
    if not returns or len(returns) < 2:
        return 0.0
    
    daily_returns = [r / 100 for r in returns]
    annualized_return = np.mean(daily_returns) * 252
    annualized_volatility = np.std(daily_returns) * np.sqrt(252)
    
    if annualized_volatility == 0:
        return 0.0
    
    sharpe = (annualized_return - risk_free_rate) / annualized_volatility
    return round(sharpe, 2)


def calculate_max_drawdown(equity_curve: List[float]) -> float:
    """Calculate maximum drawdown from equity curve."""
    if not equity_curve or len(equity_curve) < 2:
        return 0.0
    
    peak = equity_curve[0]
    max_dd_pct = 0.0
    
    for value in equity_curve:
        if value > peak:
            peak = value
        drawdown_pct = (peak - value) / peak * 100 if peak > 0 else 0
        max_dd_pct = max(max_dd_pct, drawdown_pct)
    
    return round(max_dd_pct, 2)


def calculate_sortino_ratio(returns: List[float], risk_free_rate: float = 0.02) -> float:
    """Calculate Sortino ratio using downside deviation."""
    if not returns or len(returns) < 2:
        return 0.0
    
    daily_returns = [r / 100 for r in returns]
    negative_returns = [r for r in daily_returns if r < 0]
    
    if not negative_returns:
        return 0.0
    
    downside_std = np.std(negative_returns) * np.sqrt(252/2)
    annualized_return = np.mean(daily_returns) * 252
    
    if downside_std == 0:
        return 0.0
    
    sortino = (annualized_return - risk_free_rate) / downside_std
    return round(sortino, 2)


def calculate_profit_factor(profit_trades: List[float], loss_trades: List[float]) -> float:
    """Calculate profit factor (gross profits / gross losses)."""
    if not profit_trades or not loss_trades:
        return 1.0
    
    total_profits = sum(profit_trades)
    total_losses = abs(sum(loss_trades))
    
    if total_losses == 0:
        return float('inf')
    
    pf = total_profits / total_losses
    return round(pf, 2)


def calculate_win_rate(winning_trades: int, total_trades: int) -> float:
    """Calculate win rate percentage."""
    if total_trades == 0:
        return 0.0
    
    wr = (winning_trades / total_trades) * 100
    return round(wr, 1)


def calculate_avg_profit_factor(profit_amounts: List[float], loss_amounts: List[float]) -> float:
    """Calculate average profit factor across trades."""
    if not profit_amounts or not loss_amounts:
        return 1.0
    
    total_pnl = sum(abs(a) for a in profit_amounts + loss_amounts)
    if total_pnl == 0:
        return 0.0
    
    return round(total_pnl / len(profit_amounts + loss_amounts), 2)


__all__ = [
    "calculate_sharpe_ratio",
    "calculate_max_drawdown",
    "calculate_sortino_ratio",
    "calculate_profit_factor",
    "calculate_win_rate",
    "calculate_avg_profit_factor"
]
