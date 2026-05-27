"""
Backtesting Framework Module

Provides event-driven backtesting engine for strategy evaluation and performance metrics.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Optional, List, Tuple

try:
    from trading_system.strategies.base import OHLCVBar
except ImportError:
    @dataclass
    class OHLCVBar:
        timestamp: int
        open: Optional[float] = None
        high: Optional[float] = None
        low: Optional[float] = None
        close: Optional[float] = None
        volume: Optional[float] = None


logger = logging.getLogger(__name__)


@dataclass
class Transaction:
    """Record a trade execution."""
    
    timestamp: int
    price: float
    quantity: int
    position_before: int
    position_after: int
    slippage_bps: float = 0.0
    commission_cents: float = 0.0


@dataclass 
class PerformanceMetrics:
    """Performance metrics from backtest."""
    
    total_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0


@dataclass 
class BacktestResult:
    """Complete backtest result."""
    
    strategy_key: str
    initial_capital: float
    final_balance: float
    total_return: float
    metrics: PerformanceMetrics
    transactions: List[Transaction] = field(default_factory=list)
    equity_curve: List[Tuple[int, float]] = field(default_factory=list)


class BacktestEngine:
    """
    Event-driven backtesting engine.
    
    Example usage:
        def my_strategy(bar):
            return True, bar.close
        
        engine = BacktestEngine(my_strategy)
        ohlcv_data = [...]  # list of OHLCVBar objects
        results = engine.run_backtest(ohlcv_data, initial_capital=10000)
    """
    
    def __init__(self, strategy: Callable[[OHLCVBar], Tuple[Optional[bool], Optional[float]]]):
        self.strategy = strategy
        self.initial_capital: float = 10000.0
        self.slippage_bps: float = 5.0
        self.commission_cents: float = 0.10
    
    def run_backtest(
        self,
        ohlcv_data: Sequence[OHLCVBar],
        initial_capital: Optional[float] = None,
        slippage_bps: Optional[float] = None,
        commission_cents: Optional[float] = None,
    ) -> BacktestResult:
        self.initial_capital = initial_capital or self.initial_capital
        
        if slippage_bps:
            self.slippage_bps = slippage_bps
        
        if commission_cents:
            self.commission_cents = commission_cents
        
        equity_curve: List[Tuple[int, float]] = []
        unrealized_pnl: float = 0.0
        quantity: int = 0
        entry_price: float = 0.0
        
        total_trades: int = 0
        winning_trades: int = 0
        win_amounts: List[float] = []
        loss_amounts: List[float] = []
        
        # Process each bar
        for i, bar in enumerate(ohlcv_data):
            if quantity > 0 and bar.close:
                unrealized_pnl = (bar.close - entry_price) * quantity
            
            current_equity = self.initial_capital + unrealized_pnl
            equity_curve.append((i, current_equity))
            
            signal, entry_price = self.strategy(bar)
            
            if signal is True:
                buy_price = bar.close * (1 + self.slippage_bps / 10000)
                position_value = current_equity * 0.01
                quantity = int(position_value / buy_price)
                
                if quantity > 0:
                    total_trades += 1
                    commission = quantity * buy_price * self.commission_cents / 100
                    
                    transactions.append(Transaction(
                        timestamp=bar.timestamp,
                        price=buy_price,
                        quantity=quantity,
                        position_before=0,
                        position_after=quantity,
                        slippage_bps=self.slippage_bps,
                        commission_cents=commission,
                    ))
            
            elif signal is False:
                if quantity > 0:
                    sell_price = bar.close * (1 - self.slippage_bps / 10000)
                    realized_pnl = (sell_price - entry_price) * quantity
                    
                    win_amounts.append(realized_pnl if realized_pnl > 0 else float('nan'))
                    
                    total_trades += 1
                    quantity = 0
                    unrealized_pnl = 0.0
        
        final_equity = self.initial_capital + unrealized_pnl
        
        if total_trades > 0:
            win_amounts_clean = [x for x in win_amounts if not math.isnan(x)]
            loss_amounts_clean = [x for x in win_amounts if not math.isnan(x) and x < 0]
            
            metrics = PerformanceMetrics(
                total_return=(final_equity - self.initial_capital) / self.initial_capital,
                sharpe_ratio=0.0,
                max_drawdown=0.0,
                win_rate=len(win_amounts_clean) / total_trades,
                avg_win=sum(win_amounts_clean) / len(win_amounts_clean) if win_amounts_clean else 0.0,
                avg_loss=sum(loss_amounts_clean) / len(loss_amounts_clean) if loss_amounts_clean else 0.0,
                profit_factor=abs(sum(win_amounts_clean)) / abs(sum(loss_amounts_clean)) if loss_amounts_clean else float('nan'),
                total_trades=total_trades,
                winning_trades=len(win_amounts_clean),
            )
        else:
            metrics = PerformanceMetrics(total_return=(final_equity - self.initial_capital) / self.initial_capital)
        
        return BacktestResult(
            strategy_key="ema_crossover",
            initial_capital=self.initial_capital,
            final_balance=final_equity,
            total_return=metrics.total_return,
            metrics=metrics,
            transactions=transactions,
            equity_curve=equity_curve,
        )


class OHLCVDataLoader:
    """Placeholder for exchange data loader."""
    
    def __init__(self, symbol: str, timeframe: str = "1h"):
        self.symbol = symbol
        self.timeframe = timeframe
    
    def fetch_all(self, limit: int = 1000) -> List[OHLCVBar]:
        logger.info(f"Would fetch {limit} bars of {self.symbol} {self.timeframe} data")
        return []
