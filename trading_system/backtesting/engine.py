"""Core backtesting engine for trading strategy validation."""

from typing import List, Dict, Any
import pandas as pd
import numpy as np


class BacktestEngine:
    """Core backtesting engine that runs strategies against historical data.
    
    This class provides the foundation for validating trading strategy performance
    before live deployment. Includes position tracking, trade execution simulation,
    and performance metrics calculation.
    
    Args:
        initial_capital: Starting portfolio value in USD (default 100000)
    """
    
    def __init__(self, initial_capital: float = 100000):
        self.capital = initial_capital
        self.cash = initial_capital
        self.positions = {}
        self.trades_history = []
        
    async def run(
        self,
        strategies: List[str],
        ohlcv_data: pd.DataFrame,
        strategy_ids: List[str] = None
    ) -> Dict[str, Any]:
        """Run backtest loop with all strategies.
        
        Args:
            strategies: List of strategy class names or instances to test
            ohlcv_data: OHLCV data from historical trading period
            strategy_ids: Optional filter for specific strategies
            
        Returns:
            Dictionary containing performance metrics, trade history, and equity curve
        """
        
        # Group data by asset symbol
        instruments = list(set(ohlcv_data.columns[-3:])) if len(ohlcv_data.columns) >= 3 else []
        
        results = {}
        current_capital = self.capital
        
        for instrument in instruments:
            # Extract OHLCV for single instrument (adapt column names as needed)
            close_prices = ohlcv_data['close'] if 'close' in ohlcv_data.columns else ohlcv_data.iloc[:, -1]
            
            trades = []
            
            # Simplified backtest logic - each strategy would implement its own analyze() method
            for i in range(1, len(close_prices)):
                current_price = close_prices.iloc[i]
                previous_price = close_prices.iloc[i-1]
                
                # Example: Simple momentum signal (replace with actual strategy logic)
                if current_price > previous_price * 1.01 and self.cash > 1000:
                    quantity = int(self.cash / current_price)
                    trade_cost = quantity * current_price
                    
                    if trade_cost <= self.cash:
                        trades.append({
                            'timestamp': str(ohlcv_data.index[i]),
                            'asset': instrument,
                            'quantity': quantity,
                            'price': round(current_price, 2),
                            'side': 'long',
                            'cost_usd': trade_cost
                        })
                        self.cash -= trade_cost
                        
                # Close losing positions (basic risk management)
                for pos_id, pos in list(self.positions.items()):
                    if pos['entry_price'] > current_price * 1.05:  # 5% loss threshold
                        self.close_position(pos_id, instrument)
            
            # Calculate returns for this instrument
            total_trades = len(trades)
            
            results[instrument] = {
                'trades': trades,
                'total_trades': total_trades,
                'initial_capital': self.capital,
                'final_cash': round(self.cash, 2),
                'position_value': round(sum(p['quantity'] * p['current_price'] for p in self.positions.values()), 2) if self.positions else 0
            }
        
        return results
    
    def close_position(self, position_id: str, symbol: str) -> None:
        """Close position and calculate realized P&L."""
        
        if position_id not in self.positions:
            return
        
        position = self.positions[position_id]
        
        # Calculate proceeds from closing
        quantity = position['quantity']
        current_price = position['current_price']
        proceeds = quantity * current_price
        
        # Update cash and remove position
        self.cash += proceeds
        if position_id in self.positions:
            del self.positions[position_id]


class MomentumStrategy:
    """Simple momentum strategy for backtesting examples.
    
    Args:
        lookback_period: Number of bars to consider for trend (default 20)
        entry_threshold: Percentage change above previous close to enter (default 1%)
    """
    
    def __init__(self, lookback_period: int = 20, entry_threshold: float = 0.01):
        self.lookback = lookback_period
        self.threshold = entry_threshold
        
    async def analyze(self, ohlcv_data: pd.DataFrame) -> List[Dict]:
        """Analyze OHLCV and generate trading signals.
        
        Args:
            ohlcv_data: Historical OHLCV data
            
        Returns:
            List of trade signals with entry/exit recommendations
        """
        signals = []
        close_prices = ohlcv_data.iloc[:, -1]  # Last column is typically price
        
        for i in range(self.lookback, len(close_prices)):
            current_price = close_prices.iloc[i]
            prev_close = close_prices.iloc[i-1]
            
            if prev_close > 0 and (current_price / prev_close) > (1 + self.threshold):
                signals.append({
                    'type': 'buy_signal',
                    'price': round(current_price, 2),
                    'signal_strength': round((current_price - prev_close) / prev_close, 4)
                })
        
        return signals


def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.02) -> float:
    """Calculate annualized Sharpe ratio.
    
    Args:
        returns: List of daily return percentages
        risk_free_rate: Annual risk-free rate (default 2% = 0.02)
        
    Returns:
        Annualized Sharpe ratio
    """
    
    if not returns or len(returns) < 2:
        return 0.0
    
    daily_returns = [r / 100 for r in returns]  # Convert from percentage to decimal
    
    annualized_return = np.mean(daily_returns) * 252
    annualized_volatility = np.std(daily_returns) * np.sqrt(252)
    
    if annualized_volatility == 0:
        return 0.0
    
    sharpe = (annualized_return - risk_free_rate) / annualized_volatility
    return round(sharpe, 2)


def calculate_max_drawdown(equity_curve: List[float]) -> float:
    """Calculate maximum drawdown from equity curve.
    
    Args:
        equity_curve: List of cumulative portfolio values
        
    Returns:
        Maximum drawdown as percentage
    """
    
    if not equity_curve or len(equity_curve) < 2:
        return 0.0
    
    peak = equity_curve[0]
    max_drawdown_pct = 0.0
    
    for value in equity_curve:
        if value > peak:
            peak = value
        
        drawdown_pct = (peak - value) / peak * 100 if peak > 0 else 0
        max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)
    
    return round(max_drawdown_pct, 2)


__all__ = [
    "BacktestEngine",
    "MomentumStrategy",
    "calculate_sharpe_ratio",
    "calculate_max_drawdown"
]
