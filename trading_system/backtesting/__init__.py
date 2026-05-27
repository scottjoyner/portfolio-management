"""
Backtesting Package (trading_system.backtesting)

Provides event-driven backtesting engine for strategy evaluation.

Usage:
```python
from backtesting import BacktestEngine, OHLCVDataLoader
from strategies import EMACrossoverStrategy

# Create strategy instance
strategy = EMACrossoverStrategy()

# Initialize with data
ohlcv_data = [...]  # list of OHLCVBar objects
strategy.setup(ohlcv_data)

# Define signal handler
def signal_handler(bar):
    return strategy.on_bar(bar)

# Run backtest
engine = BacktestEngine(signal_handler)
results = engine.run_backtest(ohlcv_data, initial_capital=10000)

print(f"Final Balance: ${results.final_balance:.2f}")
```
"""

from .engine import (
    BacktestEngine,
    OHLCVDataLoader,
    PerformanceMetrics,
    BacktestResult,
    Transaction,
)

__version__ = "0.1.0"
__all__ = [
    "BacktestEngine",
    "OHLCVDataLoader",
    "PerformanceMetrics",
    "BacktestResult",
    "Transaction",
]
