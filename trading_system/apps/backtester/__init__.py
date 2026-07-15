"""Backtester Apps Module

This module provides:
1. BacktestRunner - Production backtesting engine
2. Results storage for historical performance tracking
3. API endpoints integrating backtest data with /strategies endpoint
4. Benchmark comparisons and strategy ranking

The backtest results are persisted to PostgreSQL and integrated into the
GET /api/strategies response, providing both real-time metrics and
historical performance benchmarks.
"""

from .results_storage import BacktestResultsStorage, HistoricalPerformanceAPI

__all__ = [
    "BacktestResultsStorage",
    "HistoricalPerformanceAPI",
]
