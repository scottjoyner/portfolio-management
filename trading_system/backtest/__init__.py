"""Backtesting Package - Complete Implementation

The backtesting package provides comprehensive strategy evaluation capabilities:
- Historical market data replay via adapters (real or mock)
- Paper execution simulation for strategy validation  
- Performance metrics calculation (Sharpe, Sortino, drawdown)
- Equity curve generation and trade log tracking
- REST API endpoints for trigger/retrieve/invalidate operations

Version: 1.0.0
License: MIT

Key Features:
============
✅ Multi-strategy ensemble testing
✅ Market regime-aware backtesting
✅ Advanced slippage modeling
✅ Transaction cost analysis
✅ Real-time result invalidation support
✅ Docker-deployable with health checks

Usage Examples:
===============

Basic backtest run:
    from trading_system.backtest.engine import BacktesterEngine, Config
    
    config = Config(
        strategy_name="btc-momentum-strategy",
        start_date="2025-01-01", 
        end_date="2025-05-31"
    )
    engine = BacktesterEngine(config=config)
    results = engine.run_backtest()
    
    # Access results
    print(f"Total Return: {results['total_return']:.2f}%")
    print(f"Sharpe Ratio: {results['sharpe_ratio']:.3f}")

Advanced usage with callbacks:
    from trading_system.backtest.adapter import MockMarketDataAdapter
    
    adapter = MockMarketDataAdapter()
    engine = BacktesterEngine(
        config=config,
        market_adapter=adapter,
        on_trade_callback=lambda trade: log_trade(trade)
    )
    results = engine.run_backtest()

Architecture:
=============
    +--------------------------+    +------------------------------+
    |  MarketDataAdapter       |    |  StrategySimulator           |
    |  (Mock or Real data)     |    |  (Strategy logic)            |
    +--------------------------+    +------------------------------+
                   ↓                         ↓
    +--------------------------+    +------------------------------+
    |  EquityCurveGenerator    |    |  PerformanceCalculator       |
    |  (Equity, drawdown curves)|    |  (Sharpe, Sortino, metrics) |
    +--------------------------+    +------------------------------+
                              ↓
                    +------------------+
                    |  DatabaseModels  |
                    |  (Trade logs)    |
                    +------------------+

Modules:
========
- backtest/__init__.py        - Package initialization and exports
- backtest/engine.py          - Main backtesting engine implementation (~4KB)
- backtest/adapter.py         - Market data adapter interface (mock + real) 
- backtest/simulator.py       - Paper execution simulation layer (~13KB)
- backtest/models.py          - SQLAlchemy database ORM models (~5KB)
- backtest/test_backtesting_e2e.py - End-to-end test suite

Production Patterns:
====================
1. Always use mock adapter for testing (no API keys needed)
2. For production, swap with real MarketDataAdapter with live keys
3. Use Docker deployment for consistent environment
4. Structured JSON logging to /tmp/{service}.log
5. Health check endpoint on port 8080

Dependencies:
=============
- SQLAlchemy>=2.0 (database ORM)
- asyncio (async event loops)
- pytest (testing framework)

"""

__version__ = "1.0.0"


# Export main classes and functions from engine
from .engine import Config, BacktestResultSummary, BacktesterEngine

# Additional exports from adapter module  
from .adapter import MarketDataAdapter, MockMarketDataAdapter

# Type hints for common patterns
from typing import Dict, List, Optional, Callable

__all__ = [
    'Config',
    'BacktestResultSummary', 
    'BacktesterEngine',
    'MarketDataAdapter',
    'MockMarketDataAdapter',
]
