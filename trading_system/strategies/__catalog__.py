"""
Comprehensive Trading Strategy Catalog and Backtesting Framework
=====================================================================

This directory contains the complete trading strategy ecosystem:

STRATEGIES DIRECTORY STRUCTURE:
-------------------------------
trading_system/strategies/
    ├── trend_following/       # Trend following strategies (MA crossover, VWAP, Bollinger breakout)
    │   ├── macd_signal_crossover.py
    │   ├── triple_ma_strategy.py  
    │   ├── bollinger_band_squeeze.py
    │   ├── vwap_momentum.py
    │   ├── volume_breakout.py
    │   ├── ichimoku_cloud.py
    │   └── keltner_channel.py
    │
    ├── mean_reversion/       # Mean reversion strategies (Z-score, RSI, Bollinger bands)
    │   ├── zscore_statistical_arb.py
    │   ├── bollinger_mean_revert.py
    │   └── rsi_mean_revert.py
    │
    ├── arbitrage/           # Arbitrage strategies (spot-futures basis, cross-exchange)
    │   ├── spot_futures_basis_arb.py
    │   └── cross_exchange_basis_arb.py
    │
    └── ...                  # Additional strategy categories (momentum, volatility, etc.)

BACKTESTING FRAMEWORK:
----------------------
1. Strategy Testing Layer:
   - Unit tests for each strategy with deterministic inputs
   - Backtesting engine supporting batch data processing  
   - Performance metrics aggregation (win rate, profit factor, drawdown)
   - Regime classification tools (trending/ranging/volatile)

2. Deployment Infrastructure:
   - Docker containerization for fleet deployment
   - Configuration management via environment variables
   - Health check endpoints and metrics exposure
   - Error handling with automatic circuit breakers

3. Monitoring & Alerting:
   - Performance degradation alerts
   - Regime change detection notifications  
   - Risk parameter monitoring
   - Drawdown threshold warnings

USAGE GUIDE:
------------
1. Testing Individual Strategies:
   python -m trading_system.strategies.trend.macd_signal_crossover.unit_tests

2. Running Backtests Across Multiple Strategies:
   python backtest_engine.py --strategies macd,rsi,zscore,vwap --pairs btc,eth,sol --period 365d

3. Deploying to Production Fleet:
   hermes deploy strategies trading_system/strategies/ --profile default --docker-image portfolio-management:v1.0

4. Monitoring Strategy Performance:
   curl http://localhost:8000/api/v1/strategies/macdsignalcrossover/performance
   
5. Managing Configuration:
   cat ~/.hermes/profiles/default/config/trading_system.json

PERFORMANCE TRACKING:
---------------------
Each strategy exposes the following metrics via get_performance_metrics():
  - total_signals: Number of signals generated since initialization  
  - win_rate: Percentage of profitable trades (40-65% target by strategy type)
  - successful_trades: Count of profitable trade executions
  - failed_trades: Count of losing trade executions

Additional metrics exposed per strategy:
  - profit_factor: Gross profit / gross loss ratio
  - max_drawdown: Maximum equity drawdown since inception
  - sharpe_ratio: Annualized return to volatility ratio  
  - regime_fit_score: Classification quality for current market regime

SCALING TO 200+ STRATEGIES:
---------------------------
Current Implementation: 12 production-ready strategies across 3 categories

Scaling Path to 200+ Strategies:
  Phase 1 (COMPLETE): Foundational strategies with comprehensive docs and tests (12 strategies)
  Phase 2: Add 50 more diverse strategies following same factory pattern  
             - Additional trend following: Ichimoku variations, VWAP patterns, Keltner patterns  
             - Mean reversion: Stochastic RSI, Williams %R mean revert, CCI breakout-reversion
             - Arbitrage: Cross-exchange pairs, volatility arb, funding rate arb
  Phase 3: Add 70 more strategies from established trading literature  
             - Market making strategies (order book imbalance, liquidity provision)
             - Volatility-based strategies (VIX skew arb, realized vol hedge)
             - Statistical arbitrage (pairs trading, correlation breakdown)
  Phase 4: Backtest all strategies against historical data with proper out-of-sample validation

Quality Assurance Requirements for All Strategies:
  ✅ Factory pattern lifecycle (init→on_bar)  
  ✅ Comprehensive docstring with purpose, regime fit, failure modes  
  ✅ Unit tests covering initialization and signal generation  
  ✅ Production-ready error handling (NaN guards, null checks)  
  ✅ Compatible with existing structured logging system

AUTHOR: Portfolio Management System Team
DATE: June 2026

This catalog provides the foundation for a comprehensive multi-strategy trading system with  
robust testing, monitoring, and deployment capabilities.
"""
__all__ = ['STRATEGY_CATALOG', 'BACKTESTING_FRAMEWORK']
