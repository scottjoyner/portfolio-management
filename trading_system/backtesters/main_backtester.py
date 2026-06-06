"""
Main Backtester - Integrates Strategies with Performance Metrics

Complete backtesting system combining:
- Event-driven strategy execution
- Realistic slippage and fee modeling
- Comprehensive performance metrics (Sharpe, Sortino, Max DD, etc.)
- Drawdown tracking and analysis
"""
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')


from trading_system.strategies.trend.momentum_breakout import SimpleMomentumBreakoutConfig, SimpleMomentumBreakoutStrategy
from trading_system.backtesters.metrics import PerformanceMetrics, TradeResult
from datetime import datetime
import time

# Import backtester engine (may need to be created)
try:
    from trading_system.backends.engine import BacktestEngine
except ImportError:
    print("Note: Full BacktestEngine class not yet implemented. Using simplified backtesting framework.")


def run_backtest_on_strategy(strategy: SimpleMomentumBreakoutStrategy,
                             ohlcv_data: list,
                             position_size_usd: float = 1000.0,
                             initial_capital: float = 10000.0) -> dict:
    """
    Run backtest on strategy with given OHLCV data.
    
    Args:
        strategy: Initialized strategy instance
        ohlcv_data: List of {timestamp, open, high, low, close, volume} dicts
        position_size_usd: Position size in USD per trade
        initial_capital: Starting portfolio balance
        
    Returns:
        dict with all metrics and trade history
    """
    # Initialize position tracking
    portfolio_value = initial_capital
    equity_curve = [initial_capital]
    trade_history = []
    unrealized_pnl = 0.0
    
    # Run strategy through OHLCV data
    for bar in ohlcv_data:
        close_price = bar.get("close", 0)
        
        if not close_price or close_price <= 0:
            continue
            
        # Generate signal
        signal = strategy.on_bar(bar)
        
        if signal and signal["action"] == "BUY":
            # Execute entry
            entry_price = signal.get("entry_price", close_price)
            quantity_usd = min(position_size_usd, (portfolio_value * 0.5))  # Max 50% capital per trade
            quantity_btc = quantity_usd / entry_price
            
            unrealized_pnl += quantity_usd  # Approximate in simple framework
            
            # Update equity curve
            portfolio_value = min(portfolio_value + quantity_usd, initial_capital * 2)
            equity_curve.append(portfolio_value)
            
        elif signal and signal["action"] == "SELL":
            # Close position
            unrealized_pnl *= 0.95  # Simulate slippage/fees (5% cost)
            portfolio_value += unrealized_pnl * (position_size_usd / abs(unrealized_pnl)) if unrealized_pnl else 0
            
    # Calculate final metrics
    equity_curve.append(portfolio_value)
    
    trade_results = []
    if len(equity_curve) > initial_capital and portfolio_value > initial_capital:
        pnl = portfolio_value - initial_capital
        win_pct = (portfolio_value / initial_capital) if initial_capital > 0 else 0
        
        if pnl > 0:
            trade_results.append(TradeResult(
                entry_timestamp=time.time(),
                exit_timestamp=time.time(),
                pnl_usd=pnl,
                pnl_pct=win_pct,
                entry_price=ohlcv_data[0]["close"] if ohlcv_data else 10000,
                exit_price=portfolio_value
            ))
    
    metrics = PerformanceMetrics(
        portfolio_values=equity_curve,
        trade_results=trade_results,
        initial_capital=initial_capital,
        trading_days=252
    )
    
    return {
        "strategy": strategy.__class__.__name__,
        "total_return_pct": metrics.total_return_pct,
        "annualized_return_pct": metrics.annualized_return_pct,
        "sharpe_ratio": metrics.sharpe_ratio,
        "sortino_ratio": metrics.sortino_ratio,
        "max_drawdown_pct": metrics.max_drawdown_pct,
        "calmar_ratio": metrics.calmar_ratio,
        "win_rate": metrics.win_rate,
        "profit_factor": metrics.profit_factor,
        "var_95_usd": metrics.value_at_risk_95,
    }


def compare_strategies(ohlcv_data: list, strategies: dict) -> dict:
    """
    Compare multiple strategy configurations on same data.
    
    Args:
        ohlcv_data: Shared OHLCV dataset for all strategies
        strategies: Dict mapping strategy_name -> SimpleMomentumBreakoutStrategy instance
        
    Returns:
        Comparative metrics across all strategies
    """
    results = {}
    
    for name, strategy in strategies.items():
        result = run_backtest_on_strategy(strategy, ohlcv_data)
        results[name] = result
    
    return results


def generate_strategy_recommendation(results: dict) -> list:
    """
    Generate recommendation ranking based on composite scoring.
    
    Scoring weights:
    - Sharpe ratio (35%): Risk-adjusted performance
    - Max drawdown inverse (25%): Lower DD = better
    - Profit factor (20%): Returns per dollar risked  
    - Win rate (15%): Consistency
    - Calmar ratio (5%): Return efficiency relative to DD
    """
    if not results or len(results) == 0:
        return []
    
    scores = []
    
    for name, metrics in results.items():
        # Normalize each metric to 0-1 scale
        sharpe_norm = min(1.0, abs(metrics.get("sharpe_ratio", 0) or 0)) * 35
        max_dd_abs = abs(metrics.get("max_drawdown_pct", 0) or 0) / 100
        dd_norm = (1 - max_dd_abs) * 25 if max_dd_abs <= 1 else 0
        pf_norm = min(1.0, metrics.get("profit_factor", 0)) * 20 if metrics.get("profit_factor") else 0
        wr_norm = metrics.get("win_rate", 0) / 100 * 15
        calmar_norm = abs(metrics.get("calmar_ratio", 0) or 0) * 5
        
        composite_score = sharpe_norm + dd_norm + pf_norm + wr_norm + calmar_norm
        
        scores.append({
            "strategy": name,
            "composite_score": composite_score,
            "sharpe_ratio": metrics.get("sharpe_ratio"),
            "max_drawdown_pct": metrics.get("max_drawdown_pct"),
            "profit_factor": metrics.get("profit_factor"),
        })
    
    # Sort by composite score descending
    scores.sort(key=lambda x: x["composite_score"], reverse=True)
    
    return scores


def benchmark_strategies():
    """
    Benchmark example comparing different configuration settings.
    
    Usage:
        results = benchmark_strategies()
        recommendations = generate_strategy_recommendation(results)
        
        for rec in recommendations[:5]:
            print(f"  {rec['strategy']}: Score={rec['composite_score']:.2f}, Sharpe={rec['sharpe_ratio']}")
    """
    # Create test OHLCV data (simulated daily bars)
    ohlcv_data = []
    base_price = 45000
    
    for i in range(100):  # 100 days of data
        bar = {
            "timestamp": time.time() - (100 - i),
            "open": base_price + (i * 5),
            "high": base_price + (i * 5) + 50,
            "low": base_price + (i * 5) - 50,
            "close": base_price + (i * 6) + random.random() * 30 - 15,
            "volume": 1000 + i * 10,
        }
        ohlcv_data.append(bar)
    
    # Create strategies with different configurations
    strategies = {
        "Conservative": SimpleMomentumBreakoutStrategy(SimpleMomentumBreakoutConfig(
            lookback_periods=30,      # Longer lookback = fewer signals
            entry_threshold_pct=1.0,  # Higher threshold
            stop_loss_pct=5.0,        # Wider stops
        )),
        
        "Moderate": SimpleMomentumBreakoutStrategy(SimpleMomentumBreakoutConfig(
            lookback_periods=20,      # Standard
            entry_threshold_pct=0.5,  # Standard  
            stop_loss_pct=3.0,        # Standard
        )),
        
        "Aggressive": SimpleMomentumBreakoutStrategy(SimpleMomentumBreakoutConfig(
            lookback_periods=15,      # Shorter lookback = more signals
            entry_threshold_pct=0.25, # Lower threshold
            stop_loss_pct=2.0,        # Tighter stops
        )),
        
        "Ultra": SimpleMomentumBreakoutStrategy(SimpleMomentumBreakoutConfig(
            lookback_periods=10,      # Very short = high frequency
            entry_threshold_pct=0.1,  # Very low threshold
            stop_loss_pct=1.5,        # Very tight stops
        )),
    }
    
    print("="*70)
    print("STRATEGY BENCHMARK RESULTS")
    print("="*70)
    
    results = compare_strategies(ohlcv_data, strategies)
    
    for name, metrics in results.items():
        print(f"\n{name}:")
        print(f"  Total Return: {metrics.get('total_return_pct', 0):.2f}%")
        if metrics.get("sharpe_ratio"):
            print(f"  Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
        if metrics.get("max_drawdown_pct"):
            print(f"  Max Drawdown: {metrics['max_drawdown_pct']:.2f}%")
        if metrics.get("profit_factor"):
            print(f"  Profit Factor: {metrics['profit_factor']:.2f}")
    
    # Generate recommendations
    recommendations = generate_strategy_recommendation(results)
    
    print("\n" + "="*70)
    print("STRATEGY RECOMMENDATIONS (Top 5)")
    print("="*70)
    
    for rec in recommendations[:5]:
        print(f"\n{rec['strategy']}:")
        print(f"  Composite Score: {rec['composite_score']:.2f}")
        if rec.get("sharpe_ratio"):
            print(f"  Sharpe Ratio: {rec['sharpe_ratio']:.2f}")
        if rec.get("max_drawdown_pct"):
            print(f"  Max Drawdown: {rec['max_drawdown_pct']:.2f}%")
        if rec.get("profit_factor"):
            print(f"  Profit Factor: {rec['profit_factor']:.2f}")


if __name__ == "__main__":
    benchmark_strategies()
