"""
Comprehensive Backtester Integration Tests

Tests complete backtesting workflow from data loading through metrics calculation.
Includes edge case testing and performance validation.
"""
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')


from trading_system.backends.metrics import PerformanceMetrics, DrawdownMetrics
from tests.test_momentum_breakout_strategy import TestSimpleMomentumBreakoutStrategy


def test_performance_metrics_sharpe_sortino():
    """Test Sharpe and Sortino ratio calculations."""
    print("\nTesting Sharpe/Sortino calculations...")
    
    # Create equity curve with volatility
    equity_curve = [10000]
    for i in range(50):
        # Random walk with drift (approx 10% annual return)
        daily_return = 0.0003 - (random.random() - 0.5) * 0.02  # ~3% annual + volatility
        equity_curve.append(equity_curve[-1] * (1 + daily_return))
        
    metrics = PerformanceMetrics(
        portfolio_values=equity_curve,
        trading_days=252
    )
    
    assert metrics.sharpe_ratio is not None, "Sharpe ratio should be computable"
    assert metrics.sortino_ratio is not None, "Sortino ratio should be computable"
    
    print(f"  ✓ Sharpe Ratio: {metrics.sharpe_ratio:.2f}")
    print(f"  ✓ Sortino Ratio: {metrics.sortino_ratio:.2f}")


def test_max_drawdown_calculation():
    """Test maximum drawdown calculation accuracy."""
    print("\nTesting max drawdown calculation...")
    
    # Create curve with known drawdown
    equity_curve = [10000, 11000, 9500, 10500, 8000, 9000, 10000]
    
    metrics = PerformanceMetrics(equity_curve=equity_curve)
    
    assert abs(metrics.max_drawdown_pct - 37.5) < 0.1, \
        f"Expected ~37.5% max drawdown (peak 11000 to trough 8000), got {metrics.max_drawdown_pct:.2f}%"
    
    print(f"  ✓ Max Drawdown: {metrics.max_drawdown_pct:.2f}%")


def test_win_rate_with_trade_results():
    """Test win rate calculation with trade results."""
    print("\nTesting win rate with trade results...")
    
    from typing import List
    from trading_system.backends.metrics import TradeResult
    import time
    
    # Create realistic trades (6 wins, 4 losses = 60% win rate)
    trade_results = [
        TradeResult(
            entry_timestamp=time.time() - 100,
            exit_timestamp=time.time() - 90,
            pnl_usd=200,
            pnl_pct=0.02,
            entry_price=45000,
            exit_price=45900
        ) for _ in range(6)
    ] + [
        TradeResult(
            entry_timestamp=time.time() - 80,
            exit_timestamp=time.time() - 70,
            pnl_usd=-150,
            pnl_pct=-0.016,
            entry_price=46000,
            exit_price=45850
        ) for _ in range(4)
    ]
    
    equity_curve = [10000] + [10000 + t.pnl_usd for t in trade_results]
    
    metrics = PerformanceMetrics(equity_curve, trade_results)
    
    assert abs(metrics.win_rate - 60.0) < 0.1, \
        f"Expected ~60% win rate, got {metrics.win_rate:.2f}%"
    
    print(f"  ✓ Win Rate: {metrics.win_rate:.2f}%")


def test_profit_factor_calculation():
    """Test profit factor calculation."""
    print("\nTesting profit factor calculation...")
    
    from trading_system.backends.metrics import TradeResult
    import time
    
    # Create trades with known P/L (1000 profit, 600 loss = PF = 1.67)
    trade_results = [
        TradeResult(time.time() - 100, time.time() - 90, pnl_usd=200, pnl_pct=0.02, 
                   entry_price=45000, exit_price=45900) for _ in range(5)
    ] + [
        TradeResult(time.time() - 80, time.time() - 70, pnl_usd=-150, pnl_pct=-0.016,
                   entry_price=46000, exit_price=45850) for _ in range(3)
    ]
    
    gross_profits = sum(t.pnl_usd for t in trade_results if t.pnl_usd > 0)
    gross_losses = abs(sum(t.pnl_usd for t in trade_results if t.pnl_usd < 0))
    expected_pf = gross_profits / gross_losses
    
    equity_curve = [10000] + [10000 + t.pnl_usd for t in trade_results]
    metrics = PerformanceMetrics(equity_curve, trade_results)
    
    assert abs(metrics.profit_factor - expected_pf) < 0.01, \
        f"Expected PF {expected_pf:.2f}, got {metrics.profit_factor:.2f}"
    
    print(f"  ✓ Profit Factor: {metrics.profit_factor:.2f}")


def test_calmar_ratio_calculation():
    """Test Calmar ratio (return / max drawdown)."""
    print("\nTesting Calmar ratio calculation...")
    
    from trading_system.backends.metrics import TradeResult
    
    # Create curve with 50% total return and 20% max drawdown = Calmar = 2.5
    trade_results = [
        TradeResult(time.time() - 100, time.time() - 90, pnl_usd=100, pnl_pct=0.01,
                   entry_price=45000, exit_price=45900) for _ in range(8)
    ] + [
        TradeResult(time.time() - 70, time.time() - 60, pnl_usd=-200, pnl_pct=-0.025,
                   entry_price=46000, exit_price=45800) for _ in range(1)
    ]
    
    equity_curve = [10000] + [10000 + t.pnl_usd for t in trade_results]
    metrics = PerformanceMetrics(equity_curve, trade_results, trading_days=252)
    
    # Note: Calmar uses CAGR, so expect positive value
    assert metrics.calmar_ratio is not None, "Calmar ratio should be computable"
    
    print(f"  ✓ Calmar Ratio: {metrics.calmar_ratio:.2f}")


def test_var_95_calculation():
    """Test Value at Risk calculation."""
    print("\nTesting VaR 95% calculation...")
    
    import random
    
    # Create volatile curve
    equity_curve = [10000]
    for i in range(100):
        daily_return = (random.random() - 0.5) * 0.04  # ±2% volatility
        equity_curve.append(equity_curve[-1] * (1 + daily_return))
    
    metrics = PerformanceMetrics(equity_curve, trading_days=252)
    
    assert metrics.value_at_risk_95 is not None, "VaR should be computable"
    print(f"  ✓ VaR 95%: ${metrics.value_at_risk_95:.2f}")


def test_edge_cases():
    """Test edge cases and error handling."""
    print("\nTesting edge cases...")
    
    # Empty equity curve
    metrics = PerformanceMetrics([])
    assert metrics.total_return_pct == 0.0, "Empty curve should return 0%"
    
    # Single point
    metrics = PerformanceMetrics([10000])
    assert metrics.max_drawdown_pct == 0.0, "Single point should have 0% DD"
    
    print("  ✓ Edge cases handled correctly")


def test_longest_drawdown_duration():
    """Test drawdown duration tracking."""
    print("\nTesting drawdown duration calculation...")
    
    # Create curve with known drawdown period
    equity_curve = [10000, 11000] + [10500 - i for i in range(10)] + \
                   [11000 / 2 + i for i in range(8)]
    
    dd_metrics = DrawdownMetrics(equity_curve)
    
    duration = dd_metrics.longest_drawdown_duration
    
    assert duration is not None, "Duration should be computable"
    print(f"  ✓ Longest Drawdown Duration: {duration} bars")


def run_all_tests():
    """Run all integration tests."""
    print("="*70)
    print("COMPREHENSIVE BACKTESTER INTEGRATION TESTS")
    print("="*70)
    
    try:
        test_performance_metrics_sharpe_sortino()
        test_max_drawdown_calculation()
        test_win_rate_with_trade_results()
        test_profit_factor_calculation()
        test_calmar_ratio_calculation()
        test_var_95_calculation()
        test_edge_cases()
        test_longest_drawdown_duration()
        
        print("\n" + "="*70)
        print("✓ ALL INTEGRATION TESTS PASSED")
        print("="*70)
        return True
        
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {str(e)}")
        return False
    except Exception as e:
        print(f"\n✗ UNEXPECTED ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
