#!/usr/bin/env python3
"""
Comprehensive Test Runner - All ML Strategies
============================================

Runs unit tests for all machine learning trading strategies.
Provides detailed reports and performance metrics.
"""
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
import unittest
from datetime import datetime

# Import all test modules
from tests.test_regime_detection import TestRegimeDetectionStrategy
from tests.test_volatility_targeting import TestVolatilityTargetingStrategy
from tests.test_momentum_clustering import TestMomentumClusteringStrategy
from tests.test_neural_trend import TestNeuralTrendFollower
from tests.test_adaptive_stop_loss import TestAdaptiveStopLossSystem
from tests.test_stat_arb import TestStatisticalArbitrageStrategy
from tests.test_ml_grid import TestMLGridTradingStrategy


def run_all_tests():
    """
    Run all unit tests and generate comprehensive report.
    
    Returns:
        Tuple of (success_count, total_count, failure_details)
    """
    print("=" * 70)
    print("COMPREHENSIVE ML STRATEGIES UNIT TEST SUITE")
    print("=" * 70)
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print()
    
    # Create test suite
    loader = unittest.TestLoader()
    
    # Load all tests from each module
    test_suites = [
        ('Regime Detection', TestRegimeDetectionStrategy),
        ('Volatility Targeting', TestVolatilityTargetingStrategy),
        ('Momentum Clustering', TestMomentumClusteringStrategy),
        ('Neural Trend Follower', TestNeuralTrendFollower),
        ('Adaptive Stop-Loss', TestAdaptiveStopLossSystem),
        ('Statistical Arbitrage', TestStatisticalArbitrageStrategy),
        ('ML Grid Trading', TestMLGridTradingStrategy),
    ]
    
    all_tests = []
    for name, test_class in test_suites:
        tests = loader.loadTestsFromTestCase(test_class)
        all_tests.extend(tests)
        print("Loaded {} tests from {}".format(len(tests), name))
    
    total_tests = len(all_tests)
    print()
    print("Total tests loaded: {}".format(total_tests))
    print()
    
    # Run all tests
    runner = unittest.TextTestRunner(verbosity=1)
    result = runner.run(unittest.TestSuite(all_tests))
    
    # Generate report
    print()
    print("=" * 70)
    print("TEST RESULTS SUMMARY")
    print("=" * 70)
    print("Tests run: {}".format(result.testsRun))
    print("Failures: {}".format(len(result.failures)))
    print("Errors: {}".format(len(result.errors)))
    success_rate = (result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100
    print("Success rate: {:.2f}%".format(success_rate))
    print()
    
    # Detailed failure report
    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            error_msg = traceback.split('\n')[0]
            print("  - {}: {}".format(str(test), error_msg))
    
    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            error_msg = traceback.split('\n')[0]
            print("  - {}: {}".format(str(test), error_msg))
    
    # Success report
    success_count = result.testsRun - len(result.failures) - len(result.errors)
    failure_details = []
    
    if result.failures:
        for test, traceback in result.failures:
            failure_details.append({
                'test': str(test),
                'error': traceback.split('\n')[0]
            })
    
    return success_count, total_tests, failure_details


def main():
    """Main entry point."""
    try:
        success_count, total_tests, failures = run_all_tests()
        
        print("\n" + "=" * 70)
        if success_count == total_tests and total_tests > 0:
            print("ALL TESTS PASSED!")
        else:
            print("{}/{} tests passed".format(success_count, total_tests))
        print("=" * 70)
        
    except Exception as e:
        print("\nTest runner error: {}".format(e))
        import traceback
        traceback.print_exc()
        return 1
    
    return 0 if success_count == total_tests else 1


if __name__ == '__main__':
    sys.exit(main())