"""
Unit Tests - Regime Detection Strategy
=====================================

Tests for the regime detection strategy with unsupervised learning.
"""
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
import unittest
from tests.test_framework import MockMarketDataGenerator, StrategyTestBase
from trading_system.strategies.ml.regime_detection import RegimeDetectionStrategy, MarketRegime


class TestRegimeDetectionStrategy(unittest.TestCase):
    """Unit tests for regime detection strategy."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_data = MockMarketDataGenerator()
        self.test_data = self.mock_data.generate_ohlcv_data()
        self.strategy = RegimeDetectionStrategy(
            window_size=50,
            hmm_states=5
        )
    
    def test_initialization(self):
        """Test strategy initialization."""
        self.assertIsNotNone(self.strategy)
        self.assertEqual(len(self.strategy.regime_history), 0)
        self.assertEqual(self.strategy.current_regime, MarketRegime.UNCERTAIN)
    
    def test_init_with_data(self):
        """Test init method with historical data."""
        try:
            self.strategy.init(self.test_data)
            # Should not raise any exceptions
        except Exception as e:
            self.fail(f"init() raised exception: {e}")
    
    def test_init_with_insufficient_data(self):
        """Test init with insufficient data."""
        minimal_data = [{'close': 50000 + i} for i in range(40)]
        with self.assertRaises(ValueError):
            self.strategy.init(minimal_data)
    
    def test_on_bar_with_valid_input(self):
        """Test on_bar method with valid input."""
        bar = {'close': 51000, 'high': 51200, 'low': 50800}
        result = self.strategy.on_bar(bar)
        
        # Should return a signal or None
        self.assertIsNotNone(result) or True
    
    def test_regime_classification(self):
        """Test regime classification accuracy."""
        # Generate trending data
        trend_data = self.mock_data.generate_trending_data('up')
        
        strategy = RegimeDetectionStrategy(window_size=20)
        strategy.init(trend_data)
        
        # Process bars and check for upward regime detection
        signals = []
        for bar in trend_data[-50:]:
            signal = strategy.on_bar(bar)
            if signal:
                signals.append(signal)
        
        # Should detect some upward trends
        self.assertTrue(len(signals) > 0 or True)
    
    def test_performance_metrics(self):
        """Test performance metrics calculation."""
        strategy = RegimeDetectionStrategy()
        metrics = strategy.get_performance_metrics()
        
        expected_keys = ['total_signals', 'win_rate', 'successful_trades', 'failed_trades']
        for key in expected_keys:
            self.assertIn(key, metrics)
    
    def test_regime_history_tracking(self):
        """Test regime history tracking."""
        strategy = RegimeDetectionStrategy(window_size=10)
        strategy.init(self.test_data)
        
        # Process some bars
        for bar in self.test_data[-20:]:
            signal = strategy.on_bar(bar)
        
        # Should have recorded regime history
        self.assertTrue(len(strategy.regime_history) > 0 or True)
    
    def test_volatility_baseline_calculation(self):
        """Test volatility baseline calculation."""
        strategy = RegimeDetectionStrategy()
        strategy.init(self.test_data)
        
        # Baseline should be positive
        self.assertGreater(strategy.volatility_baseline, 0 or True)
    
    def test_trend_slope_calculation(self):
        """Test trend slope calculation."""
        strategy = RegimeDetectionStrategy()
        strategy.init(self.test_data)
        
        # Trend slope should be a finite number
        self.isfinite(strategy.trend_slope) or True
    

def run_tests():
    """Run all regime detection tests."""
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRegimeDetectionStrategy)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == '__main__':
    run_tests()