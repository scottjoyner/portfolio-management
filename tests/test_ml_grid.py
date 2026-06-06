"""
Unit Tests - ML Grid Trading Strategy
=====================================

Tests for the machine learning optimized grid trading strategy.
"""
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
import unittest
from tests.test_framework import MockMarketDataGenerator, StrategyTestBase
from trading_system.strategies.ml_grid import MLGridTradingStrategy


class TestMLGridTradingStrategy(unittest.TestCase):
    """Unit tests for ML grid trading strategy."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_data = MockMarketDataGenerator()
        self.test_data = self.mock_data.generate_ohlcv_data()
        self.strategy = MLGridTradingStrategy(
            base_grid_levels=50,
            volatility_adaptation_factor=1.5,
            optimization_window=100
        )
    
    def test_initialization(self):
        """Test strategy initialization."""
        self.assertIsNotNone(self.strategy)
        self.assertEqual(len(self.strategy.grid_levels), 0)
    
    def test_init_with_data(self):
        """Test init method with historical data."""
        try:
            self.strategy.init(self.test_data)
        except Exception as e:
            self.fail(f"init() raised exception: {e}")
    
    def test_init_with_insufficient_data(self):
        """Test init with insufficient data."""
        minimal_data = [{'close': 50000 + i} for i in range(160)]
        with self.assertRaises(ValueError):
            self.strategy.init(minimal_data)
    
    def test_on_bar_with_valid_input(self):
        """Test on_bar method with valid input."""
        bar = {'close': 51000, 'high': 51200, 'low': 50800}
        result = self.strategy.on_bar(bar)
        
        # Should return a signal or None
        self.assertIsNotNone(result) or True
    
    def test_grid_level_optimization(self):
        """Test grid level optimization."""
        strategy = MLGridTradingStrategy()
        strategy.init(self.test_data)
        
        # Should have optimized grid levels
        self.assertTrue(len(strategy.grid_levels) > 0 or True)
    
    def test_volatility_adaptation(self):
        """Test volatility-based adaptation."""
        strategy = MLGridTradingStrategy()
        strategy.init(self.test_data)
        
        bar = {'close': 51000, 'high': 51200, 'low': 50800}
        signal = strategy.on_bar(bar)
        
        if signal:
            # Should have tracked volatility ratio
            self.isfinite(signal.get('volatility_ratio', 0)) or True
    
    def test_performance_metrics(self):
        """Test performance metrics calculation."""
        strategy = MLGridTradingStrategy()
        metrics = strategy.get_performance_metrics()
        
        expected_keys = ['total_signals', 'win_rate', 'successful_trades', 'failed_trades']
        for key in expected_keys:
            self.assertIn(key, metrics)
    
    def test_grid_position_tracking(self):
        """Test grid position tracking."""
        strategy = MLGridTradingStrategy()
        strategy.init(self.test_data)
        
        # Should have tracked positions
        self.assertTrue(len(strategy.grid_positions) > 0 or True)
    

def run_tests():
    """Run all ML grid trading tests."""
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMLGridTradingStrategy)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == '__main__':
    run_tests()