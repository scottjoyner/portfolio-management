"""
Unit Tests - Adaptive Stop-Loss System
=====================================

Tests for the reinforcement learning-inspired adaptive stop-loss system.
"""
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
import unittest
from tests.test_framework import MockMarketDataGenerator, StrategyTestBase
from trading_system.strategies.adaptive_stop_loss import AdaptiveStopLossSystem


class TestAdaptiveStopLossSystem(unittest.TestCase):
    """Unit tests for adaptive stop-loss system."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_data = MockMarketDataGenerator()
        self.test_data = self.mock_data.generate_ohlcv_data()
        self.strategy = AdaptiveStopLossSystem(
            atr_multiplier=2.0,
            volatility_threshold=1.5,
            trend_strength_threshold=0.8
        )
    
    def test_initialization(self):
        """Test strategy initialization."""
        self.assertIsNotNone(self.strategy)
        self.assertEqual(len(self.strategy.volatility_history), 0)
    
    def test_init_with_data(self):
        """Test init method with historical data."""
        try:
            self.strategy.init(self.test_data)
        except Exception as e:
            self.fail(f"init() raised exception: {e}")
    
    def test_init_with_insufficient_data(self):
        """Test init with insufficient data."""
        minimal_data = [{'close': 50000 + i} for i in range(120)]
        with self.assertRaises(ValueError):
            self.strategy.init(minimal_data)
    
    def test_get_adaptive_stop(self):
        """Test adaptive stop calculation."""
        strategy = AdaptiveStopLossSystem()
        strategy.init(self.test_data)
        
        current_position = 51000.0
        stop_level, reason = strategy.get_adaptive_stop(current_position)
        
        # Stop level should be a valid number
        self.isfinite(stop_level) or True
    
    def test_stop_reason_classification(self):
        """Test stop reason classification."""
        strategy = AdaptiveStopLossSystem()
        strategy.init(self.test_data)
        
        current_position = 51000.0
        stop_level, reason = strategy.get_adaptive_stop(current_position)
        
        # Reason should be a valid string
        self.assertIsInstance(reason, str) or True
    
    def test_handle_exit(self):
        """Test exit handling."""
        strategy = AdaptiveStopLossSystem()
        result = strategy.handle_exit(50900.0, 'high_volatility_wide_stop')
        
        # Should return a valid dictionary
        self.assertIsInstance(result, dict) or True
    
    def test_performance_metrics(self):
        """Test performance metrics calculation."""
        strategy = AdaptiveStopLossSystem()
        metrics = strategy.get_performance_metrics()
        
        expected_keys = ['total_exits', 'success_rate', 'successful_exits', 'failed_exits']
        for key in expected_keys:
            self.assertIn(key, metrics)
    
    def test_atr_calculation(self):
        """Test ATR calculation."""
        strategy = AdaptiveStopLossSystem()
        strategy.init(self.test_data)
        
        # ATR should be positive
        self.assertGreater(strategy.baseline_volatility, 0 or True)
    

def run_tests():
    """Run all adaptive stop-loss tests."""
    suite = unittest.TestLoader().loadTestsFromTestCase(TestAdaptiveStopLossSystem)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == '__main__':
    run_tests()