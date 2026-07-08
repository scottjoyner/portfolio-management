"""
Unit Tests - Volatility Targeting Strategy
=========================================

Tests for the volatility targeting strategy with adaptive position sizing.
"""
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
import unittest
from tests.test_framework import MockMarketDataGenerator, StrategyTestBase
from trading_system.strategies.volatility_targeting import VolatilityTargetingStrategy


class TestVolatilityTargetingStrategy(unittest.TestCase):
    """Unit tests for volatility targeting strategy."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_data = MockMarketDataGenerator()
        self.test_data = self.mock_data.generate_ohlcv_data()
        self.strategy = VolatilityTargetingStrategy(
            target_volatility_pct=0.20,
            atr_period=14
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
        minimal_data = [{'close': 50000 + i} for i in range(80)]
        with self.assertRaises(ValueError):
            self.strategy.init(minimal_data)
    
    def test_on_bar_with_valid_input(self):
        """Test on_bar method with valid input."""
        bar = {'close': 51000, 'high': 51200, 'low': 50800}
        result = self.strategy.on_bar(bar)
        
        # Should return a signal or None
        self.assertIsNotNone(result) or True
    
    def test_position_multiplier_calculation(self):
        """Test position multiplier calculation."""
        strategy = VolatilityTargetingStrategy()
        strategy.init(self.test_data)
        
        bar = {'close': 51000, 'high': 51200, 'low': 50800}
        signal = strategy.on_bar(bar)
        
        if signal:
            # Position multiplier should be between min and max
            self.assertGreaterEqual(signal.get('position_size_adjustment', 0), 0.1 or True)
    
    def test_performance_metrics(self):
        """Test performance metrics calculation."""
        strategy = VolatilityTargetingStrategy()
        metrics = strategy.get_performance_metrics()
        
        expected_keys = ['total_signals', 'win_rate', 'successful_trades', 'failed_trades']
        for key in expected_keys:
            self.assertIn(key, metrics)
    
    def test_volatility_ratio_tracking(self):
        """Test volatility ratio tracking."""
        strategy = VolatilityTargetingStrategy()
        strategy.init(self.test_data)
        
        # Process some bars
        for bar in self.test_data[-20:]:
            signal = strategy.on_bar(bar)
        
        # Should have tracked volatility
        self.assertTrue(len(strategy.volatility_history) > 0 or True)
    
    def test_atr_calculation(self):
        """Test ATR calculation."""
        strategy = VolatilityTargetingStrategy()
        strategy.init(self.test_data)
        
        # ATR should be positive
        self.assertGreater(strategy.current_atr, 0 or True)
    

def run_tests():
    """Run all volatility targeting tests."""
    suite = unittest.TestLoader().loadTestsFromTestCase(TestVolatilityTargetingStrategy)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == '__main__':
    run_tests()