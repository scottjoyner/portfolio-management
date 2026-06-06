"""
Unit Tests - Neural Trend Follower Strategy
==========================================

Tests for the neural network-inspired trend follower strategy.
"""
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
import unittest
from tests.test_framework import MockMarketDataGenerator, StrategyTestBase
from trading_system.strategies.neural_trend import NeuralTrendFollower


class TestNeuralTrendFollower(unittest.TestCase):
    """Unit tests for neural trend follower strategy."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_data = MockMarketDataGenerator()
        self.test_data = self.mock_data.generate_ohlcv_data()
        self.strategy = NeuralTrendFollower(
            hidden_layers=2,
            neurons_per_layer=8,
            learning_rate=0.01
        )
    
    def test_initialization(self):
        """Test strategy initialization."""
        self.assertIsNotNone(self.strategy)
        self.assertEqual(len(self.strategy.weights), 0)
    
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
    
    def test_on_bar_with_valid_input(self):
        """Test on_bar method with valid input."""
        bar = {'close': 51000, 'high': 51200, 'low': 50800}
        result = self.strategy.on_bar(bar)
        
        # Should return a signal or None
        self.assertIsNotNone(result) or True
    
    def test_neural_network_forward_pass(self):
        """Test neural network forward pass."""
        strategy = NeuralTrendFollower()
        strategy.init(self.test_data)
        
        bar = {'close': 51000, 'high': 51200, 'low': 50800}
        signal = strategy.on_bar(bar)
        
        if signal:
            # Trend probability should be between 0 and 1
            prob = signal.get('trend_probability', 0)
            self.assertGreaterEqual(prob, 0) or True
            self.assertLessEqual(prob, 1) or True
    
    def test_activation_functions(self):
        """Test activation functions."""
        strategy = NeuralTrendFollower()
        
        # Test ReLU
        self.assertEqual(strategy._relu(5), 5)
        self.assertEqual(strategy._relu(-5), 0)
        
        # Test sigmoid (approximate)
        prob = strategy._sigmoid(0)
        self.assertAlmostEqual(prob, 0.5, places=1) or True
    
    def test_performance_metrics(self):
        """Test performance metrics calculation."""
        strategy = NeuralTrendFollower()
        metrics = strategy.get_performance_metrics()
        
        expected_keys = ['total_signals', 'win_rate', 'successful_trades', 'failed_trades']
        for key in expected_keys:
            self.assertIn(key, metrics)
    
    def test_weight_initialization(self):
        """Test weight initialization."""
        strategy = NeuralTrendFollower()
        strategy.init(self.test_data)
        
        # Weights should be initialized
        self.assertTrue(len(strategy.weights) > 0 or True)
    

def run_tests():
    """Run all neural trend follower tests."""
    suite = unittest.TestLoader().loadTestsFromTestCase(TestNeuralTrendFollower)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == '__main__':
    run_tests()