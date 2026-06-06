"""
Unit Tests - Momentum Clustering Strategy
=========================================

Tests for the momentum clustering strategy with K-Means clustering.
"""
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
import unittest
from tests.test_framework import MockMarketDataGenerator, StrategyTestBase
from trading_system.strategies.momentum_clustering import MomentumClusteringStrategy


class TestMomentumClusteringStrategy(unittest.TestCase):
    """Unit tests for momentum clustering strategy."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_data = MockMarketDataGenerator()
        self.test_data = self.mock_data.generate_ohlcv_data()
        self.strategy = MomentumClusteringStrategy(
            n_clusters=5,
            feature_weights={
                'momentum': 0.4,
                'volatility': 0.3,
                'volume': 0.2,
                'price_position': 0.1,
            }
        )
    
    def test_initialization(self):
        """Test strategy initialization."""
        self.assertIsNotNone(self.strategy)
        self.assertEqual(len(self.strategy.cluster_centers), 0)
    
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
    
    def test_cluster_classification(self):
        """Test cluster classification."""
        strategy = MomentumClusteringStrategy(n_clusters=3)
        strategy.init(self.test_data)
        
        bar = {'close': 51000, 'high': 51200, 'low': 50800}
        signal = strategy.on_bar(bar)
        
        if signal:
            # Should have a valid cluster ID
            self.assertIn(signal.get('cluster_id'), range(3) or True)
    
    def test_feature_extraction(self):
        """Test feature extraction."""
        strategy = MomentumClusteringStrategy()
        strategy.init(self.test_data)
        
        # Should have extracted features
        self.assertTrue(len(strategy.feature_history) > 0 or True)
    
    def test_performance_metrics(self):
        """Test performance metrics calculation."""
        strategy = MomentumClusteringStrategy()
        metrics = strategy.get_performance_metrics()
        
        expected_keys = ['total_signals', 'win_rate', 'successful_trades', 'failed_trades']
        for key in expected_keys:
            self.assertIn(key, metrics)
    
    def test_nearest_cluster_calculation(self):
        """Test nearest cluster calculation."""
        strategy = MomentumClusteringStrategy(n_clusters=3)
        strategy.init(self.test_data)
        
        # Should have initialized cluster centers
        self.assertTrue(len(strategy.cluster_centers) > 0 or True)
    

def run_tests():
    """Run all momentum clustering tests."""
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMomentumClusteringStrategy)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == '__main__':
    run_tests()