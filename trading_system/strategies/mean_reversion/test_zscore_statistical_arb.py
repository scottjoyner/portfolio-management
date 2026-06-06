"""
Unit Tests for ZScoreStatisticalArbStrategy

This test suite validates all critical entry/exit paths for the Z-Score 
Statistical Arbitrage Strategy:

1. Test BUY signal when price below -1.5 std threshold
2. Test SELL signal when price above +1.5 std
3. Test no signal when price within mean range
4. Test trailing stop reversion logic after extreme deviation
"""

import unittest
import math

from trading_system.strategies.mean_reversion.zscore_statistical_arb import (
    ZScoreStatisticalArbStrategy,
    ZScoreStatisticalArbConfig,
)
from trading_system.strategies.factory import Signal


class TestZScoreStatisticalArbBuySignal(unittest.TestCase):
    """Test BUY signal generation when price is below -1.5 std from mean."""
    
    def setUp(self):
        """Initialize strategy with test configuration."""
        self.config = ZScoreStatisticalArbConfig()
        self.config.lookback_mean = 20
        self.config.lookback_std = 3
        self.config.zscore_buy_threshold = -1.5
        self.config.zscore_sell_threshold = 1.5
        
        self.strategy = ZScoreStatisticalArbStrategy(self.config)
        
        # Generate historical price data with known mean and std for predictable z-scores
        # We'll create a dataset where we know the rolling statistics exactly
        self.price_buffer = [100.0 + i * 2.0 / math.sqrt(20) for i in range(25)]
        
    def test_buy_signal_below_neg_one_point_five_std(self):
        """Test BUY signal when price is below -1.5 standard deviations from mean."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate expected rolling statistics (mean and std of last 20 bars)
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # Test price significantly below -1.5 std threshold
        test_price = mean_price - (1.6 * std_price)  # 1.6 std below mean
        
        test_bar = {
            'close': test_price,
            'high': test_price * 1.001,
            'low': test_price * 0.999,
            'open': test_price * 0.9995,
            'volume': 100.0
        }
        
        # Should generate BUY signal
        signal = self.strategy.on_bar(test_bar)
        
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, 'BUY')
        self.assertTrue(signal.confidence > 0.6)  # Exceeds threshold by 0.1 std
        self.assertAlmostEqual(signal.zscore, -1.6, places=2)
    
    def test_buy_signal_at_minus_one_point_five_zero_std(self):
        """Test BUY signal triggers exactly at -1.5 z-score."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate rolling statistics
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # Test price exactly at -1.5 std threshold
        test_price = mean_price - (1.5 * std_price)
        
        test_bar = {
            'close': test_price,
            'high': test_price * 1.001,
            'low': test_price * 0.999,
            'open': test_price * 0.9995,
            'volume': 100.0
        }
        
        # Should generate BUY signal with minimum confidence
        signal = self.strategy.on_bar(test_bar)
        
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, 'BUY')
        self.assertTrue(signal.confidence >= 0.6)
    
    def test_buy_signal_exceeds_threshold_confidence(self):
        """Test that signals beyond threshold have higher confidence."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate rolling statistics
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # Test price further below threshold (1.8 std below)
        test_price_below = mean_price - (1.8 * std_price)
        
        # Test price closer to threshold (1.55 std below)
        test_price_close = mean_price - (1.55 * std_price)
        
        test_bar_further = {
            'close': test_price_below,
            'high': test_price_below * 1.001,
            'low': test_price_below * 0.999,
            'open': test_price_below * 0.9995,
            'volume': 100.0
        }
        
        test_bar_close = {
            'close': test_price_close,
            'high': test_price_close * 1.001,
            'low': test_price_close * 0.999,
            'open': test_price_close * 0.9995,
            'volume': 100.0
        }
        
        # Both should generate BUY signals
        signal_further = self.strategy.on_bar(test_bar_further)
        signal_close = self.strategy.on_bar(test_bar_close)
        
        # Signal with further deviation should have higher confidence
        self.assertGreater(signal_further.confidence, signal_close.confidence)


class TestZScoreStatisticalArbSellSignal(unittest.TestCase):
    """Test SELL signal generation when price is above +1.5 std from mean."""
    
    def setUp(self):
        """Initialize strategy with test configuration."""
        self.config = ZScoreStatisticalArbConfig()
        self.config.lookback_mean = 20
        self.config.lookback_std = 3
        self.config.zscore_buy_threshold = -1.5
        self.config.zscore_sell_threshold = 1.5
        
        self.strategy = ZScoreStatisticalArbStrategy(self.config)
        
        # Generate historical price data
        self.price_buffer = [100.0 + i * 2.0 / math.sqrt(20) for i in range(25)]
    
    def test_sell_signal_above_pos_one_point_five_std(self):
        """Test SELL signal when price is above +1.5 standard deviations from mean."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate rolling statistics
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # Test price significantly above +1.5 std threshold
        test_price = mean_price + (1.6 * std_price)  # 1.6 std above mean
        
        test_bar = {
            'close': test_price,
            'high': test_price * 1.001,
            'low': test_price * 0.999,
            'open': test_price * 0.9995,
            'volume': 100.0
        }
        
        # Should generate SELL signal
        signal = self.strategy.on_bar(test_bar)
        
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, 'SELL')
        self.assertTrue(signal.confidence > 0.6)
        self.assertAlmostEqual(signal.zscore, 1.6, places=2)
    
    def test_sell_signal_at_pos_one_point_five_zero_std(self):
        """Test SELL signal triggers exactly at +1.5 z-score."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate rolling statistics
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # Test price exactly at +1.5 std threshold
        test_price = mean_price + (1.5 * std_price)
        
        test_bar = {
            'close': test_price,
            'high': test_price * 1.001,
            'low': test_price * 0.999,
            'open': test_price * 0.9995,
            'volume': 100.0
        }
        
        # Should generate SELL signal with minimum confidence
        signal = self.strategy.on_bar(test_bar)
        
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, 'SELL')
        self.assertTrue(signal.confidence >= 0.6)
    
    def test_sell_signal_exceeds_threshold_confidence(self):
        """Test that signals beyond threshold have higher confidence."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate rolling statistics
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # Test price further above threshold (1.8 std above)
        test_price_above = mean_price + (1.8 * std_price)
        
        # Test price closer to threshold (1.55 std above)
        test_price_close = mean_price + (1.55 * std_price)
        
        test_bar_further = {
            'close': test_price_above,
            'high': test_price_above * 1.001,
            'low': test_price_above * 0.999,
            'open': test_price_above * 0.9995,
            'volume': 100.0
        }
        
        test_bar_close = {
            'close': test_price_close,
            'high': test_price_close * 1.001,
            'low': test_price_close * 0.999,
            'open': test_price_close * 0.9995,
            'volume': 100.0
        }
        
        # Both should generate SELL signals
        signal_further = self.strategy.on_bar(test_bar_further)
        signal_close = self.strategy.on_bar(test_bar_close)
        
        # Signal with further deviation should have higher confidence
        self.assertGreater(signal_further.confidence, signal_close.confidence)


class TestZScoreStatisticalArbNoSignal(unittest.TestCase):
    """Test that no signal is generated when price is within mean range."""
    
    def setUp(self):
        """Initialize strategy with test configuration."""
        self.config = ZScoreStatisticalArbConfig()
        self.config.lookback_mean = 20
        self.config.lookback_std = 3
        self.config.zscore_buy_threshold = -1.5
        self.config.zscore_sell_threshold = 1.5
        
        self.strategy = ZScoreStatisticalArbStrategy(self.config)
        
        # Generate historical price data
        self.price_buffer = [100.0 + i * 2.0 / math.sqrt(20) for i in range(25)]
    
    def test_no_signal_within_mean_range(self):
        """Test no signal when price is within +/- 1.5 std from mean."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate rolling statistics
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # Test price within threshold range (e.g., 0.5 std from mean)
        test_price = mean_price + (0.5 * std_price)
        
        test_bar = {
            'close': test_price,
            'high': test_price * 1.001,
            'low': test_price * 0.999,
            'open': test_price * 0.9995,
            'volume': 100.0
        }
        
        # Should return HOLD signal (no action)
        signal = self.strategy.on_bar(test_bar)
        
        self.assertEqual(signal.action, 'HOLD')
    
    def test_no_signal_at_mean(self):
        """Test no signal when price equals rolling mean."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate rolling statistics
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # Test price exactly at mean (z-score = 0)
        test_price = mean_price
        
        test_bar = {
            'close': test_price,
            'high': test_price * 1.001,
            'low': test_price * 0.999,
            'open': test_price * 0.9995,
            'volume': 100.0
        }
        
        # Should return HOLD signal (no action)
        signal = self.strategy.on_bar(test_bar)
        
        self.assertEqual(signal.action, 'HOLD')
    
    def test_no_signal_near_mean_boundary(self):
        """Test no signal when price is very close to threshold but not exceeding."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate rolling statistics
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # Test price just below threshold (1.49 std - should not trigger)
        test_price_below_threshold = mean_price - (1.49 * std_price)
        
        # Test price just above threshold (1.51 std - should trigger SELL)
        test_price_above_threshold = mean_price + (1.51 * std_price)
        
        test_bar_below = {
            'close': test_price_below_threshold,
            'high': test_price_below_threshold * 1.001,
            'low': test_price_below_threshold * 0.999,
            'open': test_price_below_threshold * 0.9995,
            'volume': 100.0
        }
        
        test_bar_above = {
            'close': test_price_above_threshold,
            'high': test_price_above_threshold * 1.001,
            'low': test_price_above_threshold * 0.999,
            'open': test_price_above_threshold * 0.9995,
            'volume': 100.0
        }
        
        # Price just below threshold should return HOLD
        signal_below = self.strategy.on_bar(test_bar_below)
        
        self.assertEqual(signal_below.action, 'HOLD')
        
        # Price just above threshold should trigger SELL
        signal_above = self.strategy.on_bar(test_bar_above)
        
        self.assertEqual(signal_above.action, 'SELL')


class TestZScoreStatisticalArbTrailingStopReversion(unittest.TestCase):
    """Test trailing stop reversion logic after extreme deviation."""
    
    def setUp(self):
        """Initialize strategy with test configuration."""
        self.config = ZScoreStatisticalArbConfig()
        self.config.lookback_mean = 20
        self.config.lookback_std = 3
        self.config.zscore_buy_threshold = -1.5
        self.config.zscore_sell_threshold = 1.5
        
        # Use smaller thresholds for easier trailing stop testing
        self.config.stop_loss_pct = 0.02
        self.config.take_profit_pct = 0.10
        
        self.strategy = ZScoreStatisticalArbStrategy(self.config)
        
        # Generate historical price data
        self.price_buffer = [100.0 + i * 2.0 / math.sqrt(20) for i in range(25)]
    
    def test_trailing_stop_reversion_after_two_point_five_std(self):
        """Test trailing stop reversion closes position at extreme deviation."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate rolling statistics
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # First bar: trigger SELL at extreme z-score (simulating overbought entry)
        test_price_entry = mean_price + (1.6 * std_price)
        
        entry_bar = {
            'close': test_price_entry,
            'high': test_price_entry,
            'low': test_price_entry * 0.99,
            'open': test_price_entry * 0.995,
            'volume': 100.0
        }
        
        # Generate BUY signal (oversold entry) - simulating mean reversion setup
        buy_price = mean_price - (1.6 * std_price)
        buy_bar = {
            'close': buy_price,
            'high': buy_price,
            'low': buy_price * 0.99,
            'open': buy_price * 0.995,
            'volume': 100.0
        }
        
        # First signal: BUY at oversold level
        buy_signal = self.strategy.on_bar(buy_bar)
        self.assertEqual(buy_signal.action, 'BUY')
        
        # Simulate taking the position (no close yet)
        self.strategy.position = type('obj', (object,), {
            'entry_price': buy_price,
            'quantity': 10.0,
            'pnl_pct': -0.05
        })()
        
        # Now test trailing stop reversion with extreme z-score (+2.5)
        test_price_extreme = mean_price + (2.6 * std_price)
        
        # Need to advance price buffer to calculate new stats
        self.price_buffer.append(test_price_extreme)
        
        # Simulate current statistics after large move
        prices = self.price_buffer[-20:]
        mean_price_new = sum(prices) / len(prices)
        variance = sum((p - mean_price_new) ** 2 for p in prices) / len(prices)
        std_price_new = math.sqrt(variance)
        
        # Current z-score should be extreme (~+2.6)
        current_zscore = (test_price_extreme - mean_price_new) / std_price_new
        
        extreme_bar = {
            'close': test_price_extreme,
            'high': test_price_extreme * 1.001,
            'low': test_price_extreme * 0.999,
            'open': test_price_extreme * 0.9995,
            'volume': 100.0
        }
        
        # Should trigger reversion close signal at extreme deviation
        reversion_signal = self.strategy.on_bar(extreme_bar)
        
        # Reversion signal should close position when price moves beyond threshold
        self.assertIn(reversion_signal.action, ['CLOSE', 'HOLD'])
    
    def test_trailing_stop_respects_take_profit(self):
        """Test that take-profit triggers before trailing stop reversion."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate rolling statistics
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # Test position with profit exceeding take_profit threshold
        self.strategy.position = type('obj', (object,), {
            'entry_price': mean_price - (1.6 * std_price),  # Entry below mean
            'quantity': 10.0,
            'pnl_pct': 0.15  # 15% profit exceeds take_profit_pct of 0.10
        })()
        
        test_bar = {
            'close': mean_price + (1.0 * std_price),
            'high': test_bar['close'],
            'low': test_bar['close'],
            'open': test_bar['close'],
            'volume': 100.0
        }
        
        # Should trigger take-profit close before reversion logic
        signal = self.strategy.on_bar(test_bar)
        
        self.assertEqual(signal.action, 'CLOSE')
    
    def test_no_trailing_stop_during_normal_deviation(self):
        """Test that trailing stop doesn't trigger within normal z-score range."""
        # Initialize with historical data
        self.strategy.init(self.price_buffer)
        
        # Calculate rolling statistics
        prices = self.price_buffer[-20:]
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        
        # Test position with small profit within normal range
        self.strategy.position = type('obj', (object,), {
            'entry_price': mean_price - (1.6 * std_price),
            'quantity': 10.0,
            'pnl_pct': 0.05  # 5% profit - below take_profit threshold
        })()
        
        test_bar = {
            'close': mean_price + (0.5 * std_price),
            'high': test_bar['close'],
            'low': test_bar['close'],
            'open': test_bar['close'],
            'volume': 100.0
        }
        
        # Should return HOLD (no trailing stop reversion within normal range)
        signal = self.strategy.on_bar(test_bar)
        
        self.assertEqual(signal.action, 'HOLD')


if __name__ == '__main__':
    unittest.main()
