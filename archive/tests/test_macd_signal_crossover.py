"""
Unit Tests for MACD Signal Crossover Strategy

Tests cover:
- Initialization and warmup period
- Entry signals (long and short)
- Exit signals (stop loss, take profit, trend reversal)
- Edge cases and error handling
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from trading_system.strategies.trend.macd_signal_crossover import (
    MACDSignalCrossoverStrategy,
    MACDSignalCrossoverConfig
)


def test_initialization():
    """Test strategy initialization with default config."""
    strategy = MACDSignalCrossoverStrategy()
    
    assert strategy.warmup_complete == False
    assert strategy.current_position is None
    assert strategy.entry_price == 0.0
    assert strategy.stop_loss_price == 0.0
    print("✅ test_initialization passed")


def test_custom_config():
    """Test initialization with custom configuration."""
    config = MACDSignalCrossoverConfig(
        fast_period=10,
        slow_period=20,
        signal_period=7,
        risk_per_trade=0.02
    )
    
    strategy = MACDSignalCrossoverStrategy(config)
    
    assert strategy.config.fast_period == 10
    assert strategy.config.risk_per_trade == 0.02
    print("✅ test_custom_config passed")


def test_warmup_period():
    """Test that signals are disabled during warmup."""
    strategy = MACDSignalCrossoverStrategy()
    
    # During warmup, no signals should be generated
    market_state = {
        'close': 50000.0,
        'macd': 150.0,
        'signal': 100.0,
        'histogram': 50.0,
        'trend_strength': 0.8,
        'bars_since_start': 0
    }
    
    signal = strategy.on_bar(market_state)
    assert signal is None, "Should not generate signal during warmup"
    print("✅ test_warmup_period passed")


def test_entry_signal_long():
    """Test long entry signal generation."""
    # Simulate after warmup
    strategy = MACDSignalCrossoverStrategy()
    strategy.warmup_complete = True
    
    market_state = {
        'close': 50000.0,
        'macd': 200.0,  # Above signal line
        'signal': 100.0,
        'histogram': 100.0,  # Above entry threshold (2%)
        'trend_strength': 0.8,  # Strong trend
        'bars_since_start': 30
    }
    
    signal = strategy.on_bar(market_state)
    
    assert signal is not None, "Should generate long entry signal"
    assert signal['action'] == 'open', "Action should be open"
    assert signal['quantity'] > 0, "Long position should have positive quantity"
    print("✅ test_entry_signal_long passed")


def test_entry_signal_short():
    """Test short entry signal generation."""
    strategy = MACDSignalCrossoverStrategy()
    strategy.warmup_complete = True
    
    market_state = {
        'close': 50000.0,
        'macd': -200.0,  # Below signal line
        'signal': -100.0,
        'histogram': -100.0,  # Below exit threshold (-2%)
        'trend_strength': 0.8,
        'bars_since_start': 30
    }
    
    signal = strategy.on_bar(market_state)
    
    assert signal is not None, "Should generate short entry signal"
    assert signal['action'] == 'open', "Action should be open"
    assert signal['quantity'] < 0, "Short position should have negative quantity"
    print("✅ test_entry_signal_short passed")


def test_stop_loss_exit():
    """Test stop loss exit."""
    strategy = MACDSignalCrossoverStrategy()
    strategy.warmup_complete = True
    strategy.current_position = 'long'
    strategy.entry_price = 50000.0
    strategy.stop_loss_price = 49500.0  # 0.5% below entry
    
    market_state = {
        'close': 49400.0,  # Below stop loss
        'macd': 100.0,
        'signal': 80.0,
        'histogram': 20.0,
        'trend_strength': 0.6,
        'bars_since_start': 35
    }
    
    signal = strategy.on_bar(market_state)
    
    assert signal is not None, "Should generate exit signal"
    assert signal['action'] == 'close', "Action should be close"
    assert signal['quantity'] < 0, "Closing long position needs negative quantity"
    print("✅ test_stop_loss_exit passed")


def test_take_profit_exit():
    """Test take profit exit."""
    strategy = MACDSignalCrossoverStrategy()
    strategy.warmup_complete = True
    strategy.current_position = 'long'
    strategy.entry_price = 50000.0
    strategy.stop_loss_price = 49500.0
    strategy.take_profit_price = 51500.0  # 3% above entry (1.5%)
    
    market_state = {
        'close': 51600.0,  # Above take profit
        'macd': 100.0,
        'signal': 80.0,
        'histogram': 20.0,
        'trend_strength': 0.6,
        'bars_since_start': 40
    }
    
    signal = strategy.on_bar(market_state)
    
    assert signal is not None, "Should generate exit signal"
    assert signal['action'] == 'close', "Action should be close"
    print("✅ test_take_profit_exit passed")


def test_trend_reversal_long():
    """Test trend reversal exit for long position."""
    strategy = MACDSignalCrossoverStrategy()
    strategy.warmup_complete = True
    strategy.current_position = 'long'
    
    market_state = {
        'close': 50000.0,
        'macd': -150.0,  # Below signal line (reversal)
        'signal': -100.0,
        'histogram': -50.0,  # Below exit threshold
        'trend_strength': 0.6,
        'bars_since_start': 45
    }
    
    signal = strategy.on_bar(market_state)
    
    assert signal is not None, "Should generate exit signal"
    print("✅ test_trend_reversal_long passed")


def test_no_position_no_signal():
    """Test no signal when conditions are marginal."""
    strategy = MACDSignalCrossoverStrategy()
    strategy.warmup_complete = True
    
    market_state = {
        'close': 50000.0,
        'macd': 180.0,  # Close to but below entry threshold
        'signal': 120.0,
        'histogram': 60.0,  # Below entry threshold (2%)
        'trend_strength': 0.4,  # Below minimum trend strength
        'bars_since_start': 30
    }
    
    signal = strategy.on_bar(market_state)
    
    assert signal is None, "Should not generate signal with weak trend"
    print("✅ test_no_position_no_signal passed")


def test_metadata():
    """Test metadata generation."""
    strategy = MACDSignalCrossoverStrategy()
    
    metadata = strategy.metadata()
    
    assert 'strategy_id' in metadata
    assert metadata['strategy_id'] == 'macd_signal_crossover'
    assert 'family' in metadata
    assert metadata['family'] == 'Trend Following'
    print("✅ test_metadata passed")


def run_all_tests():
    """Run all unit tests."""
    print("Running MACD Signal Crossover Strategy Unit Tests\n")
    print("=" * 60)
    
    try:
        test_initialization()
        test_custom_config()
        test_warmup_period()
        test_entry_signal_long()
        test_entry_signal_short()
        test_stop_loss_exit()
        test_take_profit_exit()
        test_trend_reversal_long()
        test_no_position_no_signal()
        test_metadata()
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED (10/10)\n")
        return True
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}\n")
        return False
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}\n")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
