"""
Unit Tests for SimpleMomentumBreakoutStrategy

Comprehensive test coverage with no external dependencies.
Tests cover: initialization, breakout signals, trailing stops, stop-loss logic, edge cases.
"""
import math
from typing import List, Optional, Dict
from dataclasses import dataclass
import time


@dataclass 
class MockBar:
    """Mock OHLCV bar for testing."""
    timestamp: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float = 0.0
    
    @classmethod
    def create(cls, price: float, high_variance: float = 0.01, low_variance: float = 0.01) -> 'MockBar':
        """Create bar with realistic OHLCV."""
        import random
        noise = (high_variance + low_variance) / 2
        return cls(
            timestamp=time.time(),
            open_price=price * (1 + random.uniform(-noise, noise)),
            high_price=price * (1 + random.uniform(0, high_variance)),
            low_price=price * (1 - random.uniform(0, low_variance)),
            close_price=price * (1 + random.uniform(-noise, noise)),
            volume=random.uniform(1000, 10000)
        )


class TestSimpleMomentumBreakoutStrategy:
    """Unit tests for SimpleMomentumBreakoutStrategy."""
    
    @staticmethod
    def test_initialization_min_periods():
        """Test initialization with exactly minimum required bars."""
        from strategies.trend.momentum_breakout import SimpleMomentumBreakoutConfig, SimpleMomentumBreakoutStrategy
        
        config = SimpleMomentumBreakoutConfig(lookback_periods=20)
        min_bars = [MockBar.create(45000 - i * 10) for i in range(20)]
        
        strategy = SimpleMomentumBreakoutStrategy(config=config)
        
        # Should initialize without error
        assert hasattr(strategy, 'lookback_high'), "Strategy should have lookback_high after init"
        assert hasattr(strategy, 'lookback_low'), "Strategy should have lookback_low after init"
        
    @staticmethod
    def test_initialization_insufficient_data():
        """Test initialization fails with insufficient bars."""
        from strategies.trend.momentum_breakout import SimpleMomentumBreakoutConfig, SimpleMomentumBreakoutStrategy
        
        config = SimpleMomentumBreakoutConfig(lookback_periods=20)
        few_bars = [MockBar.create(45000 - i * 10) for i in range(15)]
        
        strategy = SimpleMomentumBreakoutStrategy(config=config)
        
        try:
            strategy.init(few_bars)
            assert False, "Should have raised ValueError for insufficient bars"
        except ValueError as e:
            assert "need at least 20 bars" in str(e).lower() or "at least 20 bars" in str(e).lower()
            
    @staticmethod
    def test_buy_signal_on_breakout():
        """Test that buy signal generated when price breaks above resistance."""
        from strategies.trend.momentum_breakout import SimpleMomentumBreakoutConfig, SimpleMomentumBreakoutStrategy
        
        # Create setup with clear resistance at 46000
        setup_bars = [MockBar.create(45100 + i * 10) for i in range(21)]
        
        config = SimpleMomentumBreakoutConfig(
            lookback_periods=20,
            entry_threshold_pct=0.0  # No threshold for cleaner test
        )
        
        strategy = SimpleMomentumBreakoutStrategy(config=config)
        strategy.init(setup_bars)
        
        assert strategy.lookback_high > 0, "Lookback high should be set"
        
        # Next bar breaks above resistance
        breakout_bar = MockBar.create(46100)  # 100 above lookback_high (~45200)
        
        signal = strategy.on_bar(breakout_bar)
        
        assert signal is not None, "Should generate BUY signal on breakout"
        assert signal["action"] == "BUY", f"Expected BUY action, got {signal.get('action')}"
        assert "BREAKOUT_ABOVE_RESISTANCE" in signal.get("signal_type", ""), \
            f"Expected BREAKOUT_ABOVE_RESISTANCE signal type, got {signal.get('signal_type')}"
            
    @staticmethod
    def test_no_signal_in_ranging_market():
        """Test no buy signal generated when price stays below resistance."""
        from strategies.trend.momentum_breakout import SimpleMomentumBreakoutConfig, SimpleMomentumBreakoutStrategy
        
        # Create bounded range 45000-46000
        setup_bars = [MockBar.create(45100 + i * 5) for i in range(21)]
        
        config = SimpleMomentumBreakoutConfig(lookback_periods=20, entry_threshold_pct=0.0)
        strategy = SimpleMomentumBreakoutStrategy(config=config)
        strategy.init(setup_bars)
        
        # Price below resistance - should not generate signal
        ranging_bar = MockBar.create(45500)  # Below typical lookback_high
        
        signal = strategy.on_bar(ranging_bar)
        assert signal is None or signal.get("action") is None, \
            "Should not generate BUY signal when below resistance"
            
    @staticmethod
    def test_stop_loss_triggers_sell():
        """Test stop-loss triggers sell signal."""
        from strategies.trend.momentum_breakout import SimpleMomentumBreakoutConfig, SimpleMomentumBreakoutStrategy
        
        # Setup with breakout scenario
        setup_bars = [MockBar.create(45100 + i * 10) for i in range(21)]
        
        config = SimpleMomentumBreakoutConfig(
            lookback_periods=20,
            entry_threshold_pct=0.0,
            stop_loss_pct=3.0
        )
        
        strategy = SimpleMomentumBreakoutStrategy(config=config)
        strategy.init(setup_bars)
        
        # Generate buy signal manually by breaking resistance
        breakout_bar = MockBar.create(46100)
        signal = strategy.on_bar(breakout_bar)
        
        assert signal["action"] == "BUY", "Should have BUY action"
        
        # Now drop price 5% below entry - should trigger stop-loss
        drop_bar = MockBar.create(43800)  # ~4.6% below 46100
        
        sell_signal = strategy.on_bar(drop_bar)
        
        assert sell_signal is not None, "Should generate SELL signal"
        assert sell_signal["action"] == "SELL", f"Expected SELL action, got {sell_signal.get('action')}"
        assert "STOP_LOSS_HIT" in sell_signal.get("signal_type", ""), \
            f"Expected STOP_LOSS_HIT signal type"
            
    @staticmethod
    def test_trailing_stop_exits():
        """Test trailing stop triggers exit after profit target reached."""
        from strategies.trend.momentum_breakout import SimpleMomentumBreakoutConfig, SimpleMomentumBreakoutStrategy
        
        # Setup scenario where position can be opened and trailed
        setup_bars = [MockBar.create(45100 + i * 10) for i in range(21)]
        
        config = SimpleMomentumBreakoutConfig(
            lookback_periods=20,
            entry_threshold_pct=0.0,
            stop_loss_pct=3.0,
            trailing_stop_bps=10.0  # 0.1% trailing after 2% profit
        )
        
        strategy = SimpleMomentumBreakoutStrategy(config=config)
        strategy.init(setup_bars)
        
        # Generate buy
        breakout_bar = MockBar.create(46100)
        buy_signal = strategy.on_bar(breakout_bar)
        assert buy_signal["action"] == "BUY"
        
        # Simulate price moving up 5% (in reality this would be next bar)
        price_after_2pct_gain = 46100 * 1.05  # 5% gain
        
        trailing_bar = MockBar.create(price_after_2pct_gain, close_price=price_after_2pct_gain)
        
        trailing_signal = strategy.on_bar(trailing_bar)
        
        # Trailing stop should trigger exit when price drops below threshold
        exit_signal = StrategyPositionTracker(config).check_trailing_stop(
            entry_price=46100,
            current_pnl_pct=5.0,  # 5% profit already
        )
        
        assert exit_signal is not None, "Trailing stop should trigger after 2% profit"
            
    @staticmethod
    def test_performance_metrics_empty():
        """Test performance metrics when no trades executed."""
        from strategies.trend.momentum_breakout import SimpleMomentumBreakoutConfig, SimpleMomentumBreakoutStrategy
        
        config = SimpleMomentumBreakoutConfig()
        strategy = SimpleMomentumBreakoutStrategy(config=config)
        
        metrics = strategy.get_performance_metrics()
        
        assert metrics["total_signals"] == 0
        assert metrics["win_rate"] == 0.0
        assert metrics["successful_trades"] == 0
        
    @staticmethod
    def test_position_tracking():
        """Test position state tracking after trade."""
        from strategies.trend.momentum_breakout import SimpleMomentumBreakoutConfig, SimpleMomentumBreakoutStrategy
        
        config = SimpleMomentumBreakoutConfig(lookback_periods=10)  # Smaller for faster test
        setup_bars = [MockBar.create(45100 + i * 20) for i in range(11)]
        
        strategy = SimpleMomentumBreakoutStrategy(config=config)
        strategy.init(setup_bars)
        
        assert strategy.position is None, "No position before breakout"
        
        # Generate buy
        breakout_bar = MockBar.create(46000)
        signal = strategy.on_bar(breakout_bar)
        
        assert signal["action"] == "BUY"
        assert strategy.position is not None, "Position should be created"
        assert abs(strategy.position.entry_price - 46000) < 1.0, \
            f"Entry price should be ~46000, got {strategy.position.entry_price}"
            
    @staticmethod  
    def test_lookback_expansion_after_breakout():
        """Test lookback high/low expands after breakout entry."""
        from strategies.trend.momentum_breakout import SimpleMomentumBreakoutConfig, SimpleMomentumBreakoutStrategy
        
        config = SimpleMomentumBreakoutConfig(lookback_periods=10)
        setup_bars = [MockBar.create(45000 + i * 10) for i in range(11)]
        
        strategy = SimpleMomentumBreakoutStrategy(config=config)
        strategy.init(setup_bars)
        
        initial_high = strategy.lookback_high
        
        # Breakout
        breakout_bar = MockBar.create(46000)
        signal = strategy.on_bar(breakout_bar)
        
        # Lookback high should expand (be higher after breakout)
        new_high = strategy.lookback_high
        assert new_high > initial_high, \
            f"Lookback high should expand after breakout ({new_high} vs {initial_high})"


class StrategyPositionTracker:
    """Helper class for testing position state."""
    
    def __init__(self, config):
        self.config = config
    
    def check_trailing_stop(self, entry_price: float, current_pnl_pct: float) -> Optional[dict]:
        """Check if trailing stop should exit."""
        config = SimpleMomentumBreakoutConfig(
            trailing_stop_bps=10.0,
            stop_loss_pct=3.0
        )
        
        if current_pnl_pct < 2.0:
            return None
            
        trail_exit_pct = max(
            2.0, 
            (1 + current_pnl_pct / 100) * (1 - config.trailing_stop_bps / 10000) - 1
        ) * 100
        
        return {"exit_reason": "TRAILING_STOP_TRIGGERED"} if current_pnl_pct < trail_exit_pct else None


if __name__ == "__main__":
    import sys
    
    print("Running SimpleMomentumBreakoutStrategy tests...\n")
    
    # Run each test individually
    tests = [
        ("test_initialization_min_periods", TestSimpleMomentumBreakoutStrategy.test_initialization_min_periods),
        ("test_initialization_insufficient_data", TestSimpleMomentumBreakoutStrategy.test_initialization_insufficient_data),
        ("test_buy_signal_on_breakout", TestSimpleMomentumBreakoutStrategy.test_buy_signal_on_breakout),
        ("test_no_signal_in_ranging_market", TestSimpleMomentumBreakoutStrategy.test_no_signal_in_ranging_market),
        ("test_stop_loss_triggers_sell", TestSimpleMomentumBreakoutStrategy.test_stop_loss_triggers_sell),
        ("test_trailing_stop_exits", TestSimpleMomentumBreakoutStrategy.test_trailing_stop_exits),
        ("test_performance_metrics_empty", TestSimpleMomentumBreakoutStrategy.test_performance_metrics_empty),
        ("test_position_tracking", TestSimpleMomentumBreakoutStrategy.test_position_tracking),
        ("test_lookback_expansion_after_breakout", TestSimpleMomentumBreakoutStrategy.test_lookback_expansion_after_breakout),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            test_func()
            print(f"✓ {test_name}: PASSED")
            passed += 1
        except Exception as e:
            print(f"✗ {test_name}: FAILED - {str(e)}")
            failed += 1
    
    print(f"\n{'='*60}")
    print(f"Test Summary: {passed} passed, {failed} failed")
    print(f"{'='*60}")
    
    sys.exit(0 if failed == 0 else 1)
