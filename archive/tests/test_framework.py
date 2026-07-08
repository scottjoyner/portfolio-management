"""
Unit Test Framework - Trading Strategies
========================================

This module provides comprehensive unit tests for all trading strategies.
Includes mock data generation, test fixtures, and performance benchmarking.
"""
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
import unittest
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import math
import random


class MockMarketDataGenerator:
    """
    Generates realistic mock market data for testing strategies.
    
    Features:
    - Configurable price movements (trending, ranging, volatile)
    - Realistic OHLCV structure
    - Volume patterns and volatility regimes
    - Historical replay capability
    """
    
    def __init__(self, config=None):
        self.config = config or MockMarketDataConfig()
        self.current_price = 50000.0
        self.price_history: List[float] = []
    
    class MockMarketDataConfig:
        """Configuration for mock data generation."""
        start_date: str = '2024-01-01'
        end_date: str = '2024-06-30'
        bar_frequency: str = 'hourly'  # 'hourly', 'daily', 'weekly'
        trend_strength: float = 0.0    # Positive for uptrend, negative for downtrend
        volatility_regime: str = 'normal'  # 'low', 'normal', 'high'
        volume_multiplier: float = 1.0
    
    def generate_ohlcv_data(self) -> List[Dict[str, Any]]:
        """
        Generate realistic OHLCV data.
        
        Returns:
            List of dictionaries with keys: timestamp, open, high, low, close, volume
        """
        start = datetime.strptime(self.config.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.config.end_date, '%Y-%m-%d')
        
        data = []
        current_time = start
        self.current_price = 50000.0
        self.price_history = [self.current_price]
        
        while current_time <= end:
            # Calculate price movement based on trend and volatility
            daily_return = self._calculate_daily_return()
            
            open_price = self.current_price
            close_price = open_price * (1 + daily_return)
            high_price = max(open_price, close_price) + abs(random.gauss(0, 20))
            low_price = min(open_price, close_price) - abs(random.gauss(0, 20))
            
            # Generate realistic volume
            base_volume = random.randint(1000, 5000)
            vol_factor = max(0.5, min(3.0, abs(daily_return) + 1))
            volume = int(base_volume * self.config.volume_multiplier * vol_factor)
            
            data.append({
                'timestamp': current_time.isoformat(),
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': volume,
            })
            
            self.current_price = close_price
            self.price_history.append(self.current_price)
            current_time += timedelta(days=1) if self.config.bar_frequency == 'daily' else timedelta(hours=6)
        
        return data
    
    def _calculate_daily_return(self) -> float:
        """Calculate daily return based on trend and random noise."""
        volatility = {'low': 0.01, 'normal': 0.02, 'high': 0.05}[self.config.volatility_regime]
        trend_component = self.config.trend_strength * volatility
        noise_component = random.gauss(0, volatility)
        return trend_component + noise_component
    
    def generate_trending_data(self, direction: str = 'up') -> List[Dict[str, Any]]:
        """
        Generate data with strong trending behavior.
        
        Args:
            direction: 'up' for uptrend, 'down' for downtrend
        """
        self.config.trend_strength = 0.15 if direction == 'up' else -0.15
        return self.generate_ohlcv_data()
    
    def generate_ranging_data(self) -> List[Dict[str, Any]]:
        """
        Generate data with ranging/consolidation behavior.
        """
        self.config.trend_strength = 0.0
        return self.generate_ohlcv_data()
    
    def generate_volatile_data(self) -> List[Dict[str, Any]]:
        """
        Generate data with high volatility regime.
        """
        self.config.volatility_regime = 'high'
        return self.generate_ohlcv_data()


class StrategyTestBase(unittest.TestCase):
    """
    Base class for strategy unit tests.
    
    Provides common test fixtures and utilities.
    """
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_data = MockMarketDataGenerator()
        self.test_data = self.mock_data.generate_ohlcv_data()
    
    def tearDown(self):
        """Clean up after tests."""
        pass
    
    def get_strategy_class(self, strategy_name: str) -> Any:
        """
        Import and return the specified strategy class.
        
        Args:
            strategy_name: Name of the strategy module (e.g., 'regime_detection')
            
        Returns:
            Strategy class object
        """
        import importlib
        module = importlib.import_module(f'trading_system.strategies.{strategy_name}')
        return getattr(module, 'RegimeDetectionStrategy' if strategy_name == 'ml/regime_detection' else 
                          getattr(module, 'StrategyName', None))
    
    def run_strategy(self, strategy: Any, data: List[Dict[str, Any]] = None) -> List[Any]:
        """
        Run a strategy on historical data and collect signals.
        
        Args:
            strategy: Strategy instance
            data: Historical OHLCV data (uses test_data if not provided)
            
        Returns:
            List of signals generated by the strategy
        """
        if data is None:
            data = self.test_data
        
        results = []
        for bar in data:
            signal = strategy.on_bar(bar)
            if signal:
                results.append(signal)
        
        return results
    
    def assert_signal_exists(self, signals: List[Any], action: str) -> None:
        """
        Assert that at least one signal with the specified action exists.
        """
        self.assertTrue(
            any(s.get('action') == action for s in signals),
            f'Expected to find signal with action={action}, got: {signals[:3]}'
        )


class MockMarketDataConfig:
    """Configuration for mock data generation."""
    start_date: str = '2024-01-01'
    end_date: str = '2024-06-30'
    bar_frequency: str = 'hourly'
    trend_strength: float = 0.0
    volatility_regime: str = 'normal'
    volume_multiplier: float = 1.0