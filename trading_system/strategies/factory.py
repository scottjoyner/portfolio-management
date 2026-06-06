"""
Strategy Factory Pattern - Unified Interface for All Trading Strategies

This module provides a standard lifecycle for all trading strategies in the system:
- init(): Initialize strategy with data and configuration
- on_bar(bar): Generate signal on each new bar of data  
- on_order_fills(fill_data): Handle position updates
- finalize(): Close position/cleanup on exit

All strategies MUST implement these methods to be compatible with the factory.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Callable, Any
import time
from datetime import datetime


@dataclass
class StrategyConfig:
    """Base strategy configuration - subclasses can override."""
    
    name: str = "GenericStrategy"
    enable_logging: bool = True
    position_size_usd: float = 1000.0
    risk_limit_pct: float = 0.05  # Maximum portfolio drawdown allowed for this strategy
    cooldown_period_hours: int = 0  # Minimum hours before re-entry after stop


@dataclass  
class Signal:
    """Standardized buy/sell signal output."""
    
    action: str  # "BUY", "SELL", "CLOSE", "HOLD"
    price: Optional[float] = None
    quantity: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    confidence: float = 1.0  # Strategy conviction (0-1)


@dataclass
class BacktestResult:
    """Standardized backtesting result format."""
    
    strategy_name: str
    start_date: str
    end_date: str
    total_return_pct: float
    sharpe_ratio: float
    max_drawdown_pct: float
    win_rate: float
    profit_factor: float
    num_trades: int
    realized_pnl: float


# Strategy registry for type-safe registration
_strategy_registry: dict[str, Callable] = {}

def register_strategy(strategy_class):
    """Decorator to register a strategy with the factory."""
    def wrapper(cls):
        name = cls.__name__
        _strategy_registry[name] = lambda config: create_strategy_instance(cls, config)
        print(f"[StrategyRegistry] Registered: {name}")
        return cls
    return wrapper


def create_strategy_instance(strategy_class, config: StrategyConfig = None):
    """Factory function to create strategy instance from class."""
    if config is None:
        config = StrategyConfig()
    instance = strategy_class(config)
    instance._registered_name = strategy_class.__name__
    return instance


class StrategyBase(ABC):
    """Abstract base class for all trading strategies."""
    
    def __init__(self, config: Optional[StrategyConfig] = None):
        self.config = config or StrategyConfig()
        self.position = None
        self.entry_price = None
        self.current_pnl = 0.0
        self.num_trades = 0
        self.signals: List[Signal] = []
        self.last_signal_time = 0
        self._registered_name = self.__class__.__name__
        
    @abstractmethod
    def init(self, data: dict) -> None:
        """Initialize strategy with market data. Override in subclasses."""
        pass
        
    @abstractmethod  
    def on_bar(self, bar: dict) -> Optional[Signal]:
        """Generate signal from new bar. Returns Signal or None (hold)."""
        return None
        
    def on_order_fills(self, fills: List[dict]) -> None:
        """Handle order execution updates. Override if needed."""
        pass
        
    def finalize(self) -> dict:
        """Close position/cleanup. Override if strategy manages positions."""
        return {}
        
    def get_name(self) -> str:
        """Return strategy name for logging/identification."""
        return self._registered_name if hasattr(self, '_registered_name') else self.config.name


# Pre-registered strategies with factory compatibility
class TrendBreakoutStrategy(StrategyBase):
    """Trend-following breakout strategy example."""
    
    def __init__(self, config: StrategyConfig = None):
        super().__init__(config)
        
    def init(self, data: dict, config: StrategyConfig = None) -> None:
        """Initialize with price history."""
        # Store any initialization state needed
        self.lookback_periods = 20
        
    def on_bar(self, bar: dict, config: StrategyConfig = None) -> Optional[Signal]:
        """Detect breakout and generate buy signal."""
        # Simplified logic - implement full logic in subclass
        return None


class ZScoreMeanReversionStrategy(StrategyBase):
    """Z-score based mean reversion strategy example."""
    
    def __init__(self, config: StrategyConfig = None):
        super().__init__(config)
        self.lookback_bars = 60
        self.z_buy_threshold = -2.5
        self.z_sell_threshold = 2.5
        
    def init(self, data: dict) -> None:
        """Initialize with price history."""
        pass
        
    def on_bar(self, bar: dict) -> Optional[Signal]:
        """Generate buy/sell based on z-score extremes."""
        # Placeholder - implement z-score calculation logic
        return None


class EMA_CrossoverStrategy(StrategyBase):
    """Exponential moving average crossover strategy example."""
    
    def __init__(self, config: StrategyConfig = None):
        super().__init__(config)
        self.fast_period = 9
        self.slow_period = 21
        
    def init(self, data: dict) -> None:
        """Initialize EMA calculation with price history."""
        pass
        
    def on_bar(self, bar: dict) -> Optional[Signal]:
        """Generate signal on EMA crossover."""
        # Placeholder - implement EMA crossover logic
        return None


# Available strategies list for dynamic loading
AVAILABLE_STRATEGIES = {
    "trend_breakout": TrendBreakoutStrategy,
    "zscore_mean_reversion": ZScoreMeanReversionStrategy, 
    "ema_crossover": EMA_CrossoverStrategy,
}

__all__ = [
    'StrategyConfig',
    'Signal',
    'BacktestResult',
    'StrategyBase',
    'register_strategy',
    'create_strategy_instance',
    'AVAILABLE_STRATEGIES',
]
