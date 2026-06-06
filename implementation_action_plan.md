# IMMEDIATE ACTION PLAN: Strategy Factory + Backtester Buildout

## This Week (Days 1-7): Core Infrastructure Foundation

### Day 1-2: Strategy Factory Pattern Setup

**File**: `trading_system/strategies/factory.py`

```python
"""
Strategy Factory - Unified Interface for All Trading Strategies

The factory provides a standard lifecycle for all strategies:
- init(): Initialize strategy with data and configuration
- on_bar(bar): Generate signal on each new bar of data
- on_order_fills(fill_data): Handle position updates
- finalize(): Close position/cleanup on exit

All strategies MUST implement these methods to be compatible.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Callable, Any
import time

@dataclass
class StrategyConfig:
    """Base strategy configuration - subclasses can override."""
    name: str = "GenericStrategy"
    enable_logging: bool = True
    position_size_usd: float = 1000.0
    
@dataclass  
class Signal:
    """Standardized buy/sell signal output."""
    action: str  # "BUY", "SELL", "CLOSE", "HOLD"
    price: Optional[float] = None
    quantity: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    
@dataclass
class BacktestResult:
    """Standardized backtesting result format."""
    start_date: str
    end_date: str
    total_return_pct: float
    sharpe_ratio: float
    max_drawdown_pct: float
    win_rate: float
    profit_factor: float
    
# Strategy registry for type-safe registration
_strategy_registry: dict[str, Callable] = {}

def register_strategy(strategy_class):
    """Decorator to register a strategy with the factory."""
    def wrapper(cls):
        name = cls.__name__
        _strategy_registry[name] = lambda config: StrategyBase(config)
        return cls
    return wrapper

class StrategyBase(ABC):
    """Abstract base class for all trading strategies."""
    
    def __init__(self, config: Optional[StrategyConfig] = None):
        self.config = config or StrategyConfig()
        self.position = None
        self.entry_price = None
        self.signals: List[Signal] = []
        self.last_bar_time: float = 0
        
    @abstractmethod
    def init(self, data: dict) -> None:
        """Initialize strategy with market data. Override in subclasses."""
        pass
        
    @abstractmethod  
    def on_bar(self, bar: dict) -> Optional[Signal]:
        """Generate signal from new bar. Returns Signal or None (hold)."""
        return None
        
    def on_order_fills(self, fills: List[dict]) -> None:
        """Handle order execution updates. Optional override."""
        pass
        
    def finalize(self) -> dict:
        """Close position/cleanup. Override if strategy manages positions."""
        return {}
        
    def get_name(self) -> str:
        return self.config.name

# Example concrete strategies registered with factory
from .trend.breakout import BreakoutStrategy
from .mean_reversion.zscore import ZScoreStrategy
from .emacrossor_strategy import EMACrossoverStrategy

@register_strategy(BreakoutStrategy)
class TrendBreakoutStrategy(StrategyBase):
    """Decorator-wrapped breakout strategy."""
    def __init__(self, config: StrategyConfig = None):
        super().__init__(config)
        
    def get_name(self) -> str:
        return "TrendBreakout"

# Available strategies for factory usage
AVAILABLE_STRATEGIES = {
    "breakout": TrendBreakoutStrategy,
    "zscore": ZScoreStrategy, 
    "ema_crossover": EMACrossoverStrategy,
}
```

### Day 2-3: Unified Data Interface Enhancement

**File**: `trading_system/connectors/unified.py`

Enhance existing unified_price_fetcher to support all exchanges:

```python
"""
Unified Price Fetcher - Multi-exchange data aggregation layer

Supports: Binance, Coinbase Prime, Bybit, Kraken (expandable)
Provides standardized OHLCV + real-time tick data interface
"""
from abc import ABC, abstractmethod
from typing import Optional, List
from datetime import datetime

class ExchangeConnector(ABC):
    """Abstract base for exchange-specific connectors."""
    
    @abstractmethod
    async def get_ohlcv(self, symbol: str, 
                       timeframe: str = "1h",
                       start_time: float = None,
                       end_time: float = None) -> List[dict]:
        """Fetch historical OHLCV data in unified format."""
        pass
        
    @abstractmethod
    async def get_latest_bar(self, symbol: str) -> dict:
        """Get most recent bar (timestamp, open, high, low, close, volume)."""
        pass
        
    @abstractmethod
    async def subscribe_websocket(self, symbols: List[str]) -> callable:
        """Return callback function for streaming data."""
        pass
    
    @abstractmethod  
    def get_symbols(self) -> List[str]:
        """Return list of available trading pairs."""
        pass

class BinanceConnector(ExchangeConnector):
    """Binance API connector implementation."""
    # Full implementation with rate limiting, websocket streaming...
    
class CoinbasePrimeConnector(ExchangeConnector):
    """Coinbase Prime institutional API."""
    pass
    
class BybitConnector(ExchangeConnector):
    """Bybit spot market connector."""
    pass

class UnifiedPriceFetcher:
    """
    Factory for multi-exchange price fetching.
    Uses best-of-all-exchanges logic per symbol.
    """
    
    def __init__(self, exchanges: List[str] = None):
        self.exchange_classes = {
            "binance": BinanceConnector,
            "coinbase": CoinbasePrimeConnector, 
            "bybit": BybitConnector,
            "kraken": KrakenConnector,
        }
        self.active_connectors = {}
        
    def connect(self, exchange_name: str) -> ExchangeConnector:
        """Connect to specific exchange."""
        connector_class = self.exchange_classes.get(exchange_name.lower())
        if not connector_class:
            raise ValueError(f"Unknown exchange: {exchange_name}")
        return connector_class()
```

### Day 4-5: Event-Driven Backtester Core

**File**: `trading_system/backtesters/engine.py`

```python
"""
Event-Driven Backtesting Engine

Processes market data events in chronological order, executes strategy signals,
and calculates performance metrics including drawdowns, sharpe ratio, etc.
"""
from typing import List, Dict, Optional
from datetime import datetime
import time

@dataclass 
class MarketEvent:
    """Standardized market event for backtesting."""
    timestamp: float
    symbol: str
    price: float
    volume: float
    event_type: str  # "OHLCV", "TRADEREPORTED"

@dataclass
class OrderEvent:  
    """Order execution event with fill details."""
    order_id: str
    timestamp: float
    symbol: str
    side: str
    filled_quantity: float
    filled_price: float

class BacktestEngine:
    """
    Event-driven backtester with realistic slippage and fees.
    """
    
    def __init__(self, 
                 initial_capital: float = 10000.0,
                 fee_pct: float = 0.001,  # 0.1% per trade
                 slippage_bps: float = 2.0,  # 2 bps typical
                 ):
        self.initial_capital = initial_capital
        self.fee_pct = fee_pct
        self.slippage_bps = slippage_bps
        self.portfolio_value = initial_capital
        self.held_positions: Dict[str, dict] = {}
        self.events: List[MarketEvent] = []
        self.order_events: List[OrderEvent] = []
        
    def load_market_data(self, data: List[dict]) -> None:
        """Load OHLCV data into chronological order."""
        for bar in sorted(data, key=lambda x: x["timestamp"]):
            event = MarketEvent(
                timestamp=bar["timestamp"],
                symbol=bar["symbol"],
                price=bar["close"],
                volume=bar["volume"],
                event_type="OHLCV"
            )
            self.events.append(event)
            
    def initialize_strategy(self, strategy: StrategyBase, 
                          config: dict) -> None:
        """Initialize backtestable strategy."""
        strategy.init(config or {"symbol": "BTC-USD"})
        
    def step(self) -> Optional[dict]:
        """
        Process one market event and execute any pending signals.
        Returns order details if executed, None otherwise.
        """
        if not self.events:
            return None
            
        event = self.events.pop(0)
        
        # Check all active strategies for new signals
        for symbol, strategy in self.active_strategies.items():
            signal = strategy.on_bar(event)
            
            if signal and signal.action != "HOLD":
                order_result = self.execute_signal(
                    symbol=symbol,
                    signal=signal,
                    price=event.price
                )
                
                if order_result:
                    self.order_events.append(order_result)
                    
        return None
        
    def execute_signal(self, symbol: str, 
                      signal: Signal,
                      market_price: float) -> Optional[dict]:
        """
        Execute trade with realistic slippage and fees.
        Returns OrderEvent or None (cancelled/held).
        """
        # Calculate filled price with slippage
        fill_price = market_price * (1 + self.slippage_bps / 10000) if signal.action in ["BUY", "HOLD"] else \
                     market_price * (1 - self.slippage_bps / 10000)
                     
        # Calculate fee-adjusted quantity
        trade_value = abs(signal.quantity * fill_price)
        fee_deduction_pct = self.fee_pct
        effective_value = trade_value * (1 - fee_deduction_pct)
        
        # Update portfolio/positions with proper PnL tracking...
        return OrderEvent(
            order_id=f"{symbol}-{int(time.time())}",
            timestamp=market_price,  # simplified
            symbol=symbol,
            side=signal.action,
            filled_quantity=signal.quantity,
            filled_price=fill_price
        )
```

### Day 6-7: Metrics Calculator & Test Suite Setup

**File**: `trading_system/backtesters/metrics.py`

```python
"""
Performance Metrics Calculator for backtest results.
"""

class PerformanceMetrics:
    """Calculate comprehensive performance metrics."""
    
    def __init__(self, orders: List[dict], 
                 portfolio_values: List[float]):
        self.orders = orders
        self.portfolio_values = portfolio_values
        
    @property
    def total_return_pct(self) -> float:
        """Total cumulative return as percentage."""
        final_value = self.portfolio_values[-1] if self.portfolio_values else 0
        initial_value = self.portfolio_values[0] if self.portfolio_values else 1
        return ((final_value - initial_value) / initial_value) * 100
        
    @property  
    def sharpe_ratio(self) -> float:
        """Annualized Sharpe ratio (assuming 252 trading days)."""
        # Simplified implementation...
        return 0.0
        
    @property
    def max_drawdown_pct(self) -> float:
        """Maximum drawdown percentage from peak."""
        peak = self.portfolio_values[0] if self.portfolio_values else 1
        max_dd = 0.0
        for value in self.portfolio_values:
            dd = (peak - value) / peak * 100
            max_dd = max(max_dd, dd)
        return max_dd
        
    @property
    def win_rate(self) -> float:
        """Percentage of winning trades."""
        if not self.orders:
            return 0.0
        winning = sum(1 for o in self.orders if o.get("realized_pnl", 0) > 0)
        return winning / len(self.orders) * 100
        
    @property  
    def profit_factor(self) -> float:
        """Gross profits / Gross losses ratio."""
        # Implementation...
        return 1.0

# Test suite setup in test_backtest_engine.py
from trading_system.backends.engine import BacktestEngine
from trading_system.strategies.zscore_strategy import ZScoreStrategy

def test_zscore_backtest_comprehensive():
    """Comprehensive test of mean reversion strategy."""
    
    # Setup
    engine = BacktestEngine(initial_capital=10000, fee_pct=0.001)
    strategy = ZScoreStrategy(lookback_bars=60, z_buy_threshold=-2.5)
    
    # Load historical data (mock or fetch from exchange)
    mock_ohlcv_data = generate_mock_ohlcv(symbol="BTC-USD", days=365)
    engine.load_market_data(mock_ohlcv_data)
    
    # Run backtest with realistic parameters
    config = {"slippage_bps": 2.0, "fee_pct": 0.001}
    strategy.init(config)
    
    results = []
    for i in range(len(mock_ohlcv_data)):
        engine.step()
        
    # Calculate and assert metrics
    metrics = PerformanceMetrics(orders=engine.order_events)
    assert abs(metrics.total_return_pct - expected_return_pct) < tolerance
    
def test_strategy_edge_cases():
    """Test strategy behavior under extreme market conditions."""
    # Test: zero volume periods
    # Test: large price gaps (>10% daily moves)
    # Test: flash crash recovery scenarios  
    # Test: API rate limit simulation
    pass
```

---

## Parallel Tasks (Running Concurrently)

### Task 1: Connector Implementation Completion
- [ ] Binance WebSocket connector with streaming OHLCV
- [ ] Coinbase Prime REST API wrapper
- [ ] Bybit spot market integration
- [ ] Kraken exchange connectivity
- [ ] Unified interface testing

### Task 2: Strategy Registry Population  
- [ ] Trend following (50 strategies) - complete implementation
- [ ] Mean reversion (50 strategies) - complete implementation
- [ ] Market making (20 strategies) - complete implementation
- [ ] Arbitrage (20+ strategies) - complete implementation

### Task 3: Documentation Buildout
- [ ] Individual strategy docstrings (purpose, regime fit, failure modes)
- [ ] Integration guide for new strategy authors
- [ ] Backtesting best practices document
- [ ] Risk management overlay specification

---

## Success Criteria - End of Week 1

✅ **Core infrastructure complete**:
- Strategy factory with standard interface implemented
- Unified data fetching across all major exchanges functional  
- Event-driven backtester capable of processing full year of data
- Performance metrics calculator returning accurate results

✅ **Initial strategy count**: ~20 strategies fully implemented and testable (subset of 200+ target)

✅ **Documentation complete**: All core modules documented with usage examples

**Next week focus**: Implement remaining ~180 crypto spot strategies, begin forward testing on paper accounts

---

## Notes for Implementation Team

- **Quality over quantity in Phase 1**: Better to have 20 well-tested strategies than 100 poorly tested
- **Type safety**: Use dataclasses + Optional typing throughout
- **Error handling**: All connectors MUST handle rate limits gracefully with retry logic
- **Logging**: Structured JSON logging for production deployments (refer to `trading_system/core/logging/structured.py`)
