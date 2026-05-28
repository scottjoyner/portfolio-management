"""
Strategies Package (trading_system.strategies)

Provides strategy implementations, base classes, and registry for strategy management.

Architecture:
┌───────────────────────────────────────────────────────┐
│              Strategies Package                         │
├───────────────────────────────────────────────────────┤
│                                                       │
│  base.py                 │ ema_crossover_strategy.py   │
│  ─────────────           │ ─────────────────────────── │
│  BaseStrategy class      │ Standalone EMA crossover    │
│                          │ strategy implementation     │
│                               (can be loaded via registry) │
│                                                       │
│  registry.py             │ backtesting/engine.py       │
│  ─────────────           │ ─────────────────────────── │
│  StrategyRegistry        │ Event-driven backtest engine│
│  & Manager classes                      with metrics   │
│                          │                             │
└───────────────────────────────────────────────────────┘

Usage Example:
```python
from strategies.registry import StrategyManager, load_yaml_strategies
from trading_system.strategies.emacrossor_strategy import EMACrossoverStrategy

# Load strategies from YAML files
strategies = load_yaml_strategies("strategies/*.yml")

# Register with database (requires SQLAlchemy session)
# manager = StrategyManager(session)
# for key, definition in strategies:
#     manager.register_from_definition(key, definition)

# Or register inline instance:
strategy = EMACrossoverStrategy()
manager.register(strategy, key="ema_crossover_v1")
```
"""

from .registry.registry import load_strategies, strategy_metadata_index
from .base.interfaces import StrategyConfig, StrategyMetadata
from .base.simple import BaseSignalStrategy

# Import specific strategy implementations
from .emacrossor_strategy import EMACrossoverStrategy  # noqa: F401

__version__ = "0.1.0"
__all__ = [
    "load_strategies",
    "strategy_metadata_index",
    "BaseSignalStrategy",
    "StrategyConfig",
    "StrategyMetadata",
    "EMACrossoverStrategy",
]
