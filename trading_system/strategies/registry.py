"""
Strategy Registry Module

Provides persistent storage, versioning, and lifecycle management for trading strategies.

Features:
- YAML/JSON strategy definition loading
- Version control with changelog tracking
- Strategy validation (syntax, parameters, dependencies)
- Hot-reload capability for development
- Benchmark comparison metrics

Usage Example:
```python
from trading_strategies import load_registered_strategy

# Load by key name
strategy = load_registered_strategy("ema_crossover")

# Execute and record results
results = strategy.execute(
    data=ohlcv_data,
    parameters={"fast_period": 9, "slow_period": 21}
)

# Record to database
strategy_manager.record_result(strategy_key, results)
```
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


logger = logging.getLogger(__name__)


class StrategyError(Exception):
    """Base exception for strategy execution errors."""
    
    def __init__(self, message: str, key: str | None = None):
        super().__init__(message)
        self.key = key
        
    def __str__(self) -> str:
        return f"[{self.key}] {super().__str__()}" if self.key else super().__str__()


class ValidationError(StrategyError):
    """Raised when strategy validation fails."""
    pass


class LoadError(StrategyError):
    """Raised when strategy loading fails."""
    pass


class ExecutionError(StrategyError):
    """Raised during strategy execution."""
    pass


# ============================================================================
# Strategy Definition Protocol (for type checking)
# ============================================================================

class IStrategy(Protocol):
    """Protocol defining strategy interface."""
    
    def setup(self, ohlcv: list[dict[str, Any]]) -> None: ...
    
    def on_bar(self, bar: dict[str, Any]) -> tuple[bool | None, float | None]: ...


# ============================================================================
# Strategy Metadata
# ============================================================================

@dataclass
class StrategyMetadata:
    """Version control metadata for strategy definitions."""
    
    key: str
    version: int = 1
    definition_file: str | None = None
    author: str | None = None
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    changelog: list[dict] = field(default_factory=list)
    
    def with_version(self, version: int, author: str | None = None) -> StrategyMetadata:
        """Create new metadata with incremented version."""
        return StrategyMetadata(
            key=self.key,
            version=version,
            definition_file=self.definition_file,
            author=author or self.author,
            created_at=self.created_at,
            updated_at=datetime.now(timezone.utc),
            changelog=[{"message": f"Version {self.version} → {version}", "timestamp": datetime.now(timezone.utc)}] + self.changelog
        )


# ============================================================================
# Strategy Registry (In-Memory)
# ============================================================================

class StrategyRegistry:
    """
    In-memory strategy registry with YAML/JSON loading.
    
    Provides hot-reload capability for development. Use StrategyManager 
    for persistent storage and deployment.
    
    Example:
        registry = StrategyRegistry()
        
        # Load strategies from files
        registry.load_from_yaml("strategies/ema_crossover.yml")
        registry.load_from_json("strategies/momentum.py.json")
        
        # Execute strategy
        results = registry.execute_strategy("ema_crossover", parameters={})
    """
    
    def __init__(self):
        self.strategies: dict[str, tuple[IStrategy, StrategyMetadata]] = {}
        self.definition_files: dict[str, str] = {}
    
    def register(self, strategy: IStrategy, key: str | None = None) -> None:
        """
        Register strategy with optional metadata auto-generation.
        
        Args:
            strategy: Strategy instance to register
            key: Strategy name (auto-generated from module/class name if not provided)
        """
        
        if key is None:
            key = self._generate_key(strategy)
        
        logger.info(f"Registered strategy: {key}")
        
        # Extract function/class signature for documentation
        metadata = StrategyMetadata(key=key, version=1)
        self.strategies[key] = (strategy, metadata)
        self.definition_files[key] = f"registry:{key}"
    
    def load_from_yaml(self, path: str) -> None:
        """Load strategies from YAML files."""
        
        import yaml
        from pathlib import Path
        
        file_path = Path(path)
        if not file_path.exists():
            logger.warning(f"YAML file not found: {path}")
            return
        
        with open(file_path, 'r') as f:
            contents = f.read()
        
        # Parse YAML into strategy definitions
        definitions = yaml.safe_load(contents)
        
        for definition in definitions:
            key = definition.get("name") or self._generate_key(definition)
            
            try:
                strategy = Strategy.from_definition(definition)
                self.register(strategy, key)
            except Exception as e:
                logger.error(f"Failed to load strategy from {key}: {e}")
    
    def load_from_json(self, path: str) -> None:
        """Load strategies from JSON files (for pure data definitions)."""
        
        import json
        
        file_path = Path(path)
        if not file_path.exists():
            logger.warning(f"JSON file not found: {path}")
            return
        
        with open(file_path, 'r') as f:
            contents = json.load(f)
        
        for key, definition in contents.items():
            try:
                strategy = Strategy.from_definition(definition)
                self.register(strategy, key)
            except Exception as e:
                logger.error(f"Failed to load strategy from {key}: {e}")
    
    def execute_strategy(self, key: str, parameters: dict[str, Any]) -> dict[str, Any]:
        """
        Execute a registered strategy on OHLCV data.
        
        Args:
            key: Strategy name
            parameters: Execution parameters
        
        Returns:
            Results dictionary including signals and performance metrics
        
        Raises:
            LoadError: If strategy not found
            ValidationError: If parameters invalid
        """
        
        if key not in self.strategies:
            raise LoadError(f"Strategy not found: {key}", key=key)
        
        strategy, metadata = self.strategies[key]
        
        try:
            # Setup phase (run once per data source)
            ohlcv_data = self._get_ohlcv_data()  # Placeholder for actual data source
            
            # TODO: Replace with actual OHLCV data source
            setup_params = {
                "data_source": "placeholder",
                "timeframe": "1h",
            }
            
            strategy.setup(ohlcv_data)
            
            # Execution phase (signal generation)
            signals: list[dict[str, Any]] = []
            
            # Execute on each bar
            for ohlcv_bar in ohlcv_data:
                signal, _ = strategy.on_bar(ohlcv_bar)
                
                if signal is not None:
                    entry_price = ohlcv_bar.get("close", 0)
                    
                    signals.append({
                        "timestamp": ohlcv_bar.get("timestamp"),
                        "entry_price": entry_price,
                        "signal_type": "buy" if signal else "sell",
                        "parameters": parameters,
                    })
            
            # Calculate performance metrics
            results = self._calculate_performance(ohlcv_data, signals)
            
            return {
                "status": "success",
                "strategy_key": key,
                "version": metadata.version,
                "signals_generated": len(signals),
                "performance": results,
            }
            
        except Exception as e:
            logger.exception(f"Strategy execution failed for {key}: {e}")
            raise ExecutionError(str(e), key=key)
    
    def _calculate_performance(
        self,
        ohlcv_data: list[dict[str, Any]],
        signals: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Calculate performance metrics from signals."""
        
        if not signals or len(signals) < 2:
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "sharpe_ratio": 0.0,
            }
        
        # Calculate performance metrics from signals
        returns = []
        for idx in range(len(signals) - 1):
            if signals[idx].get("signal_type") == "buy":
                returns.append(signals[idx + 1]["entry_price"] / signals[idx]["entry_price"] - 1)
        
        # Calculate win rate (simplified)
        winning_trades = sum(1 for r in returns if r > 0)
        
        return {
            "total_trades": len(signals),
            "win_rate": winning_trades / max(len(signals), 1),
            "profit_factor": sum(r for r in returns if r > 0) / abs(sum(r for r in returns if r < 0)) or 0.0,
        }
    
    def _generate_key(self, definition: dict[str, Any]) -> str:
        """Generate strategy key from definition."""
        
        name = definition.get("name")
        module_name = definition.get("module", "unknown")
        
        return f"{name} ({module_name})" if name else f"{module_name}.strategy"
    
    def _get_ohlcv_data(self) -> list[dict[str, Any]]:
        """Placeholder for OHLCV data source."""
        
        return []


# ============================================================================
# Strategy Manager (Persistent Storage)
# ============================================================================

class StrategyManager:
    """
    Persistent strategy manager with database-backed storage and version control.
    
    Provides:
    - YAML/JSON definition loading to database
    - Version history tracking
    - Benchmark comparison
    - Lifecycle management (enable/disable/archive)
    
    Example:
        from trading_strategies import StrategyManager
        
        manager = StrategyManager()
        
        # Load strategy from file
        manager.load_definition_from_yaml("/strategies/ema_crossover.yml")
        
        # Register with metadata
        manager.register(strategy, key="ema_crossover", author="dev@example.com")
        
        # Record benchmark results
        metrics = {
            "sharpe_ratio": 1.5,
            "win_rate": 0.45,
            "max_drawdown": -0.12,
        }
        manager.record_result("ema_crossover", parameters={"fast": 9}, metrics=metrics)
    """
    
    def __init__(self, session: Session):
        self.session = session
        self.registry = StrategyRegistry()
    
    def load_definition_from_yaml(self, path: str) -> None:
        """Load strategy definition from YAML file and store in database."""
        
        import yaml
        
        with open(path, 'r') as f:
            definition = yaml.safe_load(f)
        
        # TODO: Store to plaid_items (strategy definitions table)
        logger.info(f"Loaded strategy definition from {path}")
    
    def load_definition_from_json(self, path: str) -> None:
        """Load strategy definition from JSON file and store in database."""
        
        import json
        
        with open(path, 'r') as f:
            definition = json.load(f)
        
        logger.info(f"Loaded strategy definition from {path}")
    
    def register(
        self,
        strategy: IStrategy,
        key: str | None = None,
        author: str | None = None
    ) -> StrategyMetadata:
        """
        Register strategy with persistent metadata.
        
        Args:
            strategy: Strategy instance to register
            key: Strategy name (auto-generated if not provided)
            author: Author name for version tracking
        
        Returns:
            Created metadata object
        """
        
        metadata = self.registry.register(strategy, key)
        
        # TODO: Store metadata to plaid_items table
        
        logger.info(f"Registered strategy {key} with metadata")
        
        return metadata
    
    def record_result(
        self,
        key: str,
        parameters: dict[str, Any],
        metrics: dict[str, Any]
    ) -> None:
        """
        Record benchmark results for strategy.
        
        Args:
            key: Strategy name
            parameters: Parameters used in benchmark run
            metrics: Performance metrics dictionary
        
        Usage:
            manager.record_result(
                "ema_crossover",
                {"fast": 9, "slow": 21},
                {"sharpe_ratio": 1.5, "win_rate": 0.45}
            )
        """
        
        result = {
            "strategy_key": key,
            "parameters": json.dumps(parameters),
            "metrics": metrics,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        
        # TODO: Store to plaid_items.benchmark_results table
        
        logger.info(f"Recorded benchmark result for {key}")
