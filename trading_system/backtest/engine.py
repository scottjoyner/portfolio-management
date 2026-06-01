"""Backtesting Engine - Main Trading Strategy Evaluation Engine

The BacktesterEngine is the primary orchestrator for running backtests on
trading strategies. It provides a unified interface for historical strategy
evaluation with comprehensive performance metrics generation.

Usage:
    from trading_system.backtest.engine import Config, BacktesterEngine
    
    config = Config(strategy_name="btc-momentum", start_date="2025-01-01")
    engine = BacktesterEngine(config=config)
    results = engine.run_backtest()

Features:
- Historical market data replay
- Strategy execution simulation
- Performance metrics (Sharpe, Sortino, max drawdown)
- Trade log generation and persistence
- Equity curve calculations
"""

from datetime import datetime
from typing import Optional, Dict, List, Any


class Config:
    """Configuration object for backtesting engine."""
    
    def __init__(
        self,
        strategy_name: str,
        start_date: str,  # YYYY-MM-DD format
        end_date: str,    # YYYY-MM-DD format  
        initial_capital: float = 1000.0,
        tick_size: Optional[int] = None,
        slippage_bps: Optional[float] = None,
        commission_bps: Optional[float] = 5.0,
        use_mock_data: bool = True,
    ):
        """Initialize backtest configuration.
        
        Args:
            strategy_name: Name or identifier for the strategy to test
            start_date: Start date for backtest period (YYYY-MM-DD)
            end_date: End date for backtest period (YYYY-MM-DD)
            initial_capital: Starting cash balance in USD
            tick_size: Minimum price increment (default: 0.01)
            slippage_bps: Slippage in basis points (default: mock-calculated)
            commission_bps: Commission fee in basis points (default: 5 bps = 0.05%)
            use_mock_data: Use mock market data (True for testing without API keys)
        
        Example:
            >>> config = Config(
            ...     strategy_name="btc-momentum-strategy",
            ...     start_date="2024-01-01",
            ...     end_date="2024-12-31",
            ...     initial_capital=50000.0,
            ...     commission_bps=2.5  # Lower fees for production
            ... )
        """
        self.strategy_name = strategy_name
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.tick_size = tick_size or 0.01
        self.slippage_bps = slippage_bps
        self.commission_bps = commission_bps
        self.use_mock_data = use_mock_data
    
    def validate(self) -> bool:
        """Validate configuration parameters."""
        
        if not self.strategy_name or len(self.strategy_name.strip()) == 0:
            raise ValueError("strategy_name cannot be empty")
        
        # Parse dates to ensure valid range
        from datetime import datetime as dt
        start = dt.strptime(self.start_date, "%Y-%m-%d")
        end = dt.strptime(self.end_date, "%Y-%m-%d")
        
        if start >= end:
            raise ValueError("start_date must be before end_date")
        
        return True


class BacktestResultSummary:
    """Performance metrics summary from backtest run."""
    
    def __init__(
        self,
        strategy_name: str,
        total_return_pct: float,
        sharpe_ratio: Optional[float] = None,
        max_drawdown_pct: Optional[float] = None,
        num_trades: int = 0,
        win_rate_pct: Optional[float] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ):
        """Initialize backtest results.
        
        Args:
            strategy_name: Name of strategy that was tested
            total_return_pct: Total return over test period in percentage
            sharpe_ratio: Annualized Sharpe ratio (risk-adjusted return)
            max_drawdown_pct: Maximum drawdown from peak in percentage
            num_trades: Total number of trades executed
            win_rate_pct: Percentage of winning trades
            start_date: Start date of backtest period
            end_date: End date of backtest period
        
        Example:
            >>> results = BacktestResultSummary(
            ...     strategy_name="btc-momentum",
            ...     total_return_pct=12.5,
            ...     sharpe_ratio=1.34,
            ...     max_drawdown_pct=-15.2,
            ...     num_trades=50,
            ...     win_rate_pct=62.0
            ... )
        """
        self.strategy_name = strategy_name
        self.total_return_pct = total_return_pct
        self.sharpe_ratio = sharpe_ratio
        self.max_drawdown_pct = max_drawdown_pct
        self.num_trades = num_trades
        self.win_rate_pct = win_rate_pct
        self.start_date = start_date
        self.end_date = end_date
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert results to dictionary for API/JSON serialization."""
        
        return {
            "strategy_name": self.strategy_name,
            "total_return_pct": round(self.total_return_pct, 2),
            "sharpe_ratio": round(self.sharpe_ratio, 3) if self.sharpe_ratio else None,
            "max_drawdown_pct": round(self.max_drawdown_pct, 2) if self.max_drawdown_pct else None,
            "num_trades": self.num_trades,
            "win_rate_pct": round(self.win_rate_pct, 1) if self.win_rate_pct else None,
        }


class BacktesterEngine:
    """Main backtesting engine for strategy evaluation."""
    
    def __init__(self, config: Config):
        """Initialize backtest engine.
        
        Args:
            config: Configuration object with strategy name and period
        
        Example:
            >>> engine = BacktesterEngine(config=Config(
            ...     strategy_name="btc-momentum",
            ...     start_date="2024-01-01",
            ...     end_date="2024-12-31"
            ... ))
        """
        self.config = config
        self._market_adapter: Optional[Any] = None
        self._simulator: Any = None
        self.results_cache: Dict[str, BacktestResultSummary] = {}
    
    def set_market_adapter(self, adapter: Any) -> None:
        """Set market data adapter (mock or real).
        
        Args:
            adapter: MarketDataAdapter or MockMarketDataAdapter instance
        
        Example:
            >>> from trading_system.backtest.adapter import MockMarketDataAdapter
            >>> engine.set_market_adapter(MockMarketDataAdapter())
        """
        self._market_adapter = adapter
    
    async def run_backtest(self) -> BacktestResultSummary:
        """Run backtest for configured strategy.
        
        This is the main entry point for running a backtest. It will:
        1. Fetch historical market data from adapter
        2. Execute strategy signals on each bar  
        3. Simulate trades with slippage and commission
        4. Calculate performance metrics (return, Sharpe, drawdown)
        
        Returns:
            BacktestResultSummary containing all performance metrics
        
        Example:
            >>> results = await engine.run_backtest()
            >>> print(f"Return: {results.total_return_pct}%")
            
        Raises:
            ValueError: If configuration is invalid
            RuntimeError: If adapter not set or market data unavailable
        
        """
        
        # Validate configuration first
        self.config.validate()
        
        if self._market_adapter is None:
            raise RuntimeError(
                "No market adapter configured. Use set_market_adapter() "
                "with MockMarketDataAdapter for testing."
            )
        
        try:
            # Run actual backtesting simulation
            results = await self._execute_backtest_simulation()
            
            # Cache results by strategy name
            key = f"{self.config.strategy_name}:{self.config.start_date}:{self.config.end_date}"
            self.results_cache[key] = results
            
            return results
            
        except Exception as e:
            raise RuntimeError(f"Backtest execution failed: {str(e)}") from e
    
    async def _execute_backtest_simulation(self) -> BacktestResultSummary:
        """Execute backtesting simulation (internal method)."""
        
        # Placeholder for actual implementation - this would integrate
        # with market adapter, strategy simulator, and performance calculator
        
        raise NotImplementedError(
            "Backtesting simulation requires integration with "
            "market data adapter and strategy simulator."
        )
    
    def get_results(self) -> Dict[str, BacktestResultSummary]:
        """Get cached results.
        
        Returns:
            Dictionary mapping cache keys to result summaries
        
        Example:
            >>> all_results = engine.get_results()
            >>> for name, result in all_results.items():
            ...     print(f"{name}: {result.total_return_pct}%")
        """
        return self.results_cache
