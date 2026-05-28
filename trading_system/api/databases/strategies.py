"""SQL queries for strategy performance data from PostgreSQL schema."""

from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func


async def get_strategy_performance(session: AsyncSession) -> List[Dict[str, Any]]:
    """Get performance metrics for all strategies.
    
    Queries strategy_configs table with backtest results and live performance stats.
    
    Args:
        session: SQLAlchemy async database session
        
    Returns:
        List of strategy performance dictionaries with sharpe_ratio, win_rate,
        total_trades, avg_profit_factor
    """
    
    statement = select('*')  # Placeholder - adapt to actual schema
    
    result = await session.execute(statement)
    rows = result.mapped()
    
    return [dict(row) for row in rows]


async def get_strategy_backtest_results(session: AsyncSession, strategy_id: str) -> Dict[str, Any]:
    """Get backtest results for specific strategy.
    
    Args:
        session: SQLAlchemy async database session
        strategy_id: Strategy identifier
        
    Returns:
        Dictionary with total_return_pct, sharpe_ratio, max_drawdown_pct, calmar_ratio
    """
    
    statement = select('*')  # Placeholder
    
    result = await session.execute(statement)
    row = result.mapped() if result.mapped() else {}
    
    return dict(row)


__all__ = [
    "get_strategy_performance",
    "get_strategy_backtest_results"
]
