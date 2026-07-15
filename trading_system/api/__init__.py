"""Trading System API Endpoints - Export all routes for Flask/FastAPI integration."""

from trading_system.api.routes import (
    # Core endpoints
    health_check,
    get_metrics,
    get_accounts,
    
    # Trading endpoints
    list_trades,
    list_positions,
    list_strategies,
    get_performance,
    
    # Valuation endpoints
    get_price_estimations,
    
    # Risk/Compliance endpoints
    get_approvals,
    get_research_hypotheses,
    get_market_regime_snapshot,
    list_backtests,
    get_capital_allocation,
)

__all__ = [
    "health_check",
    "get_metrics",
    "get_accounts",
    "list_trades",
    "list_positions",
    "list_strategies",
    "get_performance",
    "get_price_estimations",
    "get_approvals",
    "get_research_hypotheses",
    "get_market_regime_snapshot",
    "list_backtests",
    "get_capital_allocation",
]
