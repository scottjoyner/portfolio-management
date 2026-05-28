"""Trading System API Endpoints - Export all routes for Flask/FastAPI integration."""

from trading_system.api.routes import (
    # Core endpoints
    health_check,
    get_metrics,
    list_accounts,
    sync_account_transactions,
    
    # Trading endpoints
    list_trades,
    list_positions,
    list_strategies,
    get_performance,
    
    # Valuation endpoints
    get_price_estimations,
    get_valuation,
    
    # Risk/Compliance endpoints
    get_approvals,
    get_research_hypotheses,
)

__all__ = [
    "health_check",
    "get_metrics", 
    "list_accounts",
    "sync_account_transactions",
    "list_trades",
    "list_positions",
    "list_strategies",
    "get_performance",
    "get_price_estimations",
    "get_valuation",
    "get_approvals",
    "get_research_hypotheses",
]
