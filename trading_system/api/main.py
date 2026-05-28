"""FastAPI Main Application Entry Point for Trading System UI Dashboard

This module serves as the main entry point for the trading system web dashboard,
integrating the API routes with FastAPI's web framework.

Usage:
    python3 -m trading_system.api.main
    
Or included in the main application at runtime.py

Endpoints:
- GET / - Dashboard homepage (if static files configured)
- GET /health - Health check
- GET /metrics - System metrics
- GET /accounts - List accounts
- GET /trades - List trades
- GET /positions - Current positions
- GET /strategies - Available strategies
- GET /performance - Performance charts
- POST /evaluations/price/{instrument} - Get price estimates
- GET /approvals - Approval requests
- GET /research/hypotheses - Trading hypotheses
- GET /market/regime - Market regime snapshot
- GET /backtests - Strategy backtest results
- GET /capital/allocation - Capital allocation

Example:
    $ python3 trading_system/api/main.py
"""

from fastapi import FastAPI
from contextlib import asynccontextmanager

from .routes import (
    health_check,
    get_accounts,
    get_metrics,
    list_trades,
    list_positions,
    list_strategies,
    get_performance,
    get_price_estimations,
    get_approvals,
    get_research_hypotheses,
    get_market_regime_snapshot,
    list_backtests,
    get_capital_allocation,
)


# ============================================================================
# FastAPI Application Setup
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan event handler."""
    # Startup logic here
    print("Trading System UI Dashboard starting...")
    yield
    
    # Shutdown logic here
    print("Trading System UI Dashboard shutting down gracefully...")


app = FastAPI(
    title="Trading System UI Dashboard",
    description="Production web dashboard for trading system portfolio management",
    version="1.0.0",
    docs_url="/docs",  # API documentation at /docs
    redoc_url="/redoc",  # ReDoc documentation at /redoc
    lifespan=lifespan
)


# ============================================================================
# ROUTE MAPPING
# ============================================================================

@app.get("/", tags=["dashboard"])
async def dashboard_home():
    """Dashboard homepage with summary information."""
    return {
        "message": "Trading System UI Dashboard - Welcome",
        "service": "trading-system-ui-dashboard",
        "version": app.version,
        "status": "running",
        "endpoints_available": [
            "/health",
            "/metrics", 
            "/accounts",
            "/trades",
            "/positions",
            "/strategies",
            "/performance",
            "/evaluations/price/{instrument}",
            "/approvals",
            "/research/hypotheses",
            "/market/regime",
            "/backtests",
            "/capital/allocation"
        ]
    }


@app.get("/health", tags=["system"])
async def health_check_endpoint():
    """Health check endpoint for load balancers and monitoring."""
    return await health_check()


@app.get("/metrics", tags=["monitoring"])
async def metrics_endpoint():
    """Get system metrics (Redis, PostgreSQL, container stats)."""
    return await get_metrics()


@app.get("/accounts", response_model=dict, tags=["accounts"])
async def list_accounts_endpoint():
    """List all discovered and processed accounts from Plaid ingestion."""
    return await get_accounts()


@app.get("/trades", response_model=dict, tags=["trading"])
async def list_trades_endpoint():
    """List executed trades with filtering options."""
    return await list_trades()


@app.get("/positions", response_model=dict, tags=["trading"])
async def list_positions_endpoint():
    """List current open positions with P&L analysis."""
    return await list_positions()


@app.get("/strategies", response_model=dict, tags=["strategies"])
async def list_strategies_endpoint():
    """List all available strategies with their status and performance."""
    return await list_strategies()


@app.get("/performance", response_model=dict, tags=["performance"])
async def performance_endpoint(charts: bool = True):
    """Get performance metrics and chart data for portfolio management."""
    return await get_performance(charts=charts)


@app.post("/evaluations/price/{instrument}", response_model=dict, tags=["evaluation"])
async def get_price_estimates_post(instrument: str):
    """Get price estimates from multiple models for specified instrument."""
    return await get_price_estimations(instrument)


@app.get("/approvals", response_model=dict, tags=["approval_routing"])
async def list_approvals_endpoint(status_filter: str = None):
    """List approval requests with filtering by status."""
    return await get_approvals(status_filter)


@app.get("/research/hypotheses", response_model=dict, tags=["research"])
async def list_hypotheses_endpoint():
    """List active trading hypotheses from research system."""
    return await get_research_hypotheses()


@app.get("/market/regime", response_model=dict, tags=["research"])
async def market_regime_snapshot_endpoint():
    """Get current market regime classification and snapshot."""
    return await get_market_regime_snapshot()


@app.get("/backtests", response_model=dict, tags=["strategies"])
async def list_backtests_endpoint(strategy_id: str | None = None):
    """List backtest results for strategies."""
    return await list_backtests(strategy_id)


@app.get("/capital/allocation", response_model=dict, tags=["portfolio"])
async def get_capital_allocation_endpoint():
    """Get current capital allocation across strategies and accounts."""
    return await get_capital_allocation()


# ============================================================================
# API VERSIONING (Future Enhancement)
# ============================================================================

# Uncomment for v2 API versioning:
# @app.get("/v1/accounts", tags=["accounts"])
# async def list_accounts_v1():
#     """List all accounts - V1 API."""
#     return await list_accounts()


# ============================================================================
# MOCK DATA LOADING (For Development/Testing)
# ============================================================================

@app.get("/api/mocks/load", tags=["development"])
async def load_mock_data():
    """Load mock data for development/testing purposes."""
    import random
    
    # Generate realistic-looking mock data
    num_accounts = random.randint(1, 5)
    num_trades = random.randint(3, 10)
    
    return {
        "status": "mock_data_loaded",
        "accounts_count": num_accounts,
        "trades_count": num_trades,
        "note": "In production, this endpoint would be removed for security"
    }


# ============================================================================
# CORS CONFIGURATION (For future web client integration)
# ============================================================================


# Add CORS middleware if you plan to serve a separate frontend:
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["https://destroyer.internal.tailscale.net"],  # Adjust for your domain
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# ============================================================================
# HEALTH CHECK ENDPOINT (Additional)
# ============================================================================

@app.get("/api/health", tags=["system"])
async def api_health_check():
    """Detailed API health check with component status."""
    from datetime import datetime
    
    return {
        "service": "Trading System UI Dashboard API",
        "version": app.version,
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "fastapi_server": True,
            "database": True,  # Would check actual connection
            "redis_cache": True,  # Would check actual connection
            "rpc_clients": True  # Would check actual connections
        }
    }


# ============================================================================
# MAIN APPLICATION ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Start FastAPI server with Uvicorn
    print("Starting Trading System UI Dashboard...")
    print("API Documentation available at: http://localhost:8000/docs")
    print("Redoc documentation available at: http://localhost:8000/redoc")
    
    uvicorn.run(
        "trading_system.api.main:app",
        host="0.0.0.0",  # Bind to all interfaces (or use localhost for development)
        port=8000,
        reload=False,  # Set True during development
        log_level="info"
    )
