"""Trading System Web Dashboard API Routes

This module provides REST API endpoints for the trading system UI dashboard.
Integrates with FastAPI app at runtime.py or main application entry point.

Endpoints:
- GET /health - Health check
- GET /metrics - System metrics (Redis, PostgreSQL, container stats)
- GET /accounts - List all Plaid accounts
- POST /accounts/{id}/transactions - Sync transactions
- GET /trades - List executed trades
- GET /positions - Current open positions
- GET /strategies - Available strategies and status
- GET /performance - Performance charts and metrics
- POST /evaluations/price - Get price estimates for instruments
- GET /approvals - Pending and completed approvals
- GET /research/hypotheses - Trading hypotheses and market regimes

Example curl command:
$ curl http://localhost:8000/accounts
"""

from typing import List, Dict, Any
from datetime import datetime
import json


# ============================================================================
# HEALTH CHECK ENDPOINT
# ============================================================================

async def health_check() -> Dict[str, Any]:
    """Health check endpoint for container monitoring and load balancing."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "trading-system-ui-dashboard",
        "components": {
            "api": True,
            "database": True,
            "redis_cache": True
        }
    }


# ============================================================================
# METRICS ENDPOINT - System Monitoring
# ============================================================================

async def get_metrics() -> Dict[str, Any]:
    """Get system metrics (Redis, PostgreSQL, container stats)."""
    return {
        "service": "trading-system-ui-dashboard",
        "timestamp": datetime.utcnow().isoformat(),
        "metrics": {
            # Redis cache stats
            "redis": {
                "connected_keys": 142,
                "duplicated_keys": 3,
                "cache_size_bytes": 8547234,
                "hit_rate_pct": 99.2,
                "event_queue_length": 2847
            },
            # PostgreSQL database stats
            "postgresql": {
                "total_tables": 19,
                "p0_p1_tables": 8,
                "p1_4_runtime_tables": 4,
                "p3_evaluation_tables": 7,
                "db_size_mb": 4523,
                "slow_queries_count_today": 12,
                "connections_active": 3,
                "connections_max": 100
            },
            # Container resources
            "container": {
                "memory_usage_pct": 68,
                "cpu_usage_pct": 45,
                "uptime_seconds": 7200,
                "pid": 2341
            }
        }
    }


# ============================================================================
# ACCOUNTS ENDPOINT - List Plaid Accounts
# ============================================================================

async def list_accounts() -> Dict[str, Any]:
    """List all discovered and processed accounts from Plaid ingestion."""
    
    # Mock account data structure (replace with actual database query)
    accounts = [
        {
            "id": "acc_001_eth_fund",
            "display_name": "Coinbase Pro - ETH Main Fund",
            "provider": "coinbase_pro",
            "currency": "BTC",
            "current_balance_usd": 45234.67,
            "fiat_balance_usd": 1987654.32,
            "created_at": "2026-05-20T10:30:00Z",
            "last_synced": "2026-05-27T14:22:00Z",
            "status": "active",
            "institution_name": "Coinbase Pro",
            "plaid_link_token": None  # Would be populated if user links new account
        },
        {
            "id": "acc_002_eth_fund",
            "display_name": "Kraken - ETH Reserve Fund",
            "provider": "kraken",
            "currency": "ETH",
            "current_balance_usd": 78945.23,
            "fiat_balance_usd": 1789654.88,
            "created_at": "2026-05-21T09:15:00Z",
            "last_synced": "2026-05-27T14:20:00Z",
            "status": "active",
            "institution_name": "Kraken",
            "plaid_link_token": None
        }
    ]
    
    return {
        "accounts": accounts,
        "total_accounts": len(accounts),
        "total_value_usd": sum(a["current_balance_usd"] for a in accounts),
        "last_sync_timestamp": datetime.utcnow().isoformat()
    }


# ============================================================================
# ACCOUNTS/SYNC TRANSACTIONS ENDPOINT
# ============================================================================

async def sync_account_transactions(account_id: str) -> Dict[str, Any]:
    """Trigger transaction sync for specified account."""
    
    # Mock response structure
    return {
        "account_id": account_id,
        "status": "sync_started",
        "message": f"Transaction sync initiated for {account_id}",
        "estimated_completion_seconds": 15,
        "webhook_url": None,  # Would trigger async transaction fetch via Plaid API
        "last_synced": None
    }


# ============================================================================
# TRADES ENDPOINT - List Executed Trades
# ============================================================================

async def list_trades(limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    """List executed trades with filtering options."""
    
    # Mock trade data (would query database in production)
    trades = [
        {
            "id": "trade_001",
            "instrument": "BTC/USD",
            "direction": "long",  # long or short position
            "quantity_usd": 4500.0,
            "price_per_unit_usd": 68523.45,
            "timestamp": "2026-05-27T12:30:00Z",
            "strategy_id": "ema_crossover_zscore_v1",
            "approval_request_id": "app_001",
            "execution_status": "filled",  # filled, partially_filled, rejected
            "fees_usd": 4.50,
            "net_amount_usd": 4495.50,
            "exchange": "coinbase_pro"
        },
        {
            "id": "trade_002",
            "instrument": "ETH/USD",
            "direction": "long",
            "quantity_usd": 7500.0,
            "price_per_unit_usd": 3456.78,
            "timestamp": "2026-05-27T10:15:00Z",
            "strategy_id": "trend_following_v2",
            "approval_request_id": "app_002",
            "execution_status": "filled",
            "fees_usd": 7.50,
            "net_amount_usd": 7492.50,
            "exchange": "coinbase_pro"
        }
    ]
    
    return {
        "trades": trades,
        "total_trades": len(trades),
        "offset": offset,
        "limit": limit,
        "has_more": True  # Indicates pagination
    }


# ============================================================================
# POSITIONS ENDPOINT - Current Open Positions
# ============================================================================

async def list_positions() -> Dict[str, Any]:
    """List current open positions with P&L analysis."""
    
    # Mock position data
    positions = [
        {
            "instrument": "BTC/USD",
            "direction": "long",
            "quantity_usd": 4500.0,
            "entry_price_usd": 68523.45,
            "current_price_usd": 69125.32,
            "unrealized_pnl_usd": 272.15,
            "unrealized_pnl_pct": 0.88,
            "exchange": "coinbase_pro",
            "position_size_btc": 0.0657,
            "liquidation_price": None  # Not applicable for long positions without leverage
        },
        {
            "instrument": "ETH/USD",
            "direction": "long",
            "quantity_usd": 7500.0,
            "entry_price_usd": 3456.78,
            "current_price_usd": 3512.45,
            "unrealized_pnl_usd": 421.03,
            "unrealized_pnl_pct": 1.61,
            "exchange": "coinbase_pro",
            "position_size_eth": 2.17,
            "liquidation_price": None
        }
    ]
    
    return {
        "positions": positions,
        "total_positions": len(positions),
        "total_exposure_usd": sum(p["quantity_usd"] for p in positions),
        "total_unrealized_pnl_usd": sum(p["unrealized_pnl_usd"] for p in positions),
        "total_unrealized_pnl_pct": (sum(p["unrealized_pnl_usd"] for p in positions) / 
                                      sum(p["quantity_usd"] for p in positions)) * 100
    }


# ============================================================================
# STRATEGIES ENDPOINT - Available Strategies and Status
# ============================================================================

async def list_strategies() -> Dict[str, Any]:
    """List all available strategies with their status and performance."""
    
    # Mock strategy data structure (would query from registration/registry package)
    strategies = [
        {
            "strategy_id": "ema_crossover_zscore_v1",
            "name": "EMA Crossover + Z-Score",
            "description": "Exponential Moving Average crossover with mean-reversion z-score filtering",
            "category": "momentum_mean_reversion",
            "backtested": True,
            "status": "active",
            "last_backtest_date": "2026-05-27T08:00:00Z",
            "sharpe_ratio": 1.42,
            "max_drawdown_pct": -8.5,
            "total_trades": 847,
            "win_rate_pct": 68.3,
            "avg_profit_factor": 1.34
        },
        {
            "strategy_id": "trend_following_v2",
            "name": "Trend Following v2",
            "description": "Multi-timeframe trend detection with volatility-adjusted position sizing",
            "category": "trend_following",
            "backtested": True,
            "status": "active",
            "last_backtest_date": "2026-05-27T08:00:00Z",
            "sharpe_ratio": 1.67,
            "max_drawdown_pct": -12.3,
            "total_trades": 523,
            "win_rate_pct": 64.1,
            "avg_profit_factor": 1.58
        },
        {
            "strategy_id": "mean_reversion_v1",
            "name": "Mean Reversion",
            "description": "Statistical mean reversion with Bollinger Bands and RSI filters",
            "category": "statistical_arbitrage",
            "backtested": True,
            "status": "active",
            "last_backtest_date": "2026-05-27T08:00:00Z",
            "sharpe_ratio": 1.23,
            "max_drawdown_pct": -15.7,
            "total_trades": 1247,
            "win_rate_pct": 61.2,
            "avg_profit_factor": 1.28
        },
        {
            "strategy_id": "volatility_breakout_v1",
            "name": "Volatility Breakout",
            "description": "Breakout detection using volatility expansion patterns",
            "category": "volatility_trading",
            "backtested": False,
            "status": "development",
            "last_backtest_date": None,
            "sharpe_ratio": 0.0,
            "max_drawdown_pct": 0.0,
            "total_trades": 0,
            "win_rate_pct": 0.0,
            "avg_profit_factor": 0.0
        }
    ]
    
    return {
        "strategies": strategies,
        "total_strategies": len(strategies),
        "active_strategies": [s for s in strategies if s["status"] == "active"],
        "last_updated": datetime.utcnow().isoformat()
    }


# ============================================================================
# ANNUALIZED RETURN CALCULATION HELPER
# ============================================================================

def calculate_annualized_return(daily_returns: list) -> float:
    """Calculate annualized return from daily returns series."""
    if not daily_returns or len(daily_returns) == 0:
        return 0.0
    
    cumulative_multiplier = 1.0
    for r in daily_returns:
        cumulative_multiplier *= (1 + r/100)
    
    annualized_multiplier = cumulative_multiplier ** (252 / len(daily_returns))
    risk_free_rate_annual = 0.02
    return float((annualized_multiplier - risk_free_rate_annual) * 100)


# ============================================================================
# PERFORMANCE ENDPOINT - Historical Performance Charts
# ============================================================================

async def get_performance(charts: bool = True, metrics_only: bool = False) -> Dict[str, Any]:
    """Get performance metrics and chart data for portfolio management."""
    
    # Mock historical performance data (would query from database/trades table)
    daily_returns_pct = [
        0.12, -0.05, 0.34, 0.08, -0.15, 0.45, 0.23, 0.18, -0.08, 0.56,
        0.32, 0.15, 0.09, -0.12, 0.28, 0.42, 0.19, -0.06, 0.38, 0.22,
        0.16, 0.11, -0.09, 0.35, 0.27, 0.18, 0.14, -0.07, 0.31, 0.25
    ]
    
    # Cumulative returns calculation
    cumulative_returns_pct = []
    cumulative_multiplier = 1.0
    for r in daily_returns_pct:
        cumulative_multiplier *= (1 + r/100)
        cumulative_returns_pct.append((cumulative_multiplier - 1) * 100)
    
    # Monthly returns calculation (group by month for simplicity)
    monthly_returns_pct = [
        3.45, -1.23, 2.87, 1.95, -0.89, 4.12, 3.67, 2.34, -1.56, 5.23,
        3.89, 2.12, 1.78, -1.34, 3.45, 4.67, 2.56, -0.98, 3.78, 2.89,
        2.34, 1.89, -1.12, 3.67, 2.98, 2.45, 2.12, -0.87, 3.34, 2.67
    ]
    
    # Calculate annualized return from cumulative data
    annualized_return = calculate_annualized_return(daily_returns_pct)
    
    # Risk metrics (mock calculations)
    volatility_20d_pct = 18.5  # Annualized 20-day volatility
    sharpe_ratio = 1.42  # Sharpe ratio (annualized)
    max_drawdown_pct = -15.7  # Worst drawdown from peak
    calmar_ratio = 0.90  # Calmar ratio (Sharpe / Max DD)
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "summary_metrics": {
            "total_return_pct": cumulative_returns_pct[-1] if cumulative_returns_pct else 0,
            "annualized_return_pct": annualized_return,
            "volatility_20d_pct": volatility_20d_pct,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown_pct": max_drawdown_pct,
            "calmar_ratio": calmar_ratio,
            "sortino_ratio": 1.65  # Sortino (using downside deviation)
        },
        "charts": {
            "daily_returns": {
                "period_days": len(daily_returns_pct),
                "data_points": len(daily_returns_pct),
                "chart_type": "line",
                "x_axis_label": "Date",
                "y_axis_label": "Daily Return (%)",
                "values": daily_returns_pct[:50]  # First 50 data points for chart
            },
            "monthly_returns": {
                "period_months": len(monthly_returns_pct),
                "chart_type": "bar",
                "x_axis_label": "Month",
                "y_axis_label": "Monthly Return (%)",
                "values": monthly_returns_pct[:24]  # First 24 months for chart
            },
            "cumulative_returns": {
                "chart_type": "line_area",
                "x_axis_label": "Date",
                "y_axis_label": "Cumulative Return (%)",
                "values": cumulative_returns_pct[:100]  # First 100 data points
            }
        },
        "risk_metrics": {
            "volatility_20d_pct": volatility_20d_pct,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown_pct": max_drawdown_pct,
            "calmar_ratio": calmar_ratio,
            "sortino_ratio": 1.65
        }
    }


# ============================================================================
# PRICE ESTIMATES ENDPOINT - Get Fair Market Price Estimates
# ============================================================================

async def get_price_estimates(instrument: str | None = None, default_instrument: str = "BTC/USD") -> Dict[str, Any]:
    """Get price estimates from multiple models for specified instrument."""
    
    if not instrument:
        instrument = default_instrument
    
    # Mock price estimate data (would query from P3 evaluation package)
    instrument = instrument.upper()  # Normalize instrument symbol
    
    return {
        "instrument": instrument,
        "timestamp": datetime.utcnow().isoformat(),
        "price_estimates": [
            {
                "model_type": "fundamental_analysis",
                "target_price_usd": 69500.0,  # Example for BTC/USD
                "confidence_score": 0.78,
                "buy_level": 67000.0,
                "sell_level": 71500.0,
                "valuation_metrics": {
                    "pe_ratio": None,  # Would be populated for stocks
                    "book_value_multiple": None,
                    "fundamental_score": "undervalued"
                }
            },
            {
                "model_type": "technical_analysis",
                "target_price_usd": 69200.0,
                "confidence_score": 0.72,
                "support_levels": [68500, 67800, 67000],
                "resistance_levels": [69800, 70500, 71200],
                "trend_direction": "bullish"
            },
            {
                "model_type": "consensus_market",
                "target_price_usd": 69150.0,
                "confidence_score": 0.85,
                "consensus_rating": "buy",  # buy/hold/sell
                "analyst_count": 42,
                "price_targets": {
                    "high": 72000.0,
                    "mean": 69150.0,
                    "median": 69200.0,
                    "low": 66000.0
                }
            },
            {
                "model_type": "ml_predictive",
                "target_price_usd": 69300.0,
                "confidence_score": 0.74,
                "prediction_horizon_days": 30,
                "model_accuracy_pct": 72.5
            }
        ],
        "summary": {
            "weighted_avg_target_usd": 69287.5,
            "avg_confidence_score": 0.7725,
            "overall_sentiment": "bullish"  # bullish/neutral/bearish based on model consensus
        }
    }


# ============================================================================
# APPROVALS ENDPOINT - Pending and Completed Approvals
# ============================================================================

async def list_approvals(status_filter: str = None) -> Dict[str, Any]:
    """List approval requests with filtering by status."""
    
    # Mock approval data structure (from P3 approval routing system)
    approvals = [
        {
            "id": "app_001",
            "strategy_id": "ema_crossover_zscore_v1",
            "instrument": "BTC/USD",
            "quantity_usd": 4500.0,
            "direction": "long",
            "target_price_usd": 68523.45,
            "risk_score": 0.42,
            "tier": "FULL_SCALE",  # AUTO_APPROVE/CANARY_PHASE/FULL_SCALE
            "status": "approved",  # pending/auto_approved/rejected
            "submitted_at": "2026-05-27T08:15:00Z",
            "approval_decision": None,
            "decision_timestamp": None,
            "auto_approved": True,
            "reasoning": "Risk score below threshold for AUTO_APPROVE tier"
        },
        {
            "id": "app_002",
            "strategy_id": "trend_following_v2",
            "instrument": "ETH/USD",
            "quantity_usd": 7500.0,
            "direction": "long",
            "target_price_usd": 3456.78,
            "risk_score": 0.58,
            "tier": "CANARY_PHASE",
            "status": "pending",
            "submitted_at": "2026-05-27T09:30:00Z",
            "approval_decision": None,
            "decision_timestamp": None,
            "auto_approved": False,
            "reasoning": None  # Waiting for human review
        }
    ]
    
    if status_filter:
        approvals = [a for a in approvals if a["status"].lower() == status_filter.lower()]
    
    return {
        "approvals": approvals,
        "total_approvals": len(approvals),
        "filters_applied": {"status": status_filter},
        "pagination": {
            "page": 1,
            "per_page": 20,
            "total_pages": 1,
            "has_more": False
        },
        "summary": {
            "pending_count": len([a for a in approvals if a["status"] == "pending"]),
            "auto_approved_count": len([a for a in approvals if a.get("auto_approved", False)]),
            "rejected_count": len([a for a in approvals if a["status"] == "rejected"])
        }
    }


# ============================================================================
# RESEARCH ENDPOINT - Trading Hypotheses and Market Regimes
# ============================================================================

async def list_hypotheses() -> Dict[str, Any]:
    """List active trading hypotheses from research system."""
    
    # Mock hypothesis data (from P3 research package)
    hypotheses = [
        {
            "id": "hyp_001",
            "name": "eth_btc_convergence",
            "confidence_score": 0.72,
            "market_state": "bullish",
            "strategy_type": "trend_following",
            "description": "ETH/BTC pair showing convergence opportunity in bull market regime",
            "key_factors": [
                {"factor": "volume_profile", "strength": "strong", "data_points": 450},
                {"factor": "volatility_regime", "strength": "moderate", "data_points": 120},
                {"factor": "market_sentiment", "strength": "positive", "data_points": 78}
            ],
            "generated_at": "2026-05-27T10:30:00Z",
            "status": "active"
        },
        {
            "id": "hyp_002",
            "name": "btc_usd_breakout",
            "confidence_score": 0.68,
            "market_state": "bullish",
            "strategy_type": "volatility_trading",
            "description": "BTC/USD approaching multi-month resistance level with increasing volume",
            "key_factors": [
                {"factor": "technical_breakout", "strength": "strong", "data_points": 89},
                {"factor": "liquidity_analysis", "strength": "moderate", "data_points": 234},
                {"factor": "option_flow", "strength": "positive", "data_points": 56}
            ],
            "generated_at": "2026-05-27T09:15:00Z",
            "status": "active"
        }
    ]
    
    return {
        "hypotheses": hypotheses,
        "total_hypotheses": len(hypotheses),
        "active_hypotheses": len([h for h in hypotheses if h["status"] == "active"]),
        "last_generated_at": datetime.utcnow().isoformat()
    }


async def get_market_regime_snapshot() -> Dict[str, Any]:
    """Get current market regime classification and snapshot."""
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "current_regime": {
            "state": "bullish",  # bull/bear/sideways
            "confidence_score": 0.82,
            "average_volatility_20d_pct": 18.5,
            "market_momentum_30d_pct": 4.23
        },
        "regime_history": [
            {
                "date": "2026-05-27",
                "state": "bullish",
                "confidence_score": 0.82,
                "average_volatility_20d_pct": 18.5
            },
            {
                "date": "2026-05-26",
                "state": "bullish",
                "confidence_score": 0.79,
                "average_volatility_20d_pct": 17.8
            },
            {
                "date": "2026-05-25",
                "state": "bullish",
                "confidence_score": 0.81,
                "average_volatility_20d_pct": 19.2
            }
        ],
        "regime_switch_probability": {
            "to_bearish_probability": 0.12,
            "to_sideways_probability": 0.28,
            "stay_bullish_probability": 0.60
        }
    }


# ============================================================================
# BACKTESTS ENDPOINT - Strategy Backtest Results
# ============================================================================

async def list_backtests(strategy_id: str | None = None) -> Dict[str, Any]:
    """List backtest results for strategies."""
    
    # Mock backtest data structure (would query from research tables)
    backtests = [
        {
            "strategy_id": "ema_crossover_zscore_v1",
            "backtest_version": "v2.3.1",
            "period_start": "2026-01-01T00:00:00Z",
            "period_end": "2026-05-27T23:59:59Z",
            "total_return_pct": 28.45,
            "annualized_return_pct": 127.3,
            "sharpe_ratio": 1.42,
            "sortino_ratio": 1.65,
            "max_drawdown_pct": -8.5,
            "calmar_ratio": 0.90,
            "total_trades": 847,
            "win_rate_pct": 68.3,
            "avg_profit_factor": 1.34,
            "treed_max_drawdown_pct": -12.3,
            "calmar_ratio_treed": 0.85
        },
        {
            "strategy_id": "trend_following_v2",
            "backtest_version": "v1.8.4",
            "period_start": "2026-01-01T00:00:00Z",
            "period_end": "2026-05-27T23:59:59Z",
            "total_return_pct": 34.67,
            "annualized_return_pct": 182.4,
            "sharpe_ratio": 1.67,
            "sortino_ratio": 1.89,
            "max_drawdown_pct": -12.3,
            "calmar_ratio": 0.93,
            "total_trades": 523,
            "win_rate_pct": 64.1,
            "avg_profit_factor": 1.58,
            "treed_max_drawdown_pct": -15.7,
            "calmar_ratio_treed": 0.78
        }
    ]
    
    if strategy_id:
        backtests = [b for b in backtests if b["strategy_id"] == strategy_id]
    
    return {
        "backtests": backtests,
        "total_backtests": len(backtests),
        "timestamp": datetime.utcnow().isoformat()
    }


# ============================================================================
# CAPITAL ALLOCATION ENDPOINT - Portfolio Capital Distribution
# ============================================================================

async def get_capital_allocation() -> Dict[str, Any]:
    """Get current capital allocation across strategies and accounts."""
    
    # Mock capital allocation data
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "total_available_capital_usd": 200000.0,
        "allocated_capital_usd": 92739.90,
        "available_capital_usd": 107260.10,
        "allocation_summary": [
            {
                "category": "trading_positions",
                "percentage": 46.4,
                "amount_usd": 92739.90
            },
            {
                "category": "cash_reserve",
                "percentage": 53.6,
                "amount_usd": 107260.10
            }
        ],
        "strategy_allocation": [
            {
                "strategy_id": "ema_crossover_zscore_v1",
                "allocated_capital_usd": 4500.0,
                "percentage_of_total": 4.86,
                "status": "active"
            },
            {
                "strategy_id": "trend_following_v2",
                "allocated_capital_usd": 7500.0,
                "percentage_of_total": 8.09,
                "status": "active"
            }
        ],
        "account_allocation": [
            {
                "account_id": "acc_001_eth_fund",
                "provider": "coinbase_pro",
                "current_value_usd": 45234.67,
                "percentage_of_total": 48.8
            },
            {
                "account_id": "acc_002_eth_fund",
                "provider": "kraken",
                "current_value_usd": 78945.23,
                "percentage_of_total": 85.1
            }
        ]
    }
