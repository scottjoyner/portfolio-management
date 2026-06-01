"""Backtest Database Models - Performance Analytics Tables

SQLAlchemy ORM models for storing and querying backtest results,
performance metrics, trade simulations, and strategy certification.
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all models in backtest module."""
    pass


# ============================================================================
# BACKTEST RESULTS TABLES
# ============================================================================

class BacktestResult(Base):
    """Store complete backtest results for strategies."""
    __tablename__ = "backtest_results"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_id: Mapped[str] = mapped_column(String(128), index=True)
    config_hash: Mapped[str] = mapped_column(String(64))  # Unique config version
    
    # Period information
    start_date: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_date: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    
    # Capital tracking
    initial_capital: Mapped[float] = mapped_column(Numeric(20, 4), server_default="0")
    
    # Performance metrics
    total_return_pct: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    realized_pnl: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), nullable=True)
    unrealized_pnl: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), nullable=True)
    
    # Risk metrics
    sharpe_ratio: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    max_drawdown_pct: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    sortino_ratio: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    
    # Trading statistics
    trade_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    win_rate_pct: Mapped[Optional[float]] = mapped_column(Numeric(6, 2), nullable=True)
    profit_factor: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    avg_trade_pnl_usd: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    
    # Cost analysis
    fees_paid_usd: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), nullable=True)
    slippage_costs_usd: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), nullable=True)
    gross_traded_usd: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), nullable=True)
    
    # Certification status
    is_certified: Mapped[bool] = mapped_column(Boolean, default=False)
    certification_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    certification_score: Mapped[Optional[float]] = mapped_column(Numeric(5, 4), nullable=True)
    
    # Metadata
    status: Mapped[str] = mapped_column(String(32), server_default="completed")
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ============================================================================
# EQUITY CURVE TRACKING
# ============================================================================

class EquityCurvePoint(Base):
    """Time-series equity curve snapshots for backtests."""
    __tablename__ = "equity_curve_points"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    backtest_id: Mapped[int] = mapped_column(
        ForeignKey("backtest_results.id"), 
        index=True
    )
    
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    
    # Capital tracking
    available_capital: Mapped[float] = mapped_column(Numeric(20, 4), server_default="0")
    realized_pnl: Mapped[float] = mapped_column(Numeric(20, 4), server_default="0")
    unrealized_pnl: Mapped[float] = mapped_column(Numeric(20, 4), server_default="0")
    position_value: Mapped[float] = mapped_column(Numeric(20, 4), server_default="0")
    
    # Running totals
    total_equity: Mapped[float] = mapped_column(Numeric(20, 4))
    total_traded_usd: Mapped[float] = mapped_column(Numeric(20, 4), server_default="0")


# ============================================================================
# TRADE LOG STORAGE
# ============================================================================

class BacktestTrade(Base):
    """Individual trade records from backtests."""
    __tablename__ = "backtest_trades"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    backtest_id: Mapped[int] = mapped_column(
        ForeignKey("backtest_results.id"), 
        index=True
    )
    
    strategy_id: Mapped[str] = mapped_column(String(128))
    product_id: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(16))  # buy/sell
    
    # Order details
    order_type: Mapped[str] = mapped_column(String(32))  # market, limit
    quantity: Mapped[float] = mapped_column(Numeric(20, 8))
    fill_price: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    
    # Execution tracking
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(32), server_default="filled")
    
    # Cost analysis
    fee_paid_usd: Mapped[float] = mapped_column(Numeric(20, 4), server_default="0")
    slippage_bps: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    notional_usd: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), nullable=True)


# ============================================================================
# PERFORMANCE SIGNALS (Alternative Strategy Output)
# ============================================================================

class PerformanceSignal(Base):
    """Store strategy signals for evaluation analysis."""
    __tablename__ = "performance_signals"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_id: Mapped[str] = mapped_column(String(64), unique=True)
    
    strategy_id: Mapped[str] = mapped_column(String(128))
    product_id: Mapped[str] = mapped_column(String(64))
    
    # Signal attributes
    side: Mapped[str] = mapped_column(String(16))
    quantity: Mapped[float] = mapped_column(Numeric(20, 8))
    order_type: Mapped[str] = mapped_column(String(32))
    limit_price: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    
    # Metadata
    strategy_version: Mapped[str] = mapped_column(String(32), server_default="1.0")
    confidence_score: Mapped[Optional[float]] = mapped_column(Numeric(5, 4), nullable=True)
    expiration_datetime: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# STRATEGY CERTIFICATION (Backtest Validation Results)
# ============================================================================

class StrategyCertification(Base):
    """Backtest certification records for strategy approval."""
    __tablename__ = "strategy_certifications"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_id: Mapped[str] = mapped_column(String(128), index=True)
    hypothesis_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    
    # Certification status
    status: Mapped[str] = mapped_column(String(32), server_default="pending")
    certification_score: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    
    # Performance thresholds
    min_sharpe_ratio: Mapped[Optional[float]] = mapped_column(Numeric(6, 2), nullable=True)
    max_drawdown_threshold: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    min_win_rate_pct: Mapped[Optional[float]] = mapped_column(Numeric(6, 2), nullable=True)
    min_profit_factor: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    
    # Certification metadata
    certified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    certified_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    
    rejection_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    review_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ============================================================================
# BACKTEST CONFIGURATION LOGGING
# ============================================================================

class BacktestConfiguration(Base):
    """Track backtest configuration versions and parameters."""
    __tablename__ = "backtest_configurations"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_id: Mapped[str] = mapped_column(String(128))
    config_version: Mapped[str] = mapped_column(String(32))  # Semantic version
    
    # Parameters stored as JSON in production (simplified here)
    parameters: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    replaced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    replaced_by_version: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# PERFORMANCE COMPARISONS (Multiple Strategies)
# ============================================================================

class StrategyComparison(Base):
    """Compare performance across multiple strategies."""
    __tablename__ = "strategy_comparisons"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    comparison_id: Mapped[str] = mapped_column(String(64), unique=True)
    
    # Comparison metadata
    title: Mapped[str] = mapped_column(String(256))
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    comparison_type: Mapped[str] = mapped_column(String(32))  # performance, risk, etc.
    
    # Strategies being compared
    strategy_ids: Mapped[str] = mapped_column(Text)  # JSON array of strategy IDs
    
    # Results stored as JSON (production implementation)
    results_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Status
    status: Mapped[str] = mapped_column(String(32), server_default="completed")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
