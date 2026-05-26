from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Portfolio(Base):
    __tablename__ = "portfolios"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    objective: Mapped[str] = mapped_column(String(64))
    nav: Mapped[float] = mapped_column(Numeric(20, 4), default=0)
    available_capital: Mapped[float] = mapped_column(Numeric(20, 4), default=0)
    locked_capital: Mapped[float] = mapped_column(Numeric(20, 4), default=0)
    realized_pnl: Mapped[float] = mapped_column(Numeric(20, 4), default=0)
    unrealized_pnl: Mapped[float] = mapped_column(Numeric(20, 4), default=0)
    liquidity_score: Mapped[float] = mapped_column(Numeric(6, 4), default=0)
    capital_efficiency: Mapped[float] = mapped_column(Numeric(6, 4), default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    sleeves: Mapped[list[PortfolioSleeve]] = relationship(back_populates="portfolio", cascade="all, delete-orphan")
    strategy_allocations: Mapped[list[StrategyAllocation]] = relationship(back_populates="portfolio", cascade="all, delete-orphan")


class PortfolioSleeve(Base):
    __tablename__ = "portfolio_sleeves"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_id: Mapped[str] = mapped_column(String(64), ForeignKey("portfolios.id"))
    name: Mapped[str] = mapped_column(String(64))
    weight: Mapped[float] = mapped_column(Numeric(6, 4))

    portfolio: Mapped[Portfolio] = relationship(back_populates="sleeves")


class StrategyAllocation(Base):
    __tablename__ = "strategy_allocations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_id: Mapped[str] = mapped_column(String(64), ForeignKey("portfolios.id"))
    strategy_id: Mapped[str] = mapped_column(String(128))
    weight: Mapped[float] = mapped_column(Numeric(6, 4))

    portfolio: Mapped[Portfolio] = relationship(back_populates="strategy_allocations")


class StrategyConfig(Base):
    __tablename__ = "strategy_configs"

    strategy_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    strategy_type: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), default="implemented")
    paper_mode: Mapped[bool] = mapped_column(default=True)
    live_supported: Mapped[bool] = mapped_column(default=False)
    replay_supported: Mapped[bool] = mapped_column(default=True)
    backtest_supported: Mapped[bool] = mapped_column(default=True)
    risk_mode_hint: Mapped[str] = mapped_column(String(32), default="NORMAL")
    capital_bucket: Mapped[str] = mapped_column(String(32), default="ACTIVE_TRADING")
    enabled: Mapped[bool] = mapped_column(default=True)
    config_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class StrategyRun(Base):
    __tablename__ = "strategy_runs"

    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String(128), ForeignKey("strategy_configs.strategy_id"))
    status: Mapped[str] = mapped_column(String(32), default="queued")
    mode: Mapped[str] = mapped_column(String(32), default="paper")
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    preview_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    strategy_id: Mapped[str] = mapped_column(String(128), ForeignKey("strategy_configs.strategy_id"))
    portfolio_id: Mapped[str] = mapped_column(String(64), ForeignKey("portfolios.id"))
    sleeve_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    product_id: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(16))
    size: Mapped[float] = mapped_column(Numeric(20, 8))
    remaining_size: Mapped[float] = mapped_column(Numeric(20, 8), default=0)
    price: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    notional: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), nullable=True)
    order_type: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32))
    maker_taker_expectation: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    queue_age_s: Mapped[int] = mapped_column(Integer, default=0)
    risk_mode: Mapped[str] = mapped_column(String(32), default="NORMAL")
    reduce_only: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Fill(Base):
    __tablename__ = "fills"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fill_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    order_id: Mapped[str] = mapped_column(String(64), ForeignKey("orders.order_id"))
    product_id: Mapped[str] = mapped_column(String(64))
    side: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    size: Mapped[float] = mapped_column(Numeric(20, 8))
    price: Mapped[float] = mapped_column(Numeric(20, 8))
    notional: Mapped[float] = mapped_column(Numeric(20, 4))
    slippage_bps: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    fee: Mapped[float] = mapped_column(Numeric(20, 4), default=0)
    fee_currency: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    liquidity: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class CapitalBucket(Base):
    __tablename__ = "capital_buckets"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String(64), ForeignKey("portfolios.id"))
    name: Mapped[str] = mapped_column(String(64))
    bucket_type: Mapped[str] = mapped_column(String(32))
    amount: Mapped[float] = mapped_column(Numeric(20, 4), default=0)
    target_weight: Mapped[float] = mapped_column(Numeric(6, 4), default=0)
    min_weight: Mapped[float] = mapped_column(Numeric(6, 4), default=0)
    max_weight: Mapped[float] = mapped_column(Numeric(6, 4), default=1)
    locked: Mapped[bool] = mapped_column(default=False)
    status: Mapped[str] = mapped_column(String(32), default="idle")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Approval(Base):
    __tablename__ = "approvals"

    approval_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    approval_type: Mapped[str] = mapped_column(String(32))
    summary: Mapped[str] = mapped_column(String(256))
    capital_affected: Mapped[float] = mapped_column(Numeric(20, 4), default=0)
    liquidity_impact: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    risk_impact: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="pending")
    approved_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    actor: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    resource_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    resource_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Alert(Base):
    __tablename__ = "alerts"

    alert_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    severity: Mapped[str] = mapped_column(String(16))
    summary: Mapped[str] = mapped_column(String(256))
    acknowledged: Mapped[bool] = mapped_column(default=False)
    acknowledged_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Incident(Base):
    __tablename__ = "incidents"

    incident_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    severity: Mapped[str] = mapped_column(String(16))
    summary: Mapped[str] = mapped_column(String(256))
    status: Mapped[str] = mapped_column(String(32), default="monitoring")
    assigned_to: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class ExchangeState(Base):
    __tablename__ = "exchange_states"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    exchange: Mapped[str] = mapped_column(String(32), default="coinbase")
    trust_score: Mapped[str] = mapped_column(String(16), default="HEALTHY")
    open_orders_count: Mapped[int] = mapped_column(Integer, default=0)
    unknown_fills: Mapped[int] = mapped_column(Integer, default=0)
    duplicate_events: Mapped[int] = mapped_column(Integer, default=0)
    stale_sequence_gaps: Mapped[int] = mapped_column(Integer, default=0)
    snapshot_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class MarketDataFeed(Base):
    __tablename__ = "market_data_feeds"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    feed_name: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(16), default="healthy")
    freshness_ms: Mapped[int] = mapped_column(Integer, default=0)
    update_rate_hz: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    dropped_messages_1m: Mapped[int] = mapped_column(Integer, default=0)
    failover_active: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
