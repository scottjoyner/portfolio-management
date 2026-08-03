"""Canonical SQLAlchemy models for the operational PostgreSQL schema.

The table and column definitions in this module intentionally mirror
``trading_system/alembic/versions/0001_initial.py``.  Older revisions of this
file described a different portfolio schema, which made Alembic, the API, and
``OpsRepository`` disagree depending on Python import order.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import declarative_base, relationship, synonym

Base = declarative_base()


class Portfolio(Base):
    __tablename__ = "portfolios"

    id = Column(String(64), primary_key=True)
    name = Column(String(128), nullable=False)
    objective = Column(String(64), nullable=False)
    nav = Column(Numeric(20, 4), nullable=False, default=0, server_default="0")
    available_capital = Column(Numeric(20, 4), nullable=False, default=0, server_default="0")
    locked_capital = Column(Numeric(20, 4), nullable=False, default=0, server_default="0")
    realized_pnl = Column(Numeric(20, 4), nullable=False, default=0, server_default="0")
    unrealized_pnl = Column(Numeric(20, 4), nullable=False, default=0, server_default="0")
    liquidity_score = Column(Numeric(6, 4), nullable=False, default=0, server_default="0")
    capital_efficiency = Column(Numeric(6, 4), nullable=False, default=0, server_default="0")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    sleeves = relationship("PortfolioSleeve", back_populates="portfolio", cascade="all, delete-orphan")
    orders = relationship("Order", back_populates="portfolio")
    capital_buckets = relationship("CapitalBucket", back_populates="portfolio", cascade="all, delete-orphan")

    # Non-persistent compatibility views for callers from the retired account
    # schema. They deliberately do not add columns absent from Alembic 0001.
    @property
    def balance_usd(self) -> float:
        return float(self.nav or 0)

    @balance_usd.setter
    def balance_usd(self, value: float) -> None:
        self.nav = value

    @property
    def fiat_balance_usd(self) -> float:
        return float(self.available_capital or 0)

    @fiat_balance_usd.setter
    def fiat_balance_usd(self, value: float) -> None:
        self.available_capital = value


class PortfolioSleeve(Base):
    __tablename__ = "portfolio_sleeves"

    id = Column(Integer, primary_key=True, autoincrement=True)
    portfolio_id = Column(String(64), ForeignKey("portfolios.id"), nullable=False)
    name = Column(String(64), nullable=False)
    weight = Column(Numeric(6, 4), nullable=False)

    portfolio = relationship("Portfolio", back_populates="sleeves")


class StrategyConfig(Base):
    __tablename__ = "strategy_configs"

    strategy_id = Column(String(128), primary_key=True)
    strategy_type = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, default="implemented", server_default="implemented")
    paper_mode = Column(Boolean, nullable=False, default=True, server_default="true")
    live_supported = Column(Boolean, nullable=False, default=False, server_default="false")
    replay_supported = Column(Boolean, nullable=False, default=True, server_default="true")
    backtest_supported = Column(Boolean, nullable=False, default=True, server_default="true")
    risk_mode_hint = Column(String(32), nullable=False, default="NORMAL", server_default="NORMAL")
    capital_bucket = Column(String(32), nullable=False, default="ACTIVE_TRADING", server_default="ACTIVE_TRADING")
    enabled = Column(Boolean, nullable=False, default=True, server_default="true")
    config_json = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    runs = relationship("StrategyRun", back_populates="strategy")

    # Compatibility aliases used by a small amount of older strategy code.
    id = synonym("strategy_id")
    config_key = synonym("strategy_id")
    category = synonym("strategy_type")


class StrategyRun(Base):
    __tablename__ = "strategy_runs"

    task_id = Column(String(64), primary_key=True)
    strategy_id = Column(String(128), ForeignKey("strategy_configs.strategy_id"), nullable=False)
    status = Column(String(32), nullable=False, default="queued", server_default="queued")
    mode = Column(String(32), nullable=False, default="paper", server_default="paper")
    queued_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)

    strategy = relationship("StrategyConfig", back_populates="runs")


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(64), unique=True, index=True, nullable=True)
    preview_id = Column(String(64), nullable=True)
    strategy_id = Column(String(128), ForeignKey("strategy_configs.strategy_id"), nullable=False)
    portfolio_id = Column(String(64), ForeignKey("portfolios.id"), nullable=False)
    sleeve_id = Column(String(64), nullable=True)
    product_id = Column(String(64), nullable=False)
    side = Column(String(16), nullable=False)
    size = Column(Numeric(20, 8), nullable=False)
    remaining_size = Column(Numeric(20, 8), nullable=False, default=0, server_default="0")
    price = Column(Numeric(20, 8), nullable=True)
    notional = Column(Numeric(20, 4), nullable=True)
    order_type = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False)
    maker_taker_expectation = Column(String(16), nullable=True)
    queue_age_s = Column(Integer, nullable=False, default=0, server_default="0")
    risk_mode = Column(String(32), nullable=False, default="NORMAL", server_default="NORMAL")
    reduce_only = Column(Boolean, nullable=False, default=False, server_default="false")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    portfolio = relationship("Portfolio", back_populates="orders")
    fills = relationship("Fill", back_populates="order", cascade="all, delete-orphan")

    # Retired execution-schema compatibility aliases.
    original_size = synonym("size")

    @property
    def filled_size(self) -> Decimal:
        return (self.size or Decimal("0")) - (self.remaining_size or Decimal("0"))


class StrategyAllocation(Base):
    __tablename__ = "strategy_allocations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    portfolio_id = Column(String(64), ForeignKey("portfolios.id"), nullable=False)
    strategy_id = Column(String(128), nullable=False)
    weight = Column(Numeric(6, 4), nullable=False)


class Fill(Base):
    __tablename__ = "fills"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fill_id = Column(String(64), unique=True, index=True, nullable=True)
    order_id = Column(String(64), ForeignKey("orders.order_id"), nullable=False)
    product_id = Column(String(64), nullable=False)
    side = Column(String(16), nullable=True)
    size = Column(Numeric(20, 8), nullable=False)
    price = Column(Numeric(20, 8), nullable=False)
    notional = Column(Numeric(20, 4), nullable=False)
    slippage_bps = Column(Numeric(10, 4), nullable=False, default=0, server_default="0")
    fee = Column(Numeric(20, 4), nullable=False, default=0, server_default="0")
    fee_currency = Column(String(16), nullable=True)
    liquidity = Column(String(16), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    order = relationship("Order", back_populates="fills")

    size_filled = synonym("size")
    price_per_unit = synonym("price")
    commission_fee = synonym("fee")
    timestamp = synonym("created_at")


class CapitalBucket(Base):
    __tablename__ = "capital_buckets"

    id = Column(String(64), primary_key=True)
    portfolio_id = Column(String(64), ForeignKey("portfolios.id"), nullable=False)
    name = Column(String(64), nullable=False)
    bucket_type = Column(String(32), nullable=False)
    amount = Column(Numeric(20, 4), nullable=False, default=0, server_default="0")
    target_weight = Column(Numeric(6, 4), nullable=False, default=0, server_default="0")
    min_weight = Column(Numeric(6, 4), nullable=False, default=0, server_default="0")
    max_weight = Column(Numeric(6, 4), nullable=False, default=1, server_default="1")
    locked = Column(Boolean, nullable=False, default=False, server_default="false")
    status = Column(String(32), nullable=False, default="idle", server_default="idle")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    portfolio = relationship("Portfolio", back_populates="capital_buckets")
    target_percentage = synonym("target_weight")
    current_percentage = synonym("amount")


class Approval(Base):
    __tablename__ = "approvals"

    approval_id = Column(String(64), primary_key=True)
    approval_type = Column(String(32), nullable=False)
    summary = Column(String(256), nullable=False)
    capital_affected = Column(Numeric(20, 4), nullable=False, default=0, server_default="0")
    liquidity_impact = Column(String(32), nullable=True)
    risk_impact = Column(String(32), nullable=True)
    status = Column(String(32), nullable=False, default="pending", server_default="pending")
    approved_by = Column(String(128), nullable=True)
    approved_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=True)

    id = synonym("approval_id")


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String(64), index=True, nullable=False)
    actor = Column(String(128), nullable=True)
    resource_type = Column(String(64), nullable=True)
    resource_id = Column(String(64), nullable=True)
    details = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Alert(Base):
    __tablename__ = "alerts"

    alert_id = Column(String(64), primary_key=True)
    severity = Column(String(16), nullable=False)
    summary = Column(String(256), nullable=False)
    acknowledged = Column(Boolean, nullable=False, default=False, server_default="false")
    acknowledged_by = Column(String(128), nullable=True)
    acknowledged_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    id = synonym("alert_id")
    message = synonym("summary")


class Incident(Base):
    __tablename__ = "incidents"

    incident_id = Column(String(64), primary_key=True)
    severity = Column(String(16), nullable=False)
    summary = Column(String(256), nullable=False)
    status = Column(String(32), nullable=False, default="monitoring", server_default="monitoring")
    assigned_to = Column(String(128), nullable=True)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    id = synonym("incident_id")


class ExchangeState(Base):
    __tablename__ = "exchange_states"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String(32), nullable=False, default="coinbase", server_default="coinbase")
    trust_score = Column(String(16), nullable=False, default="HEALTHY", server_default="HEALTHY")
    open_orders_count = Column(Integer, nullable=False, default=0, server_default="0")
    unknown_fills = Column(Integer, nullable=False, default=0, server_default="0")
    duplicate_events = Column(Integer, nullable=False, default=0, server_default="0")
    stale_sequence_gaps = Column(Integer, nullable=False, default=0, server_default="0")
    snapshot_json = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class MarketDataFeed(Base):
    __tablename__ = "market_data_feeds"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feed_name = Column(String(64), nullable=False)
    state = Column(String(16), nullable=False, default="healthy", server_default="healthy")
    freshness_ms = Column(Integer, nullable=False, default=0, server_default="0")
    update_rate_hz = Column(Numeric(10, 4), nullable=False, default=0, server_default="0")
    dropped_messages_1m = Column(Integer, nullable=False, default=0, server_default="0")
    failover_active = Column(Boolean, nullable=False, default=False, server_default="false")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


__all__ = [
    "Base",
    "Portfolio",
    "PortfolioSleeve",
    "StrategyConfig",
    "StrategyRun",
    "Order",
    "StrategyAllocation",
    "Fill",
    "CapitalBucket",
    "Approval",
    "AuditEvent",
    "Alert",
    "Incident",
    "ExchangeState",
    "MarketDataFeed",
]
