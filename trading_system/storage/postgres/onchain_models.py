# =============================================================================
# Onchain Runtime Models (P1.4) - SQLAlchemy ORM Models
# =============================================================================

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Integer, Numeric, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TokenMetadata(Base):
    """Cached token metadata with 24-hour TTL caching support."""
    __tablename__ = "token_metadata"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    address: Mapped[str] = mapped_column(String(66), unique=True, index=True)
    chain: Mapped[str] = mapped_column(String(32), default="base")
    symbol: Mapped[Optional[str]] = mapped_column(String(48), nullable=True)
    name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    decimals: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    token_uri: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_fetch_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    fetch_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class PoolSnapshot(Base):
    """Periodic pool state snapshots for monitoring and replay."""
    __tablename__ = "pool_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    network: Mapped[str] = mapped_column(String(32), index=True)
    pool_address: Mapped[Optional[str]] = mapped_column(String(66), nullable=True)
    pool_type: Mapped[Optional[str]] = mapped_column(String(48), nullable=True)  # AMM, concentrated, perp
    protocol: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    snapshot_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    pool_data_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # Full snapshot JSON
    health_score: Mapped[float] = mapped_column(Numeric(6, 4), default=1.0)
    is_valid: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ContractEvent(Base):
    """Contract event logs with feed health tracking."""
    __tablename__ = "contract_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    network: Mapped[str] = mapped_column(String(32), index=True)
    contract_address: Mapped[str] = mapped_column(String(66), index=True)
    event_type: Mapped[str] = mapped_column(String(48))  # swap, transfer, approve
    block_number: Mapped[int] = mapped_column(Integer)
    transaction_hash: Mapped[Optional[str]] = mapped_column(String(66), nullable=True)
    indexed_args_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_log: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    event_index: Mapped[int] = mapped_column(Integer, default=0)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class FeedHealthRecord(Base):
    """Feed health monitoring with metrics counters."""
    __tablename__ = "feed_health_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    network: Mapped[str] = mapped_column(String(32), index=True)
    feed_type: Mapped[str] = mapped_column(String(32))  # pool_snapshot, event_listener, token_metadata
    status: Mapped[str] = mapped_column(String(16), default="healthy")  # healthy, degraded, error
    last_success_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    metrics_polls_total: Mapped[int] = mapped_column(Integer, default=0)
    metrics_last_error_time_seconds: Mapped[float] = mapped_column(Numeric(15, 6), default=0.0)
    metrics_latency_ms: Mapped[Optional[float]] = mapped_column(Numeric(12, 3), nullable=True)
    last_poll_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
