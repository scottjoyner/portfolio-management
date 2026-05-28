from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


# ============================================================================
# P0 FOUNDATION TABLES (Trading Infrastructure)
# ============================================================================


class Portfolio(Base):
    __tablename__ = "portfolios"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    objective: Mapped[str] = mapped_column(String(64))
    nav: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), server_default="0")
    available_capital: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), server_default="0")
    locked_capital: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), server_default="0")
    realized_pnl: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), server_default="0")
    unrealized_pnl: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), server_default="0")
    liquidity_score: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), server_default="0")
    capital_efficiency: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class StrategyConfig(Base):
    __tablename__ = "strategy_configs"
    strategy_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    strategy_type: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), server_default="implemented")
    paper_mode: Mapped[bool] = mapped_column(Boolean, server_default="true")
    live_supported: Mapped[bool] = mapped_column(Boolean, server_default="false")
    enabled: Mapped[bool] = mapped_column(Boolean, server_default="true")
    config_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    config_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    hypothesis_id: Mapped[Optional[str]] = mapped_column(String(64), ForeignKey("strategy_hypotheses.hypothesis_id"), nullable=True)
    certification_status: Mapped[str] = mapped_column(String(32), server_default="uncertified")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class StrategyRun(Base):
    __tablename__ = "strategy_runs"
    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String(128), ForeignKey("strategy_configs.strategy_id"))
    status: Mapped[str] = mapped_column(String(32), server_default="queued")
    mode: Mapped[str] = mapped_column(String(32), server_default="paper")
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    hypothesis_id: Mapped[Optional[str]] = mapped_column(String(64), ForeignKey("strategy_hypotheses.hypothesis_id"), nullable=True)


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
    remaining_size: Mapped[float] = mapped_column(Numeric(20, 8), server_default="0")
    price: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    notional: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), nullable=True)
    order_type: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


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
    slippage_bps: Mapped[float] = mapped_column(Numeric(10, 4), server_default="0")
    fee: Mapped[float] = mapped_column(Numeric(20, 4), server_default="0")
    liquidity: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Position(Base):
    __tablename__ = "positions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_id: Mapped[str] = mapped_column(String(64), ForeignKey("portfolios.id"))
    product_id: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(16))
    size: Mapped[float] = mapped_column(Numeric(20, 8))
    entry_price: Mapped[float] = mapped_column(Numeric(20, 8))
    current_price: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    unrealized_pnl: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), nullable=True)
    realized_pnl: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class AuditEvent(Base):
    __tablename__ = "audit_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(64))
    actor: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    resource_type: Mapped[str] = mapped_column(String(64))
    resource_id: Mapped[str] = mapped_column(String(64))
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Alert(Base):
    __tablename__ = "alerts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    severity: Mapped[str] = mapped_column(String(16))
    title: Mapped[str] = mapped_column(String(256))
    message: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(64))
    acknowledged: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Incident(Base):
    __tablename__ = "incidents"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    severity: Mapped[str] = mapped_column(String(16))
    title: Mapped[str] = mapped_column(String(256))
    summary: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), server_default="open")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class CapitalBucket(Base):
    __tablename__ = "capital_buckets"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64))
    amount: Mapped[float] = mapped_column(Numeric(20, 4), default=0)
    status: Mapped[str] = mapped_column(String(16), default="idle")
    portfolio_id: Mapped[Optional[str]] = mapped_column(String(64), ForeignKey("portfolios.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class PortfolioSleeve(Base):
    __tablename__ = "portfolio_sleeves"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_id: Mapped[str] = mapped_column(String(64), ForeignKey("portfolios.id"))
    name: Mapped[str] = mapped_column(String(64))
    weight: Mapped[float] = mapped_column(Numeric(6, 4))


class ExchangeState(Base):
    __tablename__ = "exchange_states"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    exchange: Mapped[str] = mapped_column(String(32), server_default="coinbase")
    trust_score: Mapped[str] = mapped_column(String(16), server_default="HEALTHY")
    open_orders_count: Mapped[int] = mapped_column(Integer, server_default="0")
    unknown_fills: Mapped[int] = mapped_column(Integer, server_default="0")
    duplicate_events: Mapped[int] = mapped_column(Integer, server_default="0")
    stale_sequence_gaps: Mapped[int] = mapped_column(Integer, server_default="0")
    snapshot_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ============================================================================
# P2 APPROVAL TABLE
# ============================================================================


class Approval(Base):
    __tablename__ = "approvals"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    approval_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    order_preview_id: Mapped[str] = mapped_column(String(64), ForeignKey("orders.preview_id"))
    status: Mapped[str] = mapped_column(String(32), server_default="pending")
    requested_by: Mapped[str] = mapped_column(String(128))
    approved_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    decision_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


# ============================================================================
# P5 APPROVAL PIPELINE
# ============================================================================


class StrategyApproval(Base):
    """Strategy approval packet — bundles hypothesis, evidence, and rollout plan."""
    __tablename__ = "strategy_approvals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    approval_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    strategy_id: Mapped[str] = mapped_column(String(128), ForeignKey("strategy_configs.strategy_id"))
    hypothesis_id: Mapped[Optional[str]] = mapped_column(String(64), ForeignKey("strategy_hypotheses.hypothesis_id"), nullable=True)
    config_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    philosophy: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    target_instruments: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    fair_value_logic: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    signal_rules: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    backtest_evidence_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    paper_evidence_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    expected_return_range: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    expected_risk_range: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    max_allocation: Mapped[Optional[float]] = mapped_column(Numeric(6, 2), nullable=True)
    capital_at_risk: Mapped[Optional[float]] = mapped_column(Numeric(20, 4), nullable=True)
    holding_period: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    exit_criteria: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stop_loss_policy: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    hedge_policy: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    kill_switch_policy: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    compliance_constraints: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    rollout_plan_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    required_approver: Mapped[str] = mapped_column(String(128))
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), server_default="pending")
    status_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    approved_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class TradeApproval(Base):
    """Trade approval packet — references a strategy approval and describes a specific intended order."""
    __tablename__ = "trade_approvals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    approval_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    strategy_approval_id: Mapped[str] = mapped_column(String(64), ForeignKey("strategy_approvals.approval_id"))
    strategy_id: Mapped[str] = mapped_column(String(128), ForeignKey("strategy_configs.strategy_id"))
    account: Mapped[str] = mapped_column(String(64))
    venue: Mapped[str] = mapped_column(String(64))
    product_id: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(16))
    order_type: Mapped[str] = mapped_column(String(32))
    order_bounds_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    fair_value_low: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    fair_value_high: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    expected_slippage_bps: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    expected_fee_bps: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    spread_bps: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    liquidity_score: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    fill_risk_score: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    position_exposure_impact_pct: Mapped[Optional[float]] = mapped_column(Numeric(6, 2), nullable=True)
    portfolio_exposure_impact_pct: Mapped[Optional[float]] = mapped_column(Numeric(6, 2), nullable=True)
    holding_period: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    exit_plan: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), server_default="pending")
    status_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    approved_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# P4 STRATEGY HYPOTHESIS REGISTRY
# ============================================================================


class StrategyHypothesis(Base):
    """Immutable strategy hypothesis record for certification and audit."""
    __tablename__ = "strategy_hypotheses"

    hypothesis_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String(128), ForeignKey("strategy_configs.strategy_id"))
    config_hash: Mapped[str] = mapped_column(String(64))
    philosophy: Mapped[str] = mapped_column(String(32))
    target_instruments: Mapped[str] = mapped_column(Text)
    timeframe: Mapped[str] = mapped_column(String(32))
    holding_period: Mapped[str] = mapped_column(String(32))
    signal_rules: Mapped[str] = mapped_column(Text)
    exit_rules: Mapped[str] = mapped_column(Text)
    risk_constraints: Mapped[str] = mapped_column(Text)
    expected_edge: Mapped[str] = mapped_column(String(128))
    config_snapshot: Mapped[str] = mapped_column(Text)
    author: Mapped[str] = mapped_column(String(128), server_default="system")
    version: Mapped[str] = mapped_column(String(32), server_default="1.0.0")
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class StrategyCertification(Base):
    """Backtest certification record for a strategy hypothesis."""
    __tablename__ = "strategy_certifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hypothesis_id: Mapped[str] = mapped_column(String(64), ForeignKey("strategy_hypotheses.hypothesis_id"))
    strategy_id: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), server_default="pending")
    sharpe: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    max_drawdown: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    total_return: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    win_rate: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    profit_factor: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)
    live_transfer_confidence: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    fragility_score: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    check_details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    rejection_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    certified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# P3 EVALUATION TABLES (Intrinsic Value & Market Intelligence)
# ============================================================================

class PriceEstimate(Base):
    """Price estimates for instruments - DCF intrinsic value and technical analysis."""
    __tablename__ = "price_estimates"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument: Mapped[str] = mapped_column(String(64))
    current_market_price: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    dcf_intrinsic_value: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    technical_score: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    consensus_vs_current_pct: Mapped[Optional[float]] = mapped_column(Numeric(6, 2), nullable=True)
    confidence_score: Mapped[float] = mapped_column(Numeric(5, 4), default=0.0)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AnalystRating(Base):
    """Analyst ratings and buy/sell/hold recommendations."""
    __tablename__ = "analyst_ratings"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument: Mapped[str] = mapped_column(String(64))
    analyst: Mapped[str] = mapped_column(String(128))
    rating_text: Mapped[str] = mapped_column(String(32))  # "BUY", "HOLD", "SELL"
    price_target: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class MarketDataFeed(Base):
    """Market data feed status and latency monitoring."""
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


class ResearchHypothesis(Base):
    """Trading hypotheses from research agents - high-confidence signals."""
    __tablename__ = "research_hypotheses"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_id: Mapped[str] = mapped_column(String(64))
    hypothesis_text: Mapped[str] = mapped_column(Text)
    confidence_score: Mapped[float] = mapped_column(Numeric(5, 4), default=0.0)
    expiration_datetime: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SentimentAnalysis(Base):
    """Market sentiment analysis - bullish/bearish/neutal signals."""
    __tablename__ = "sentiment_analysis"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_id: Mapped[str] = mapped_column(String(64))
    regime: Mapped[str] = mapped_column(String(32))  # BULLISH, BEARISH, NEUTRAL
    bullish_pct: Mapped[float] = mapped_column(Numeric(6, 2), default=0.0)
    bearish_pct: Mapped[float] = mapped_column(Numeric(6, 2), default=0.0)
    sentiment_score: Mapped[float] = mapped_column(Numeric(3, 4), default=0.0)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# P3 RISK MANAGEMENT TABLES (VaR, Drawdowns, Position Limits)
# ============================================================================

class ValueAtRisk(Base):
    """Value at Risk calculations by portfolio."""
    __tablename__ = "value_at_risk"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_id: Mapped[str] = mapped_column(String(64))
    confidence_level: Mapped[float] = mapped_column(Numeric(5, 2), default=0.95)  # 95% or 99%
    value_at_risk_usd: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    portfolio_volatility_30d: Mapped[float] = mapped_column(Numeric(6, 4), default=0.15)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Drawdown(Base):
    """Maximum drawdown tracking by portfolio."""
    __tablename__ = "drawdowns"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_id: Mapped[str] = mapped_column(String(64))
    peak_value: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    trough_value: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    drawdown_pct: Mapped[float] = mapped_column(Numeric(6, 4), default=0.0)
    duration_days: Mapped[int] = mapped_column(Integer, default=0)
    recovered: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class PositionLimit(Base):
    """Position limit configurations by product/category."""
    __tablename__ = "position_limits"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    category: Mapped[str] = mapped_column(String(64))  # SINGLE_POSITION, SECTOR, TOTAL_PORTFOLIO
    product_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    limit_pct: Mapped[float] = mapped_column(Numeric(6, 2), default=5.0)  # e.g., 5% of portfolio
    current_position_pct: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    breaching: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# P1.4 RUNTIME TABLES (Blockchain Events & Webhook Delivery)
# ============================================================================

class OnchainRuntimeEvent(Base):
    """On-chain blockchain events ingestion."""
    __tablename__ = "onchain_runtime_events"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chain_id: Mapped[str] = mapped_column(String(32))  # ETH_MAINNET, POLYGON_MAINNET, etc.
    event_type: Mapped[str] = mapped_column(String(64))  # TRANSFER, CONTRACT_CREATED, etc.
    contract_address: Mapped[Optional[str]] = mapped_column(String(42), nullable=True)
    block_number: Mapped[int] = mapped_column(Integer)
    log_index: Mapped[int] = mapped_column(Integer)
    data_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Webhook(Base):
    """Webhook subscription configurations."""
    __tablename__ = "webhooks"
    
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    url: Mapped[str] = mapped_column(String(256))
    events: Mapped[list[str]] = mapped_column(Text)  # JSON array of event types
    headers_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class WebhookDelivery(Base):
    """Webhook delivery tracking with retry logic."""
    __tablename__ = "webhook_deliveries"
    
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    webhook_id: Mapped[str] = mapped_column(String(64), ForeignKey("webhooks.id"))
    event_type: Mapped[str] = mapped_column(String(64))
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="pending")  # pending, success, failed
    last_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class InstrumentMetadata(Base):
    """Instrument metadata - ticker symbols, exchanges, trading hours."""
    __tablename__ = "instrument_metadata"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_id: Mapped[str] = mapped_column(String(64))
    ticker: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    exchange: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    underlying_asset: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    trading_hours_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# P2 ACCOUNTS TABLE (Plaid API Ingestion)
# ============================================================================

class Account(Base):
    """Bank account data from Plaid API ingestion."""
    __tablename__ = "accounts"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(64))
    plaid_account_id: Mapped[str] = mapped_column(String(128))
    bank_name: Mapped[str] = mapped_column(String(128))
    account_type: Mapped[str] = mapped_column(String(32))  # checking, savings, custodian
    official_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    current_balance: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    available_balance: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    currency: Mapped[str] = mapped_column(String(3), default="USD")
    institution_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    mask: Mapped[Optional[str]] = mapped_column(String(24), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ============================================================================
# P0 TRADES TABLE (Historical Trade Execution Log)
# ============================================================================

class Trade(Base):
    """Historical trade execution log with P&L tracking."""
    __tablename__ = "trades"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(64), ForeignKey("orders.order_id"))
    product_id: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(16))  # BUY, SELL
    execution_price: Mapped[float] = mapped_column(Numeric(20, 8))
    size: Mapped[float] = mapped_column(Numeric(20, 8))
    notional_usd: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    fees_paid: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    pnl_realized: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    pnl_unrealized: Mapped[Optional[float]] = mapped_column(Numeric(20, 8), nullable=True)
    market_impact_bps: Mapped[float] = mapped_column(Numeric(6, 2), default=0.0)
    execution_latency_ms: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
