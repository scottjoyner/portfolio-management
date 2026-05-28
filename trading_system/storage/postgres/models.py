    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


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
