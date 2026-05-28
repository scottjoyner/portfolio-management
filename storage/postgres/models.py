"""SQLAlchemy ORM Models for Trading System PostgreSQL Database (19 Tables)

These models define the schema for P0-P2 foundation tables and P3 evaluation tables:

P0 Foundation Tables (8):
- portfolios, capital_buckets, orders, fills, trade_history
- strategy_configs, approvals, approval_requests

P1.4 Runtime Tables (4):
- onchain_runtime_events, webhooks, webhook_deliveries
- instrument_metadata

P3 Evaluation Tables (7):
- price_estimates, analyst_ratings, market_data_feeds
- research_hypotheses, sentiment_analysis
- drawdowns, value_at_risk, position_limits
"""

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean, Text, Enum, ForeignKey, Text
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()


# ============================================================================
# P0 FOUNDATION TABLES - Portfolio Capital Management
# ============================================================================

class Portfolio(Base):
    """Portfolio representing a Plaid-connected investment account."""
    
    __tablename__ = "portfolios"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    type = Column(Enum("ACTIVE", "INACTIVE", "ARCHIVED"), default="ACTIVE")
    provider = Column(String(50), nullable=False)  # e.g., "chase_checking", "fidelity_cash"
    account_number = Column(String(50))
    
    # Balance tracking
    balance_usd = Column(Float, default=0.0)
    fiat_balance_usd = Column(Float, default=0.0)
    
    # Timestamps and state
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), 
        server_default=func.now(),
        onupdate=func.now()
    )
    last_synced = Column(DateTime(timezone=True))
    status = Column(Enum("CONNECTED", "DISCONNECTED", "ERROR"), default="CONNECTED")
    
    # Relationship to capital buckets and positions
    capital_buckets = relationship(
        "CapitalBucket", 
        back_populates="portfolio",
        cascade="all, delete-orphan"
    )
    trade_history = relationship(
        "TradeHistory", 
        back_populates="portfolio"
    )
    orders = relationship(
        "Order", 
        back_populates="portfolio"
    )


class CapitalBucket(Base):
    """Capital allocation bucket for diversified investing."""
    
    __tablename__ = "capital_buckets"
    
    id = Column(Integer, primary_key=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"), nullable=False)
    
    # Allocation metadata
    name = Column(String(100), nullable=False)  # e.g., "Core Equity", "Bond Bond Allocation"
    bucket_type = Column(Enum("EQUITY", "FIXED_INCOME", "CASH", "ALTERNATIVE"))
    
    # Target and current allocations
    target_percentage = Column(Float, default=25.0)  # 25% of portfolio
    current_percentage = Column(Float, default=0.0)
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# P0 ORDERS AND TRADES - Order Execution Lifecycle
# ============================================================================

class Order(Base):
    """Order with execution lifecycle tracking."""
    
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"), nullable=False)
    
    # Order metadata
    product_id = Column(String(50), nullable=False)  # e.g., "AAPL", "BTC-USD"
    side = Column(Enum("BUY", "SELL"), nullable=False)
    order_type = Column(Enum("MARKET", "LIMIT", "STOP_LIMIT"))
    
    # Size tracking
    original_size = Column(Float, nullable=False)  # Quantity or USD value
    remaining_size = Column(Float, default=0.0)
    filled_size = Column(Float, default=0.0)
    
    # Price (nullable for market orders)
    price = Column(Float, nullable=True)
    
    # Order state and timing
    status = Column(Enum("PENDING", "OPEN", "PARTIALLY_FILLED", "CLOSED", "CANCELLED"), 
                    default="PENDING")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), 
        server_default=func.now(),
        onupdate=func.now()
    )
    filled_at = Column(DateTime(timezone=True))


class Fill(Base):
    """Individual order fill (execution of portion of order)."""
    
    __tablename__ = "fills"
    
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    
    # Fill details
    size_filled = Column(Float, nullable=False)  # Quantity or USD filled
    price_per_unit = Column(Float, nullable=False)
    commission_fee = Column(Float, default=0.0)
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class TradeHistory(Base):
    """Executed trade with final aggregation."""
    
    __tablename__ = "trade_history"
    
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=True)  # Can be standalone
    
    # Trade details
    product_id = Column(String(50), nullable=False)
    side = Column(Enum("BUY", "SELL"), nullable=False)
    quantity = Column(Float, nullable=False)  # Shares/units purchased/sold
    price_per_unit = Column(Float, nullable=False)
    
    # P&L tracking
    cost_basis = Column(Float, default=0.0)
    proceeds = Column(Float, default=0.0)
    profit_loss = Column(Float, default=0.0)  # Realized P&L
    
    # Metadata
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# P0 STRATEGIES - Strategy Configuration and Performance
# ============================================================================

class StrategyConfig(Base):
    """Strategy configuration from backtesting database."""
    
    __tablename__ = "strategy_configs"
    
    id = Column(Integer, primary_key=True)
    config_key = Column(String(50), unique=True, nullable=False)
    name = Column(String(100))
    description = Column(Text)
    
    # Category and metadata
    category = Column(String(50), default="momentum")  # momentum, mean_reversion, etc.
    
    # Backtesting results
    backtested = Column(Boolean, default=True)
    sharpe_ratio = Column(Float, nullable=True)
    max_drawdown = Column(Float, nullable=True)
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# P0 APPROVALS - Approval Workflow for Trades
# ============================================================================

class Approval(Base):
    """Approval request for trade execution."""
    
    __tablename__ = "approvals"
    
    id = Column(Integer, primary_key=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"))
    strategy_id = Column(Integer, ForeignKey("strategy_configs.id"))
    
    # Request details
    product_id = Column(String(50))
    side = Column(Enum("BUY", "SELL"))
    quantity = Column(Float)
    estimated_cost = Column(Float)
    
    # Status tracking
    status = Column(Enum("PENDING", "IN_REVIEW", "APPROVED", "REJECTED"), default="PENDING")
    reviewer_notes = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    reviewed_at = Column(DateTime(timezone=True))


# ============================================================================
# P1.4 RUNTIME TABLES - On-Chain Execution and Webhooks
# ============================================================================

class OnchainRuntimeEvent(Base):
    """On-chain execution event from smart contract."""
    
    __tablename__ = "onchain_runtime_events"
    
    id = Column(Integer, primary_key=True)
    event_type = Column(String(50), nullable=False)  # e.g., "TOKEN_SWAP", "LIQUIDITY_ADD"
    
    # Event data
    chain_id = Column(Integer, nullable=False)  # Ethereum chain ID
    transaction_hash = Column(String(64))
    contract_address = Column(String(64))
    
    status = Column(Enum("PENDING", "COMPLETED", "FAILED"), default="PENDING")
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class Webhook(Base):
    """Webhook subscription for event delivery."""
    
    __tablename__ = "webhooks"
    
    id = Column(Integer, primary_key=True)
    url = Column(String(512), nullable=False)
    event_type = Column(String(50))  # e.g., "TRADE_EXECUTED", "APPROVAL_REVIEWED"
    
    # Status and authentication
    status = Column(Enum("ACTIVE", "INACTIVE", "ERROR"), default="ACTIVE")
    secret_key = Column(String(64), nullable=True)
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class WebhookDelivery(Base):
    """Webhook delivery tracking."""
    
    __tablename__ = "webhook_deliveries"
    
    id = Column(Integer, primary_key=True)
    webhook_id = Column(Integer, ForeignKey("webhooks.id"))
    event_data = Column(Text, nullable=False)  # JSON payload
    
    # Delivery status
    status = Column(Enum("PENDING", "SUCCESS", "FAILED"), default="PENDING")
    response_status_code = Column(Integer, nullable=True)
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# P3 EVALUATION TABLES - Valuation and Market Data
# ============================================================================

class PriceEstimate(Base):
    """Price estimates from multiple valuation models."""
    
    __tablename__ = "price_estimates"
    
    id = Column(Integer, primary_key=True)
    instrument = Column(String(50), nullable=False)  # e.g., "AAPL", "BTC-USD"
    
    # DCF valuation
    dcf_intrinsic_value = Column(Float, nullable=True)
    
    # Technical indicators
    technical_score = Column(Float, nullable=True)  # 0-1 signal strength
    
    # Consensus and confidence
    current_market_price = Column(Float, nullable=True)
    consensus_vs_current_pct = Column(Float, nullable=True)  # e.g., -5.2 means 5% below market
    
    # Model confidence
    confidence_score = Column(Float, default=0.7)  # 0-1
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class AnalystRating(Base):
    """Analyst consensus ratings and price targets."""
    
    __tablename__ = "analyst_ratings"
    
    id = Column(Integer, primary_key=True)
    instrument = Column(String(50))
    analyst_firm = Column(String(100))
    
    rating = Column(String(20), nullable=True)  # e.g., "Overweight", "Buy"
    price_target = Column(Float, nullable=True)
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class MarketDataFeed(Base):
    """Historical market data (OHLCV) feed."""
    
    __tablename__ = "market_data_feeds"
    
    id = Column(Integer, primary_key=True)
    instrument = Column(String(50), nullable=False)
    
    # OHLCV data
    open_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    close_price = Column(Float)
    volume = Column(Float, default=0.0)
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class InstrumentMetadata(Base):
    """Current price and market cap metadata."""
    
    __tablename__ = "instrument_metadata"
    
    id = Column(Integer, primary_key=True)
    instrument = Column(String(50), unique=True, nullable=False)
    
    # Market data
    current_price = Column(Float)
    market_cap = Column(Float, nullable=True)
    volume_24h = Column(Float, default=0.0)
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class ResearchHypothesis(Base):
    """Trading hypothesis generated from agentic research."""
    
    __tablename__ = "research_hypotheses"
    
    id = Column(Integer, primary_key=True)
    strategy_id = Column(Integer, ForeignKey("strategy_configs.id"))
    product_id = Column(String(50), nullable=True)
    
    # Hypothesis content
    hypothesis_text = Column(Text, nullable=False)
    
    # Confidence and timing
    confidence_score = Column(Float, default=0.6)
    expiration_datetime = Column(DateTime(timezone=True), nullable=True)
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class SentimentAnalysis(Base):
    """Sentiment analysis from news/articles."""
    
    __tablename__ = "sentiment_analysis"
    
    id = Column(Integer, primary_key=True)
    instrument = Column(String(50))
    source = Column(String(100))  # e.g., "Bloomberg", "Reuters"
    
    sentiment_score = Column(Float, default=0.0)  # -1 to +1
    sentiment_label = Column(Enum("POSITIVE", "NEGATIVE", "NEUTRAL"), default="NEUTRAL")
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


# ============================================================================
# P3 RISK MANAGEMENT TABLES
# ============================================================================

class Drawdown(Base):
    """Drawdown period tracking."""
    
    __tablename__ = "drawdowns"
    
    id = Column(Integer, primary_key=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"))
    
    # Drawdown details
    peak_value = Column(Float, nullable=False)
    trough_value = Column(Float, nullable=False)
    drawdown_pct = Column(Float, nullable=False)  # e.g., -15.3 means 15.3% below peak
    
    start_date = Column(DateTime(timezone=True), server_default=func.now())
    end_date = Column(DateTime(timezone=True), nullable=True)  # NULL if ongoing
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class ValueAtRisk(Base):
    """Value at Risk (VaR) calculations."""
    
    __tablename__ = "value_at_risk"
    
    id = Column(Integer, primary_key=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"))
    
    # VaR metrics
    var_95_pct = Column(Float)  # 95% confidence VaR
    var_99_pct = Column(Float)  # 99% confidence VaR
    expected_shortfall = Column(Float, nullable=True)
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class PositionLimit(Base):
    """Position size limits by instrument type."""
    
    __tablename__ = "position_limits"
    
    id = Column(Integer, primary_key=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"))
    
    # Limit configuration
    instrument_type = Column(String(50), nullable=False)  # e.g., "CRYPTO", "EQUITY"
    limit_percentage_of_portfolio = Column(Float, default=25.0)  # 25% of portfolio max
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
