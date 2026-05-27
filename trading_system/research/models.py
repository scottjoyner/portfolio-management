"""Database models for research and hypothesis generation."""

from sqlalchemy import Column, Integer, Float, String, DateTime, ForeignKey, Text, JSON
from sqlalchemy.orm import relationship


class HypothesisModel(Base):
    """Generated trading hypothesis model."""
    
    __tablename__ = 'trading_hypotheses'
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Hypothesis metadata
    name = Column(String(256), nullable=False)
    description = Column(Text, nullable=True)
    strategy_type = Column(String(50), nullable=False)  # "mean-reversion" | "momentum" | "carry" etc.
    generated_by = Column(String(128), nullable=True)  # Model/service identifier
    
    # Target instruments
    target_instruments = Column(JSON, nullable=True)  # ["ETH", "BTC"] or null
    
    # Expected relationships
    expected_correlation = Column(Float, nullable=True)  # -1 to 1
    correlation_window_days = Column(Integer, nullable=True, default=252)
    
    # Confidence tracking
    confidence_score = Column(Float, nullable=False)  # 0-1 normalized
    validation_status = Column(String(32), nullable=True, default="pending")  # "pending" | "backtested" | "rejected"
    
    # Market regime
    market_regime_state = Column(String(50), nullable=True)  # "bull" | "bear" | "sideways"
    volatility_percentile = Column(Float, nullable=True)  # 0-1 percentile
    
    # Results from validation
    backtest_total_return = Column(Float, nullable=True)
    backtest_sharpe_ratio = Column(Float, nullable=True)
    backtest_max_drawdown = Column(Float, nullable=True)
    
    # Timestamps
    generated_at = Column(DateTime, default=lambda: datetime.now())
    validated_at = Column(DateTime, nullable=True)
    rejected_at = Column(DateTime, nullable=True)
    rejection_reason = Column(Text, nullable=True)
    
    # Relations
    backtest_results = relationship("BacktestResultModel", back_populates="hypothesis")


class MarketRegimeSnapshot(Base):
    """Market regime snapshot for hypothesis generation."""
    
    __tablename__ = 'market_regime_snapshots'
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Regime classification
    state = Column(String(50), nullable=False)  # "bull" | "bear" | "sideways"
    volatility_percentile = Column(Float, nullable=True)  # 0-1 normalized
    
    # Market indicators (stored as JSON for flexibility)
    price_levels = Column(JSON, nullable=True)  # {"ETH": 5000.5, "BTC": 65000.2}
    market_momentum_30d = Column(String(10), nullable=True)  # "+2.5%" or "-1.2%"
    vix_equivalent_20d = Column(Float, nullable=True)  # Volatility index equivalent
    
    # Correlation matrix (stored as JSON)
    correlation_matrix = Column(JSON, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=lambda: datetime.now())


class SignalCorrelationModel(Base):
    """Signal correlation analysis results."""
    
    __tablename__ = 'signal_correlations'
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Instruments being analyzed
    instrument_a = Column(String(50), nullable=False)
    instrument_b = Column(String(50), nullable=False)
    
    # Correlation metrics
    pearson_correlation = Column(Float, nullable=True)  # Linear correlation
    spearman_correlation = Column(Float, nullable=True)  # Rank correlation
    cointegration_score = Column(Float, nullable=True)  # Cointegration test score
    
    # Statistical properties
    correlation_window_days = Column(Integer, nullable=True, default=252)
    lag_optimal = Column(Integer, nullable=True)  # Optimal lag for correlation
    
    # Created at (always when new data arrives)
    analyzed_at = Column(DateTime, default=lambda: datetime.now())


class BacktestResultModel(Base):
    """Backtest result linked to hypothesis."""
    
    __tablename__ = 'backtest_results'
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Links to related objects
    strategy_id = Column(String(128), nullable=False)
    hypothesis_id = Column(Integer, ForeignKey("trading_hypotheses.id"), nullable=False)
    
    # Backtest configuration
    ohlcv_source = Column(String(256), nullable=True)  # API endpoint or file path
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    interval = Column(String(10), nullable=True)  # "1m", "5m", "1h", etc.
    
    # Performance metrics
    initial_capital = Column(Float, nullable=False)
    final_capital = Column(Float, nullable=True)
    total_return_pct = Column(Float, nullable=True)  # Percentage return
    
    win_rate = Column(Float, nullable=True)  # 0-1 normalized
    profit_factor = Column(Float, nullable=True)
    sharpe_ratio = Column(Float, nullable=True)
    max_drawdown_pct = Column(Float, nullable=True)
    
    trades_executed = Column(Integer, nullable=False, default=0)
    avg_trade_duration_minutes = Column(Float, nullable=True)
    
    # Risk-adjusted metrics
    volatility_adjusted_return = Column(Float, nullable=True)
    calmar_ratio = Column(Float, nullable=True)  # Return / max drawdown
    
    # Status
    status = Column(String(32), nullable=False, default="completed")  # "completed" | "error"
    
    created_at = Column(DateTime, default=lambda: datetime.now())


class ResearchExperimentModel(Base):
    """Research experiment tracking for hypothesis validation."""
    
    __tablename__ = 'research_experiments'
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Experiment metadata
    name = Column(String(256), nullable=False)
    description = Column(Text, nullable=True)
    objective = Column(String(512), nullable=False)
    
    # Strategy being tested
    strategy_type = Column(String(50), nullable=True)
    strategy_key = Column(String(256), nullable=True)
    version = Column(String(32), nullable=True)
    
    # Experimental design
    hypothesis_name = Column(String(256), nullable=True)
    hypothesis_description = Column(Text, nullable=True)
    
    # Expected results (pre-commit estimates)
    expected_total_return = Column(Float, nullable=True)
    expected_sharpe_ratio = Column(Float, nullable=True)
    expected_max_drawdown = Column(Float, nullable=True)
    
    # Confidence in hypothesis
    hypothesis_confidence = Column(Float, nullable=True)  # 0-1
    
    # Results
    actual_total_return = Column(Float, nullable=True)
    actual_sharpe_ratio = Column(Float, nullable=True)
    actual_max_drawdown = Column(Float, nullable=True)
    
    # Outcome
    hypothesis_validated = Column(Boolean, nullable=False, default=False)  # True if performance > expectations
    
    # Experiment status
    status = Column(String(32), nullable=False, default="running")  # "running" | "completed" | "aborted"
    
    created_at = Column(DateTime, default=lambda: datetime.now())


# Helper function for creating research engine with database integration
def create_research_engine(db_session=None):
    """Create research engine with database-backed storage.
    
    Args:
        db_session: SQLAlchemy database session
        
    Returns:
        HypothesisGenerator configured with database storage
    """
    from research.hypothesis_generator import HypothesisGenerator
    
    return HypothesisGenerator()
