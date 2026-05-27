"""Database models for evaluation and pricing."""

from sqlalchemy import Column, Integer, Float, String, DateTime, ForeignKey, Text, Index
from sqlalchemy.orm import relationship
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class PriceEstimationModel(Base):
    """Price estimation results model."""
    
    __tablename__ = 'price_estimates'
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(50), nullable=False)  # e.g., "ETH", "BTC"
    current_price = Column(Float, nullable=True)
    buy_level = Column(Float, nullable=True)
    sell_level = Column(Float, nullable=True)
    hold_level = Column(Float, nullable=True)
    confidence_score = Column(Float, nullable=True)
    model_used = Column(String(50), nullable=True)  # e.g., "fundamental", "technical"
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relations
    instrument = relationship("InstrumentModel", back_populates="price_estimates")
    
    __table_args__ = (
        Index('ix_price_symbol', 'symbol'),
        Index('ix_price_confidence', 'confidence_score'),
    )


class PositionQualityMetrics(Base):
    """Position quality scoring model."""
    
    __tablename__ = 'position_quality_metrics'
    
    id = Column(Integer, primary_key=True, index=True)
    position_id = Column(String(50), nullable=False, index=True)
    
    # Risk metrics
    risk_score = Column(Float, nullable=True)  # 0-1 normalized
    alpha_score = Column(Float, nullable=True)  # Expected excess return (bps)
    beta_exposure = Column(Float, nullable=True)  # Market sensitivity
    
    # Quality metrics
    correlation_to_index = Column(Float, nullable=True)
    volatility_regime = Column(String(50), nullable=True)  # "low" | "moderate" | "high" | "extreme"
    
    # Additional quality factors
    liquidity_score = Column(Float, nullable=True)  # 0-1 normalized
    market_impact_estimate = Column(Float, nullable=True)  # Expected slippage (bps)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PriceTargetModel(Enum):
    """Price target models for fair-value estimation."""
    FUNDAMENTAL_BASED = "fundamental"
    TECHNICAL_ANALYSIS = "technical"
    CONSENSUS_AVERAGE = "consensus"
    ML_PREDICTIVE = "ml_predictive"


class EvaluationConfiguration(Base):
    """Configuration for price estimation engine."""
    
    __tablename__ = 'evaluation_config'
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Price source configuration
    price_source = Column(String(50), nullable=False, default="fundamental")
    use_ml_models = Column(Boolean, nullable=False, default=False)
    
    # Model thresholds
    volatility_threshold_high = Column(Float, nullable=False, default=0.4)
    volatility_threshold_extreme = Column(Float, nullable=False, default=0.6)
    
    # Algorithm configuration
    correlation_window_size = Column(Integer, nullable=False, default=252)  # Days
    cointegration_threshold = Column(Float, nullable=False, default=0.95)
    
    created_at = Column(DateTime, default=datetime.utcnow)


class EvaluationModel:
    """Evaluation model factory."""
    
    @staticmethod
    def create_engine(config_dict: dict = None):
        """Create price estimation engine with database-backed config."""
        from evaluation.pricing_models import PriceEstimationEngine
        
        config = {
            "price_source": config_dict.get("price_source", "fundamental") if config_dict else "fundamental",
            "use_ml_models": config_dict.get("use_ml_models", False) if config_dict else False,
            "volatility_threshold_high": config_dict.get("volatility_threshold_high", 0.4) if config_dict else 0.4,
            "volatility_threshold_extreme": config_dict.get("volatility_threshold_extreme", 0.6) if config_dict else 0.6,
        }
        
        return PriceEstimationEngine(config)
