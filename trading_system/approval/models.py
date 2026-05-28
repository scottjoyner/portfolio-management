"""Database models for approval workflow engine."""

from sqlalchemy import Column, Integer, Float, String, DateTime, Text, Boolean
from sqlalchemy.orm import relationship
from datetime import datetime, UTC

# SQLAlchemy Base class would be defined in app/models.py or similar


class ApprovalRequest(Base):
    """Approval request model for strategies and trades."""
    
    __tablename__ = 'approval_requests'
    
    id = Column(Integer, primary_key=True, index=True)
    strategy_key = Column(String(256), nullable=False, index=True)
    version = Column(String(32), nullable=False)  # e.g., "1.0.0", "1.1.0"
    
    # Risk parameters
    risk_level = Column(Float, nullable=False)  # 0-1 normalized
    capital_allocation = Column(Float, nullable=False)  # Amount to deploy (USD or token units)
    target_performance = Column(Float, nullable=False)  # Expected annualized return (%)
    
    # Tolerance settings
    max_drawdown_tolerance = Column(Float, nullable=True, default=5.0)  # Maximum acceptable drawdown (%)
    
    # Status tracking
    status = Column(String(32), nullable=False, index=True, default="pending")  # "pending" | "approved" | "rejected" | "canary_approved"
    
    tier = Column(String(32), nullable=True)  # "auto" | "canary" | "production"
    requires_human_approval = Column(Boolean, nullable=False, default=False)
    
    # Timestamps
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    reviewed_at = Column(DateTime, nullable=True)
    approval_date = Column(DateTime, nullable=True)
    rejected_at = Column(DateTime, nullable=True)
    
    # Metadata
    review_comments = Column(Text, nullable=True)  # Human review comments
    rejection_reasons = Column(Text, nullable=True)  # Rejection reason if denied
    
    # Relations
    strategy_metadata = relationship("StrategyMetadata", back_populates="approval_requests")
    audit_trails = relationship("AuditTrailModel", back_populates="approval_request")


class ApprovalStatus(Enum):
    """Approval status enumeration."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    CANARY_APPROVED = "canary_approved"
    FULL_SCALE_APPROVED = "full_scale_approved"


class AuditTrailModel(Base):
    """Audit trail for approval decisions."""
    
    __tablename__ = 'audit_trails'
    
    id = Column(Integer, primary_key=True, index=True)
    strategy_key = Column(String(256), nullable=False, index=True)
    audit_trail_id = Column(String(128), nullable=False, index=True)
    
    # Approval information
    status = Column(String(32), nullable=False)  # "approved" | "rejected" | "pending_review"
    tier = Column(String(32), nullable=True)  # "auto" | "canary" | "production"
    
    requires_human_approval = Column(Boolean, nullable=False, default=False)
    
    approval_date = Column(DateTime, nullable=True)
    reviewed_by = Column(String(128), nullable=True)  # Reviewer identifier (user or service)
    reviewer_email = Column(String(256), nullable=True)
    
    review_comments = Column(Text, nullable=True)
    rejection_reasons = Column(Text, nullable=True)
    
    # Validation results
    risk_assessment_score = Column(Float, nullable=True)
    code_review_passed = Column(Boolean, nullable=True)
    security_scan_passed = Column(Boolean, nullable=True)
    performance_benchmark_met = Column(Boolean, nullable=True)
    
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))


class RiskAssessmentModel(Base):
    """Risk assessment results for strategy evaluation."""
    
    __tablename__ = 'risk_assessments'
    
    id = Column(Integer, primary_key=True, index=True)
    strategy_key = Column(String(256), nullable=False, index=True)
    
    # Risk metrics
    overall_risk_score = Column(Float, nullable=False)  # 0-1 normalized
    market_risk_score = Column(Float, nullable=True)
    liquidity_risk_score = Column(Float, nullable=True)
    operational_risk_score = Column(Float, nullable=True)
    
    # Risk categories
    high_risk_factors = Column(Text, nullable=True)  # JSON array of risk factors
    
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))


class CapacityTrackingModel(Base):
    """Approval capacity tracking."""
    
    __tablename__ = 'approval_capacities'
    
    id = Column(Integer, primary_key=True, index=True)
    strategy_key = Column(String(256), nullable=False)
    
    # Allocation tracking
    total_approved_capital = Column(Float, default=0)
    pending_capital = Column(Float, default=0)
    monthly_limit = Column(Float, nullable=True)  # Monthly capital allocation limit
    
    # Capacity status
    is_at_capacity = Column(Boolean, nullable=False, default=False)
    
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))


# Helper function for creating engine with database models
def create_evaluation_engine(db_session=None):
    """Create evaluation engine with database integration.
    
    Args:
        db_session: SQLAlchemy database session
        
    Returns:
        PriceEstimationEngine configured with database-backed configuration
    """
    from evaluation.pricing_models import PriceEstimationEngine
    
    # Load config from database if provided
    config = {}
    if db_session:
        eval_config = db_session.query(EvaluationConfiguration).first()
        if eval_config:
            config = {
                "price_source": eval_config.price_source,
                "use_ml_models": eval_config.use_ml_models,
                "volatility_threshold_high": eval_config.volatility_threshold_high,
                "volatility_threshold_extreme": eval_config.volatility_threshold_extreme,
            }
    
    return PriceEstimationEngine(config)
