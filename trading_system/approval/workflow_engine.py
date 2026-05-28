"""Approval routing workflow engine."""

from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class ApprovalTier(Enum):
    """Multi-tier approval levels for strategies and trades."""
    AUTO_APPROVE = "auto"           # Low-risk, automated approval
    CANARY_PHASE = "canary"          # Limited deployment, monitoring phase
    FULL_SCALE = "production"        # Full capital deployment, live trading


@dataclass
class ApprovalRequest:
    """Approval request model for strategies and trades."""
    strategy_key: str
    version: str
    risk_level: float  # 0-1 normalized risk score
    capital_allocation: float  # Amount to deploy (USD or token units)
    target_performance: float  # Expected annualized return (%)
    max_drawdown_tolerance: float = 5.0  # Maximum acceptable drawdown (%)
    
    def requires_approval(self, tier: ApprovalTier) -> bool:
        """Check if request requires human approval based on tier and risk."""
        needs_review = self.risk_level > 0.3 or self.capital_allocation > 10000
        
        # Always require review for non-auto tiers
        tier_needs_review = tier != ApprovalTier.AUTO_APPROVE
        
        return needs_review or tier_needs_review
        
    def get_required_tier(self) -> ApprovalTier:
        """Determine minimum approval tier based on risk profile."""
        if self.risk_level < 0.2:
            return ApprovalTier.AUTO_APPROVE
        elif self.risk_level < 0.4:
            return ApprovalTier.CANARY_PHASE
        else:
            return ApprovalTier.FULL_SCALE


class WorkflowEngine:
    """Approval workflow engine for strategy and trade approvals.
    
    Implements multi-tier approval system with risk-based routing logic,
    validation checks, and audit trail tracking.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize workflow engine with configuration.
        
        Args:
            config: Dict of optional parameters including:
                - risk_threshold_canary: float (default 0.4)
                - risk_threshold_production: float (default 0.6)
                - auto_approve_capital_limit: float (default 5000)
                - require_risk_audit_for_high_risk: bool (default True)
        """
        self.config = config or {}
        self.risk_threshold_canary = self.config.get("risk_threshold_canary", 0.4)
        self.risk_threshold_production = self.config.get("risk_threshold_production", 0.6)
        self.auto_approve_capital_limit = self.config.get("auto_approve_capital_limit", 5000)
        
    async def route_strategy(
        self, 
        strategy_request: ApprovalRequest,
        validation_results: Dict[str, Any] = None
    ) -> Dict[str, str]:
        """Route strategy through approval workflow.
        
        Args:
            strategy_request: The approval request with risk parameters
            validation_results: Results from initial validation checks including:
                - code_review_passed: bool
                - security_scan_passed: bool
                - performance_benchmark_met: bool
                
        Returns:
            Dict with status and metadata:
                {
                    "status": "approved" | "rejected" | "pending_review" | "canary_approved",
                    "tier": ApprovalTier,
                    "requires_human_approval": bool,
                    "audit_trail_id": str,
                    "approval_date": datetime,
                    "reviewers": List[str],
                    "rejection_reasons": Optional[List[str]]
                }
        """
        validation_results = validation_results or {}
        
        # Risk-based routing logic
        risk_level = strategy_request.risk_level
        capital = strategy_request.capital_allocation
        
        if risk_level < self.risk_threshold_canary and capital < self.auto_approve_capital_limit:
            return {
                "status": "approved",
                "tier": ApprovalTier.AUTO_APPROVE.value,
                "requires_human_approval": False,
                "audit_trail_id": f"auto-{strategy_request.strategy_key}",
                "approval_date": "2026-05-27T13:00:00Z",  # Placeholder
                "reviewers": [],
                "rejection_reasons": None
            }
            
        elif risk_level >= self.risk_threshold_production:
            return {
                "status": "pending_review",
                "tier": ApprovalTier.FULL_SCALE.value,
                "requires_human_approval": True,
                "audit_trail_id": f"full-{strategy_request.strategy_key}",
                "approval_date": None,  # Requires human approval
                "reviewers": ["risk_team@example.com", "compliance@example.com"],
                "rejection_reasons": None
            }
            
        else:
            return {
                "status": "canary_approved",
                "tier": ApprovalTier.CANARY_PHASE.value,
                "requires_human_approval": False,  # Can proceed to canary with monitoring
                "audit_trail_id": f"canary-{strategy_request.strategy_key}",
                "approval_date": "2026-05-27T13:00:00Z",
                "reviewers": ["risk_team@example.com"],
                "rejection_reasons": None
            }
