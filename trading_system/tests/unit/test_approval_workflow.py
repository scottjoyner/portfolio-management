"""Tests for approval workflow engine."""

import pytest


def test_approval_request_creation():
    """Test ApprovalRequest creation with various risk levels."""
    from approval.workflow_engine import (
        ApprovalTier, 
        ApprovalRequest
    )
    
    # Test low-risk request
    low_risk = ApprovalRequest(
        strategy_key="ema_crossover_v1",
        version="1.0.0",
        risk_level=0.2,  # Low risk
        capital_allocation=5000,
        target_performance=8.0
    )
    assert low_risk.requires_approval(ApprovalTier.AUTO_APPROVE) == False


def test_approval_request_high_risk():
    """Test high-risk request requires human approval."""
    from approval.workflow_engine import (
        ApprovalTier, 
        ApprovalRequest
    )
    
    # Test high-risk request
    high_risk = ApprovalRequest(
        strategy_key="momentum_strategy_v1",
        version="1.0.0",
        risk_level=0.65,  # High risk
        capital_allocation=50000,
        target_performance=15.0
    )
    assert high_risk.requires_approval(ApprovalTier.AUTO_APPROVE) == True


def test_workflow_engine_auto_approve():
    """Test workflow engine auto-approval for low-risk strategies."""
    from approval.workflow_engine import WorkflowEngine
    
    engine = WorkflowEngine(config={
        "risk_threshold_canary": 0.3,
        "risk_threshold_production": 0.6,
        "auto_approve_capital_limit": 10000,
    })
    
    # Create low-risk request
    request = ApprovalRequest(
        strategy_key="ema_crossover_v1",
        version="1.2.0",
        risk_level=0.2,
        capital_allocation=5000,
        target_performance=8.5
    )
    
    result = engine.route_strategy(request)
    assert result["status"] == "approved"
    assert result["tier"] == "auto"
    assert result["requires_human_approval"] == False


def test_workflow_engine_canary_approve():
    """Test workflow engine canary approval for medium-risk strategies."""
    from approval.workflow_engine import WorkflowEngine
    
    engine = WorkflowEngine()
    
    # Create medium-risk request
    request = ApprovalRequest(
        strategy_key="momentum_strategy_v1",
        version="1.0.0",
        risk_level=0.4,  # Medium-high risk
        capital_allocation=25000,
        target_performance=12.0
    )
    
    result = engine.route_strategy(request)
    assert result["status"] == "canary_approved"
    assert result["tier"] == "canary"
    assert result["requires_human_approval"] == False


def test_workflow_engine_production_approve():
    """Test workflow engine production approval for high-risk strategies."""
    from approval.workflow_engine import WorkflowEngine
    
    engine = WorkflowEngine(config={
        "risk_threshold_canary": 0.4,
        "risk_threshold_production": 0.6,
        "auto_approve_capital_limit": 10000,
    })
    
    # Create high-risk request
    request = ApprovalRequest(
        strategy_key="trend_breakout_v1",
        version="1.0.0",
        risk_level=0.75,  # High risk
        capital_allocation=75000,
        target_performance=20.0
    )
    
    result = engine.route_strategy(request)
    assert result["status"] == "pending_review"
    assert result["tier"] == "production"
    assert result["requires_human_approval"] == True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
