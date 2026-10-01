"""Tests for the approval workflow engine."""

import asyncio

import pytest

from approval.workflow_engine import (
    ApprovalRequest,
    ApprovalTier,
    WorkflowEngine,
)


# Every required check passed. route_strategy refuses when a required check is
# missing, so approval paths must say so explicitly rather than relying on the
# absence of a result being treated as a pass.
VALIDATED = {
    "code_review_passed": True,
    "security_scan_passed": True,
    "performance_benchmark_met": True,
}


def test_approval_request_creation():
    low_risk = ApprovalRequest(
        strategy_key="ema_crossover_v1",
        version="1.0.0",
        risk_level=0.2,
        capital_allocation=5000,
        target_performance=8.0,
    )
    assert not low_risk.requires_approval(ApprovalTier.AUTO_APPROVE)


def test_approval_request_high_risk():
    high_risk = ApprovalRequest(
        strategy_key="momentum_strategy_v1",
        version="1.0.0",
        risk_level=0.65,
        capital_allocation=50_000,
        target_performance=15.0,
    )
    assert high_risk.requires_approval(ApprovalTier.AUTO_APPROVE)


def test_workflow_engine_auto_approve():
    engine = WorkflowEngine(
        {
            "risk_threshold_canary": 0.3,
            "risk_threshold_production": 0.6,
            "auto_approve_capital_limit": 10_000,
        }
    )
    request = ApprovalRequest(
        strategy_key="ema_crossover_v1",
        version="1.2.0",
        risk_level=0.2,
        capital_allocation=5000,
        target_performance=8.5,
    )

    result = engine.route_strategy(request, VALIDATED)
    assert result["status"] == "approved"
    assert result["tier"] == "auto"
    assert not result["requires_human_approval"]


def test_workflow_engine_remains_await_compatible():
    engine = WorkflowEngine()
    result = engine.route_strategy(
        ApprovalRequest(
            strategy_key="ema_crossover_v1",
            version="1.2.0",
            risk_level=0.2,
            capital_allocation=1000,
            target_performance=8.5,
        )
    )

    async def resolve():
        return await result

    assert asyncio.run(resolve()) is result


def test_workflow_engine_canary_approve():
    engine = WorkflowEngine()
    request = ApprovalRequest(
        strategy_key="momentum_strategy_v1",
        version="1.0.0",
        risk_level=0.4,
        capital_allocation=25_000,
        target_performance=12.0,
    )

    result = engine.route_strategy(request, VALIDATED)
    assert result["status"] == "canary_approved"
    assert result["tier"] == "canary"
    assert not result["requires_human_approval"]


def test_workflow_engine_production_approve():
    engine = WorkflowEngine(
        {
            "risk_threshold_canary": 0.4,
            "risk_threshold_production": 0.6,
            "auto_approve_capital_limit": 10_000,
        }
    )
    request = ApprovalRequest(
        strategy_key="trend_breakout_v1",
        version="1.0.0",
        risk_level=0.75,
        capital_allocation=75_000,
        target_performance=20.0,
    )

    result = engine.route_strategy(request, VALIDATED)
    assert result["status"] == "pending_review"
    assert result["tier"] == "production"
    assert result["requires_human_approval"]


def test_failed_validation_rejects_before_risk_routing():
    engine = WorkflowEngine()
    request = ApprovalRequest(
        strategy_key="ema_crossover_v1",
        version="1.0.0",
        risk_level=0.1,
        capital_allocation=100,
        target_performance=5.0,
    )

    result = engine.route_strategy(
        request,
        {**VALIDATED, "security_scan_passed": False},
    )
    assert result["status"] == "rejected"
    assert result["requires_human_approval"]
    assert result["rejection_reasons"] == [
        "validation_failed:security_scan_passed"
    ]


def test_missing_validation_is_not_treated_as_a_pass():
    """A required check that was never run has not passed.

    route_strategy used to skip absent keys entirely, so calling it with no
    validations at all auto-approved a strategy that had no code review and no
    security scan.
    """
    engine = WorkflowEngine()
    request = ApprovalRequest(
        strategy_key="ema_crossover_v1",
        version="1.0.0",
        risk_level=0.1,
        capital_allocation=100,
        target_performance=5.0,
    )

    result = engine.route_strategy(request)
    assert result["status"] == "rejected"
    assert result["rejection_reasons"] == [
        "validation_failed:code_review_passed",
        "validation_failed:security_scan_passed",
        "validation_failed:performance_benchmark_met",
    ]

    partial = engine.route_strategy(request, {"code_review_passed": True})
    assert partial["status"] == "rejected"


def test_invalid_request_fails_closed():
    with pytest.raises(ValueError, match="risk_level"):
        ApprovalRequest(
            strategy_key="bad",
            version="1",
            risk_level=1.5,
            capital_allocation=100,
            target_performance=5.0,
        )
