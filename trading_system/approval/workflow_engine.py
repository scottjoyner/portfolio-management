"""Approval routing workflow engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional


class ApprovalTier(Enum):
    """Multi-tier approval levels for strategies and trades."""

    AUTO_APPROVE = "auto"
    CANARY_PHASE = "canary"
    FULL_SCALE = "production"


class AwaitableApprovalResult(dict[str, Any]):
    """Mapping result compatible with both sync and legacy await callers."""

    def __await__(self):
        async def _resolve() -> AwaitableApprovalResult:
            return self

        return _resolve().__await__()


@dataclass
class ApprovalRequest:
    """Approval request model for strategies and trades."""

    strategy_key: str
    version: str
    risk_level: float
    capital_allocation: float
    target_performance: float
    max_drawdown_tolerance: float = 5.0

    def __post_init__(self) -> None:
        self.strategy_key = str(self.strategy_key).strip()
        self.version = str(self.version).strip()
        if not self.strategy_key:
            raise ValueError("strategy_key is required")
        if not self.version:
            raise ValueError("version is required")
        if not 0 <= float(self.risk_level) <= 1:
            raise ValueError("risk_level must be between 0 and 1")
        if float(self.capital_allocation) < 0:
            raise ValueError("capital_allocation cannot be negative")
        if float(self.max_drawdown_tolerance) < 0:
            raise ValueError("max_drawdown_tolerance cannot be negative")

    def requires_approval(self, tier: ApprovalTier) -> bool:
        """Return whether a human decision is required."""
        needs_review = (
            self.risk_level > 0.3
            or self.capital_allocation > 10_000
        )
        return needs_review or tier != ApprovalTier.AUTO_APPROVE

    def get_required_tier(self) -> ApprovalTier:
        """Determine the minimum approval tier from risk."""
        if self.risk_level < 0.2:
            return ApprovalTier.AUTO_APPROVE
        if self.risk_level < 0.4:
            return ApprovalTier.CANARY_PHASE
        return ApprovalTier.FULL_SCALE


class WorkflowEngine:
    """Risk-based approval router with explicit validation gates."""

    REQUIRED_VALIDATIONS = (
        "code_review_passed",
        "security_scan_passed",
        "performance_benchmark_met",
    )

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.risk_threshold_canary = float(
            self.config.get("risk_threshold_canary", 0.4)
        )
        self.risk_threshold_production = float(
            self.config.get("risk_threshold_production", 0.6)
        )
        self.auto_approve_capital_limit = float(
            self.config.get("auto_approve_capital_limit", 5000)
        )

        if not 0 < self.risk_threshold_canary < 1:
            raise ValueError("risk_threshold_canary must be in (0, 1)")
        if not (
            self.risk_threshold_canary
            < self.risk_threshold_production
            <= 1
        ):
            raise ValueError(
                "risk_threshold_production must exceed canary threshold"
            )
        if self.auto_approve_capital_limit <= 0:
            raise ValueError(
                "auto_approve_capital_limit must be positive"
            )

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()

    def route_strategy(
        self,
        strategy_request: ApprovalRequest,
        validation_results: Optional[Dict[str, Any]] = None,
    ) -> AwaitableApprovalResult:
        """Route a strategy through validation and risk-based approval."""
        if not isinstance(strategy_request, ApprovalRequest):
            raise TypeError("strategy_request must be an ApprovalRequest")

        validation_results = validation_results or {}
        failed_validations = [
            name
            for name in self.REQUIRED_VALIDATIONS
            if name in validation_results
            and validation_results[name] is not True
        ]
        if failed_validations:
            return AwaitableApprovalResult(
                {
                    "status": "rejected",
                    "tier": ApprovalTier.FULL_SCALE.value,
                    "requires_human_approval": True,
                    "audit_trail_id": (
                        f"rejected-{strategy_request.strategy_key}"
                    ),
                    "approval_date": None,
                    "reviewers": ["risk_team", "security_team"],
                    "rejection_reasons": [
                        f"validation_failed:{name}"
                        for name in failed_validations
                    ],
                }
            )

        risk_level = float(strategy_request.risk_level)
        capital = float(strategy_request.capital_allocation)

        if (
            risk_level < self.risk_threshold_canary
            and capital <= self.auto_approve_capital_limit
        ):
            return AwaitableApprovalResult(
                {
                    "status": "approved",
                    "tier": ApprovalTier.AUTO_APPROVE.value,
                    "requires_human_approval": False,
                    "audit_trail_id": (
                        f"auto-{strategy_request.strategy_key}"
                    ),
                    "approval_date": self._timestamp(),
                    "reviewers": [],
                    "rejection_reasons": None,
                }
            )

        if risk_level >= self.risk_threshold_production:
            return AwaitableApprovalResult(
                {
                    "status": "pending_review",
                    "tier": ApprovalTier.FULL_SCALE.value,
                    "requires_human_approval": True,
                    "audit_trail_id": (
                        f"full-{strategy_request.strategy_key}"
                    ),
                    "approval_date": None,
                    "reviewers": ["risk_team", "compliance"],
                    "rejection_reasons": None,
                }
            )

        return AwaitableApprovalResult(
            {
                "status": "canary_approved",
                "tier": ApprovalTier.CANARY_PHASE.value,
                "requires_human_approval": False,
                "audit_trail_id": (
                    f"canary-{strategy_request.strategy_key}"
                ),
                "approval_date": self._timestamp(),
                "reviewers": ["risk_team"],
                "rejection_reasons": None,
            }
        )
