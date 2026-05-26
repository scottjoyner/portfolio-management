from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RiskApproval:
    action: str
    requested_by: str
    approved_by: str | None = None
    approved: bool = False
    reason: str = ""


@dataclass
class RiskApprovalService:
    required_actions: set[str] = field(default_factory=lambda: {"enable_aggressive", "enable_expert", "enable_hft", "increase_capital_limit"})
    pending: list[RiskApproval] = field(default_factory=list)

    def requires_approval(self, action: str) -> bool:
        return action in self.required_actions

    def request(self, action: str, requested_by: str) -> RiskApproval:
        approval = RiskApproval(action=action, requested_by=requested_by)
        self.pending.append(approval)
        return approval

    def approve(self, approval: RiskApproval, approved_by: str) -> None:
        approval.approved = True
        approval.approved_by = approved_by

    def reject(self, approval: RiskApproval, reason: str) -> None:
        approval.approved = False
        approval.reason = reason
