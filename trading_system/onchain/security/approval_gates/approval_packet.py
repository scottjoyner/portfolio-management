from __future__ import annotations

from datetime import datetime, timedelta, timezone

from onchain.models import ExecutionPlan, OnchainApprovalPayload


class ApprovalPacketBuilder:
    def build(self, plan: ExecutionPlan, wallet: str, reason: str, urgency: str = "normal") -> OnchainApprovalPayload:
        expires = datetime.now(timezone.utc) + timedelta(minutes=5)
        concise = (
            f"{plan.route.action_type.value} on {plan.route.protocol} {plan.route.chain}; "
            f"net {plan.economics.expected_net_edge:.2f}, gas {plan.economics.expected_gas_cost:.2f}, "
            f"slippage {plan.economics.expected_slippage_cost:.2f}, trust {plan.diagnostics.path_trust_score:.2f}."
        )
        detailed = (
            f"Opportunity {plan.opportunity_id} via {plan.strategy_name}: contracts={plan.route.contracts_touched}, "
            f"tokens={plan.route.tokens_touched}, approvals={plan.route.approvals_required}, "
            f"gross={plan.economics.expected_gross_edge:.2f}, net={plan.economics.expected_net_edge:.2f}, "
            f"worst_case={plan.economics.worst_case_downside:.2f}, revert_risk={plan.diagnostics.revert_probability:.2f}, "
            f"rollback={plan.risk.unwind_plan}."
        )
        return OnchainApprovalPayload(
            opportunity_id=plan.opportunity_id,
            strategy_name=plan.strategy_name,
            action_type=plan.route.action_type,
            chain=plan.route.chain,
            protocol=plan.route.protocol,
            wallet=wallet,
            contracts_touched=plan.route.contracts_touched,
            tokens_touched=plan.route.tokens_touched,
            approvals_required=plan.route.approvals_required,
            expected_gross_pnl=plan.economics.expected_gross_edge,
            expected_net_pnl=plan.economics.expected_net_edge,
            gas_estimate=plan.economics.expected_gas_cost,
            slippage_estimate=plan.economics.expected_slippage_cost,
            worst_case_loss=plan.economics.worst_case_downside,
            capital_at_risk=plan.risk.capital_at_risk,
            hedge_plan="coinbase_delta_hedge_if_filled",
            route_trust_score=plan.diagnostics.path_trust_score,
            token_risk_score=plan.diagnostics.token_risk_score,
            contract_risk_score=plan.diagnostics.contract_risk_score,
            revert_risk=plan.diagnostics.revert_probability,
            expiration_time=expires.isoformat(),
            simulation_hash=plan.simulation.simulation_hash,
            rollback_plan=plan.risk.unwind_plan,
            reason=reason,
            urgency=urgency,
            operator_actions_available=["approve", "reject", "approve_once", "quarantine_contract"],
            concise_summary=concise,
            detailed_summary=detailed,
        )
