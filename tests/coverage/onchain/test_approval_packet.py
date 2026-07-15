from __future__ import annotations

from unittest import TestCase

from decimal import Decimal

from onchain.models import (
    ActionType,
    ExecutionPlan,
    ExecutionRoute,
    RouteDiagnostics,
    RouteEconomics,
    RiskDiagnostics,
    SafetyState,
    SimulationResult,
)
from onchain.security.approval_gates.approval_packet import ApprovalPacketBuilder


def _make_plan() -> ExecutionPlan:
    return ExecutionPlan(
        opportunity_id="opp-1",
        strategy_name="arb",
        route=ExecutionRoute(
            action_type=ActionType.SWAP,
            chain="base",
            protocol="uniswap",
            contracts_touched=["0xC0NTRACT"],
            tokens_touched=["0xTOKEN"],
            approvals_required=["0xAPPR"],
        ),
        economics=RouteEconomics(
            expected_gross_edge=10.0,
            expected_gas_cost=1.0,
            expected_priority_fee=0.5,
            expected_slippage_cost=0.2,
            expected_price_impact=0.1,
            expected_net_edge=8.3,
            worst_case_downside=2.0,
            break_even_slippage_bps=5.0,
            break_even_gas=1.0,
        ),
        diagnostics=RouteDiagnostics(
            path_trust_score=0.9,
            pool_liquidity_quality=0.8,
            route_fragility=0.1,
            token_risk_score=0.2,
            contract_risk_score=0.3,
            oracle_freshness_seconds=10,
            quote_staleness_ms=100,
            mev_reorder_risk=0.1,
            bridge_settlement_risk=0.0,
            revert_probability=0.05,
            fill_confidence=0.9,
        ),
        risk=RiskDiagnostics(
            capital_at_risk=1000.0,
            inventory_impact={},
            exposure_delta={},
            strategy_cap_ok=True,
            sleeve_cap_ok=True,
            reserve_lock_ok=True,
            drawdown_mode_ok=True,
            unwind_plan="reverse",
        ),
        simulation=SimulationResult(
            success=True,
            simulation_hash="0xHASH",
            gas_used=21000,
            min_out_respected=True,
        ),
        executable=True,
    )


class TestApprovalPacketBuilder(TestCase):
    def test_build_default_urgency(self):
        plan = _make_plan()
        payload = ApprovalPacketBuilder().build(plan, "0xWALLET", "do it")
        self.assertEqual(payload.opportunity_id, "opp-1")
        self.assertEqual(payload.wallet, "0xWALLET")
        self.assertEqual(payload.urgency, "normal")
        self.assertEqual(payload.action_type, ActionType.SWAP)
        self.assertIn("uniswap", payload.concise_summary)
        self.assertIn("opp-1", payload.detailed_summary)
        self.assertIn("approve", payload.operator_actions_available)

    def test_build_custom_urgency(self):
        plan = _make_plan()
        payload = ApprovalPacketBuilder().build(plan, "0xWALLET", "reason", urgency="high")
        self.assertEqual(payload.urgency, "high")
        self.assertEqual(payload.expected_net_pnl, 8.3)
        self.assertEqual(payload.contract_risk_score, 0.3)
        self.assertEqual(payload.rollback_plan, "reverse")
        self.assertEqual(payload.simulation_hash, "0xHASH")
