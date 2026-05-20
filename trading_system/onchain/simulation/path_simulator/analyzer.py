from __future__ import annotations

from dataclasses import dataclass

from onchain.contracts.registry.service import ContractRegistry
from onchain.models import ExecutionPlan, ExecutionRoute, RouteDiagnostics, RouteEconomics, RiskDiagnostics
from onchain.security.contract_safety.engine import ContractSafetyEngine
from onchain.security.token_safety.engine import TokenSafetyEngine
from onchain.simulation.call_static.harness import CallStaticHarness
from risk.engine import RiskEngine


@dataclass
class PathAnalyzer:
    contracts: ContractRegistry
    token_safety: TokenSafetyEngine
    contract_safety: ContractSafetyEngine
    simulation: CallStaticHarness
    risk_engine: RiskEngine
    min_path_trust: float = 0.65
    max_revert_risk: float = 0.35
    min_net_edge: float = 1.0

    def analyze(
        self,
        opportunity_id: str,
        strategy_name: str,
        route: ExecutionRoute,
        wallet: str,
        expected_gross_edge: float,
        expected_gas_cost: float,
        expected_slippage_cost: float,
        capital_at_risk: float,
        hedge_cost: float = 0.0,
        bridge_cost: float = 0.0,
    ) -> ExecutionPlan:
        token_risks = [self.token_safety.classify(route.chain, t)[1] for t in route.tokens_touched] or [0.5]
        contract_risks = [self.contract_safety.risk_score(route.chain, c) for c in route.contracts_touched] or [0.5]
        path_trust = max(0.0, 1.0 - (sum(token_risks) / len(token_risks) + sum(contract_risks) / len(contract_risks)) / 2)
        revert_probability = min(0.95, 0.1 + (1 - path_trust) * 0.6)

        economics = RouteEconomics(
            expected_gross_edge=expected_gross_edge,
            expected_gas_cost=expected_gas_cost,
            expected_priority_fee=expected_gas_cost * 0.2,
            expected_slippage_cost=expected_slippage_cost,
            expected_price_impact=expected_slippage_cost,
            expected_lp_fee_capture=0.0,
            expected_hedge_cost=hedge_cost,
            expected_bridge_cost=bridge_cost,
            expected_net_edge=expected_gross_edge - expected_gas_cost - expected_slippage_cost - hedge_cost - bridge_cost,
            worst_case_downside=capital_at_risk * (0.02 + revert_probability * 0.05),
            break_even_slippage_bps=max(1.0, expected_gross_edge / max(capital_at_risk, 1.0) * 10_000),
            break_even_gas=max(0.0, expected_gross_edge - expected_slippage_cost),
        )
        diagnostics = RouteDiagnostics(
            path_trust_score=path_trust,
            pool_liquidity_quality=min(1.0, 0.5 + len(route.route_graph) * 0.1),
            route_fragility=max(0.0, 1.0 - min(1.0, len(route.fallback_routes) * 0.25 + path_trust * 0.5)),
            token_risk_score=sum(token_risks) / len(token_risks),
            contract_risk_score=sum(contract_risks) / len(contract_risks),
            oracle_freshness_seconds=12,
            quote_staleness_ms=250,
            fill_confidence=max(0.0, path_trust - 0.1),
            mev_reorder_risk=max(0.0, 0.4 - path_trust * 0.3),
            bridge_settlement_risk=0.0 if route.action_type.value != "bridge_transfer" else 0.35,
            revert_probability=revert_probability,
        )
        risk = RiskDiagnostics(
            capital_at_risk=capital_at_risk,
            inventory_impact={t: capital_at_risk / max(len(route.tokens_touched), 1) for t in route.tokens_touched},
            exposure_delta={route.protocol: capital_at_risk},
            strategy_cap_ok=capital_at_risk <= 100_000,
            sleeve_cap_ok=capital_at_risk <= 200_000,
            reserve_lock_ok=True,
            drawdown_mode_ok=self.risk_engine.live_drawdown_pct < self.risk_engine.policy.drawdown_halt_pct,
            unwind_plan="swap back via fallback route then flatten hedge on Coinbase",
        )
        simulation = self.simulation.simulate(route=route, amount_in=capital_at_risk, min_out=capital_at_risk * 0.98, modeled_out=capital_at_risk * 0.985)

        fail_reasons: list[str] = []
        for contract in route.contracts_touched:
            if not self.contracts.is_allowed(route.chain, contract):
                fail_reasons.append(f"contract_not_allowed:{contract}")
        if economics.expected_net_edge < self.min_net_edge:
            fail_reasons.append("net_edge_below_threshold")
        if diagnostics.path_trust_score < self.min_path_trust:
            fail_reasons.append("path_trust_below_threshold")
        if diagnostics.revert_probability > self.max_revert_risk:
            fail_reasons.append("revert_risk_too_high")
        if not simulation.success:
            fail_reasons.append("simulation_failed")
        if not route.tokens_touched or not route.contracts_touched:
            fail_reasons.append("missing_route_components")

        return ExecutionPlan(
            opportunity_id=opportunity_id,
            strategy_name=strategy_name,
            route=route,
            economics=economics,
            diagnostics=diagnostics,
            risk=risk,
            simulation=simulation,
            executable=not fail_reasons,
            fail_reasons=fail_reasons,
        )
