import unittest
from datetime import datetime
from decimal import Decimal

from pydantic import ValidationError

from onchain.dex.clmm.schemas import (
    HedgeMode, TokenRef, PoolRef, CLMMRange, CLMMPositionSnapshot, LPInventoryState,
    LPFeeState, ILDecomposition, HedgePlan, HedgeExecutionLink, RouteStep, RouteGraph,
    ActionSimulationRequest, RouteFallbackPlan, ActionSimulationResult, ActionProfitabilityReport,
    LPRebalanceDecision, ApprovalPacketOnchainMM, ProfitSweepDecision, PoolVolatilitySnapshot,
    PoolLiquiditySnapshot, HybridExposureSnapshot, StressScenarioInput, StressScenarioResult,
    RangeSelectionInput, RangeCandidate,
)


def token_ref(addr="0x1234"):
    return TokenRef(chain="eth", symbol="ETH", address=addr, decimals=18)


def pool_ref():
    return PoolRef(chain="eth", protocol="uni", pool_address="0xpool",
                   token0=token_ref("0xaaaa"), token1=token_ref("0xbbbb"),
                   fee_tier_bps=30, tick_spacing=10)


def route_graph():
    return RouteGraph(route_id="r1", steps=[RouteStep(venue="uni", contract="0xc", method="swap",
                                                      token_in="T1", token_out="T2",
                                                      amount_in=Decimal("1"), min_amount_out=Decimal("0.9"))],
                      quote_age_ms=100)


class TestCLMMSchemas(unittest.TestCase):
    def test_token_ref(self):
        t = token_ref()
        self.assertEqual(t.symbol, "ETH")

    def test_pool_ref(self):
        p = pool_ref()
        self.assertEqual(p.tick_spacing, 10)

    def test_clmm_range(self):
        r = CLMMRange(lower_tick=0, upper_tick=10)
        self.assertEqual(r.upper_tick, 10)

    def test_clmm_range_invalid(self):
        with self.assertRaises(ValidationError):
            CLMMRange(lower_tick=10, upper_tick=5)

    def test_position_snapshot(self):
        s = CLMMPositionSnapshot(position_id="p", pool=pool_ref(), owner_wallet="0xw",
                                 range=CLMMRange(0, 10), amount0=Decimal("1"), amount1=Decimal("2"),
                                 opened_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 2),
                                 mark_price=Decimal("2000"))
        self.assertEqual(s.position_id, "p")

    def test_inventory_state(self):
        s = LPInventoryState(token0_units=Decimal("1"), token1_units=Decimal("2"),
                             token0_usd=Decimal("2000"), token1_usd=Decimal("2"),
                             total_usd=Decimal("2002"), imbalance_ratio=Decimal("0.001"))
        self.assertEqual(s.total_usd, Decimal("2002"))

    def test_fee_state(self):
        s = LPFeeState(fee0_realized=Decimal("1"), fee1_realized=Decimal("2"),
                      fee_usd_realized=Decimal("3"), fee_usd_unrealized=Decimal("4"))
        self.assertEqual(s.collection_count, 0)

    def test_il_decomp(self):
        s = ILDecomposition(hodl_value_usd=Decimal("1"), lp_terminal_value_usd=Decimal("2"),
                            il_usd=Decimal("3"), fee_income_usd=Decimal("4"),
                            net_lp_edge_usd=Decimal("5"), fee_to_il_ratio=Decimal("6"))
        self.assertEqual(s.fee_to_il_ratio, Decimal("6"))

    def test_hedge_plan(self):
        s = HedgePlan(hedge_mode=HedgeMode.DELTA_NEUTRAL, base_asset="ETH", quote_asset="USD",
                      target_delta_usd=Decimal("1"), hedge_notional_usd=Decimal("1"),
                      max_slippage_bps=Decimal("15"), urgency_score=Decimal("0.5"), cooldown_seconds=30)
        self.assertEqual(s.hedge_mode, HedgeMode.DELTA_NEUTRAL)

    def test_hedge_exec_link(self):
        s = HedgeExecutionLink(opportunity_id="o", onchain_action_id="a", hedge_order_preview_id="h",
                               sequencing="ONCHAIN_FIRST", semi_atomic_score=Decimal("0.5"))
        self.assertEqual(s.sequencing, "ONCHAIN_FIRST")

    def test_route_step(self):
        s = RouteStep(venue="uni", contract="0xc", method="swap", token_in="T1", token_out="T2",
                      amount_in=Decimal("1"), min_amount_out=Decimal("0.9"))
        self.assertEqual(s.contract, "0xc")

    def test_route_graph(self):
        self.assertEqual(route_graph().route_id, "r1")

    def test_action_sim_request(self):
        s = ActionSimulationRequest(action_type="add_liquidity", wallet="0xw", pool=pool_ref(),
                                    route=route_graph(), amount_usd=Decimal("100"),
                                    slippage_bps_limit=Decimal("50"), deadline_ts=datetime(2024, 1, 1))
        self.assertEqual(s.amount_usd, Decimal("100"))

    def test_route_fallback(self):
        s = RouteFallbackPlan(reason="r", fallback_route=None, expected_net_delta_usd=Decimal("-5"))
        self.assertEqual(s.reason, "r")

    def test_action_sim_result(self):
        s = ActionSimulationResult(action_type="add", contracts_touched=[], tokens_touched=[],
                                   estimated_gas=0, gas_cost_usd=Decimal("0"), slippage_bps=Decimal("0"),
                                   price_impact_bps=Decimal("0"), route_fragility_score=Decimal("0"),
                                   pool_liquidity_quality=Decimal("0"), contract_trust_score=Decimal("0"),
                                   token_safety_score=Decimal("0"), expected_gross_value_change_usd=Decimal("0"),
                                   expected_net_value_change_usd=Decimal("0"), worst_case_downside_usd=Decimal("0"),
                                   break_even_gas_usd=Decimal("0"), break_even_slippage_bps=Decimal("0"),
                                   confidence_score=Decimal("0"))
        self.assertEqual(s.action_type, "add")

    def test_action_profit_report(self):
        res = ActionSimulationResult(action_type="add", contracts_touched=[], tokens_touched=[],
                                     estimated_gas=0, gas_cost_usd=Decimal("0"), slippage_bps=Decimal("0"),
                                     price_impact_bps=Decimal("0"), route_fragility_score=Decimal("0"),
                                     pool_liquidity_quality=Decimal("0"), contract_trust_score=Decimal("0"),
                                     token_safety_score=Decimal("0"), expected_gross_value_change_usd=Decimal("0"),
                                     expected_net_value_change_usd=Decimal("0"), worst_case_downside_usd=Decimal("0"),
                                     break_even_gas_usd=Decimal("0"), break_even_slippage_bps=Decimal("0"),
                                     confidence_score=Decimal("0"))
        s = ActionProfitabilityReport(opportunity_id="o", simulation=res, is_profitable=True)
        self.assertTrue(s.is_profitable)

    def test_rebalance_decision(self):
        s = LPRebalanceDecision(should_rebalance=True, reason="r", distance_to_boundary_bps=Decimal("1"),
                                expected_rebalance_cost_usd=Decimal("2"), expected_rebalance_edge_usd=Decimal("3"))
        self.assertTrue(s.should_rebalance)

    def test_approval_packet(self):
        s = ApprovalPacketOnchainMM(opportunity_id="o", strategy_name="s", action_type="add",
                                    protocol="uni", chain="eth", pool="0xp", wallet="0xw",
                                    current_position_summary="a", proposed_position_summary="b",
                                    contracts_touched=[], tokens_touched=[], approvals_required=[],
                                    expected_fee_capture=Decimal("1"), expected_gas_cost=Decimal("1"),
                                    expected_slippage=Decimal("1"), expected_net_benefit=Decimal("1"),
                                    worst_case_loss=Decimal("1"), hedge_required=False,
                                    route_trust_score=Decimal("0.5"), fragility_score=Decimal("0.5"),
                                    token_risk_score=Decimal("0.5"), contract_risk_score=Decimal("0.5"),
                                    expiration_time=datetime(2024, 1, 1), rollback_plan="rollback",
                                    concise_voice_summary="concise voice summary",
                                    detailed_operator_summary="detailed operator summary here")
        self.assertEqual(s.opportunity_id, "o")

    def test_profit_sweep_decision(self):
        s = ProfitSweepDecision(should_sweep=True, should_compound=False, sweep_amount_usd=Decimal("1"),
                                compound_amount_usd=Decimal("0"), destination_bucket="CASH", reason="r")
        self.assertTrue(s.should_sweep)

    def test_pool_vol_snapshot(self):
        s = PoolVolatilitySnapshot(realized_vol_1d=Decimal("1"), realized_vol_7d=Decimal("1"),
                                  short_horizon_vol=Decimal("1"), regime="CALM")
        self.assertEqual(s.regime, "CALM")

    def test_pool_liq_snapshot(self):
        s = PoolLiquiditySnapshot(depth_usd_50bps=Decimal("1"), depth_usd_200bps=Decimal("1"),
                                  daily_volume_usd=Decimal("1"), quality_score=Decimal("0.5"))
        self.assertEqual(s.quality_score, Decimal("0.5"))

    def test_hybrid_exposure(self):
        s = HybridExposureSnapshot(onchain_delta_usd=Decimal("1"), cex_delta_usd=Decimal("2"),
                                  net_delta_usd=Decimal("3"), hedge_coverage_ratio=Decimal("0.5"))
        self.assertEqual(s.net_delta_usd, Decimal("3"))

    def test_stress_input(self):
        s = StressScenarioInput(name="n", spot_shock_pct=Decimal("1"), vol_multiplier=Decimal("1"),
                                gas_multiplier=Decimal("1"), liquidity_haircut_pct=Decimal("1"))
        self.assertEqual(s.name, "n")

    def test_stress_result(self):
        s = StressScenarioResult(name="n", pnl_usd=Decimal("1"), max_drawdown_usd=Decimal("2"),
                                 time_out_of_range_pct=Decimal("3"), hedge_failures=0,
                                 fragility_score=Decimal("0.1"))
        self.assertEqual(s.pnl_usd, Decimal("1"))

    def test_range_selection_input(self):
        s = RangeSelectionInput(mark_price=Decimal("2000"), tick_spacing=10, realized_vol=Decimal("0.1"),
                                short_vol=Decimal("0.1"), market_regime="NORMAL", inventory_skew=Decimal("0"),
                                gas_regime_score=Decimal("0.1"))
        self.assertEqual(s.mark_price, Decimal("2000"))

    def test_range_candidate(self):
        s = RangeCandidate(lower_tick=0, upper_tick=10, width_bps=Decimal("100"),
                           expected_utilization=Decimal("0.5"), expected_fee_density=Decimal("1"),
                           expected_rebalance_pressure=Decimal("0"), expected_il_pressure=Decimal("0"),
                           expected_hedge_drag=Decimal("0"), confidence_score=Decimal("0.5"))
        self.assertGreater(s.upper_tick, s.lower_tick)

    def test_range_candidate_invalid(self):
        with self.assertRaises(ValidationError):
            RangeCandidate(lower_tick=10, upper_tick=5, width_bps=Decimal("100"),
                           expected_utilization=Decimal("0.5"), expected_fee_density=Decimal("1"),
                           expected_rebalance_pressure=Decimal("0"), expected_il_pressure=Decimal("0"),
                           expected_hedge_drag=Decimal("0"), confidence_score=Decimal("0.5"))


if __name__ == "__main__":
    unittest.main()
