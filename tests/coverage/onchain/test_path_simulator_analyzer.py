import unittest
from types import SimpleNamespace

from onchain.models import (
    ActionType,
    ExecutionRoute,
    SimulationResult,
    SafetyState,
)
from onchain.simulation.path_simulator.analyzer import PathAnalyzer
from onchain.simulation.call_static.harness import CallStaticHarness


class TokenSafetyStub:
    def __init__(self, risk=0.1):
        self.risk = risk

    def classify(self, chain, token):
        return SafetyState.WATCHED, self.risk


class ContractSafetyStub:
    def __init__(self, risk=0.1):
        self.risk = risk

    def risk_score(self, chain, address):
        return self.risk


class ContractsStub:
    def __init__(self, allowed=True):
        self.allowed = allowed

    def is_allowed(self, chain, address, selector=None):
        return self.allowed


class SimStub:
    def __init__(self, success=True):
        self.success = success

    def simulate(self, route, amount_in, min_out, modeled_out):
        return SimulationResult(success=self.success, simulation_hash="h", gas_used=0,
                                min_out_respected=self.success, revert_reason=None if self.success else "X")


def risk_engine():
    return SimpleNamespace(live_drawdown_pct=0.0, policy=SimpleNamespace(drawdown_halt_pct=1.0))


def make_route(action=ActionType.SWAP, contracts=("0xc1",), tokens=("T1", "T2"), sim_success=True):
    return (
        ExecutionRoute(action_type=action, chain="eth", protocol="uni",
                       contracts_touched=list(contracts), tokens_touched=list(tokens)),
        sim_success,
    )


def make_analyzer(token_risk=0.1, contract_risk=0.1, allowed=True, sim_success=True, **kw):
    return PathAnalyzer(
        contracts=ContractsStub(allowed),
        token_safety=TokenSafetyStub(token_risk),
        contract_safety=ContractSafetyStub(contract_risk),
        simulation=SimStub(sim_success),
        risk_engine=risk_engine(),
        **kw,
    )


class TestPathAnalyzer(unittest.TestCase):
    def test_executable(self):
        route, _ = make_route()
        a = make_analyzer()
        plan = a.analyze("opp1", "strat", route, "0xw", 100.0, 5.0, 5.0, 1000.0)
        self.assertTrue(plan.executable)
        self.assertEqual(plan.fail_reasons, [])

    def test_bridge_settlement(self):
        route, _ = make_route(action=ActionType.BRIDGE_TRANSFER)
        a = make_analyzer()
        plan = a.analyze("opp1", "strat", route, "0xw", 100.0, 5.0, 5.0, 1000.0)
        self.assertEqual(plan.diagnostics.bridge_settlement_risk, 0.35)

    def test_disallowed_contract(self):
        route, _ = make_route()
        a = make_analyzer(allowed=False)
        plan = a.analyze("opp1", "strat", route, "0xw", 100.0, 5.0, 5.0, 1000.0)
        self.assertIn("contract_not_allowed:0xc1", plan.fail_reasons)
        self.assertFalse(plan.executable)

    def test_low_net_edge(self):
        route, _ = make_route()
        a = make_analyzer()
        plan = a.analyze("opp1", "strat", route, "0xw", -100.0, 5.0, 5.0, 1000.0)
        self.assertIn("net_edge_below_threshold", plan.fail_reasons)

    def test_low_trust_and_revert(self):
        # token_risk=0.95 & contract_risk=0.95 -> path_trust ~ 0.05 -> below min and revert>max
        route, _ = make_route()
        a = make_analyzer(token_risk=0.95, contract_risk=0.95)
        plan = a.analyze("opp1", "strat", route, "0xw", 100.0, 5.0, 5.0, 1000.0)
        self.assertIn("path_trust_below_threshold", plan.fail_reasons)
        self.assertIn("revert_risk_too_high", plan.fail_reasons)

    def test_simulation_failed(self):
        route, _ = make_route(sim_success=False)
        a = make_analyzer(sim_success=False)
        plan = a.analyze("opp1", "strat", route, "0xw", 100.0, 5.0, 5.0, 1000.0)
        self.assertIn("simulation_failed", plan.fail_reasons)

    def test_missing_components(self):
        route = ExecutionRoute(action_type=ActionType.SWAP, chain="eth", protocol="uni",
                               contracts_touched=[], tokens_touched=[])
        a = make_analyzer()
        plan = a.analyze("opp1", "strat", route, "0xw", 100.0, 5.0, 5.0, 1000.0)
        self.assertIn("missing_route_components", plan.fail_reasons)
        self.assertEqual(plan.diagnostics.token_risk_score, 0.5)
        self.assertEqual(plan.diagnostics.contract_risk_score, 0.5)

    def test_default_thresholds(self):
        a = make_analyzer()
        self.assertEqual(a.min_path_trust, 0.65)
        self.assertEqual(a.max_revert_risk, 0.35)


if __name__ == "__main__":
    unittest.main()
