from onchain.contracts.registry.service import ContractRegistry
from onchain.models import ActionType, ContractProfile, ExecutionRoute, RouteEdge, SafetyState, TokenProfile
from onchain.security.approval_gates.approval_packet import ApprovalPacketBuilder
from onchain.security.contract_safety.engine import ContractSafetyEngine
from onchain.security.token_safety.engine import TokenSafetyEngine
from onchain.simulation.call_static.harness import CallStaticHarness
from onchain.simulation.path_simulator.analyzer import PathAnalyzer
from risk.engine import RiskEngine, RiskPolicy


def _setup() -> tuple[PathAnalyzer, ExecutionRoute]:
    reg = ContractRegistry()
    reg.register(ContractProfile(chain="base", address="0x1", protocol="uni", codehash="0xabcdef123456", verified_abi=True, safety_state=SafetyState.TRUSTED, risk_score=0.2))
    tokens = TokenSafetyEngine()
    tokens.register_token(TokenProfile(chain="base", address="0x2", symbol="WETH", decimals=18, safety_state=SafetyState.TRUSTED, risk_score=0.2))
    analyzer = PathAnalyzer(reg, tokens, ContractSafetyEngine(reg), CallStaticHarness(), RiskEngine(RiskPolicy()))
    route = ExecutionRoute(
        action_type=ActionType.SWAP,
        chain="base",
        protocol="uniswap_v3",
        contracts_touched=["0x1"],
        tokens_touched=["0x2"],
        approvals_required=["WETH->router"],
        route_graph=[RouteEdge(protocol="uniswap_v3", pool="p", token_in="WETH", token_out="USDC", liquidity_score=0.9)],
        fallback_routes=["aerodrome"],
    )
    return analyzer, route


def test_path_analyzer_success_and_approval_packet() -> None:
    analyzer, route = _setup()
    plan = analyzer.analyze("o1", "cex_dex_arb", route, "hot", 20, 1, 1, 1_000)
    assert plan.executable
    payload = ApprovalPacketBuilder().build(plan, wallet="hot", reason="edge_detected")
    assert payload.expected_net_pnl > 0
    assert "net" in payload.concise_summary


def test_path_analyzer_fails_for_missing_contract_allowlist() -> None:
    analyzer, route = _setup()
    route.contracts_touched = ["0x999"]
    plan = analyzer.analyze("o2", "cex_dex_arb", route, "hot", 20, 1, 1, 1_000)
    assert not plan.executable
    assert any(r.startswith("contract_not_allowed") for r in plan.fail_reasons)
