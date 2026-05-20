from time import perf_counter

from onchain.contracts.registry.service import ContractRegistry
from onchain.models import ActionType, ContractProfile, ExecutionRoute, RouteEdge, SafetyState, TokenProfile
from onchain.security.contract_safety.engine import ContractSafetyEngine
from onchain.security.token_safety.engine import TokenSafetyEngine
from onchain.simulation.call_static.harness import CallStaticHarness
from onchain.simulation.path_simulator.analyzer import PathAnalyzer
from risk.engine import RiskEngine, RiskPolicy

reg = ContractRegistry()
reg.register(ContractProfile(chain="base", address="0x1", protocol="uni", codehash="0xabc123456", verified_abi=True, safety_state=SafetyState.TRUSTED, risk_score=0.2))
tokens = TokenSafetyEngine()
tokens.register_token(TokenProfile(chain="base", address="0x2", symbol="WETH", decimals=18, risk_score=0.2, safety_state=SafetyState.TRUSTED))
pa = PathAnalyzer(reg, tokens, ContractSafetyEngine(reg), CallStaticHarness(), RiskEngine(RiskPolicy()))
route = ExecutionRoute(action_type=ActionType.SWAP, chain="base", protocol="uni", contracts_touched=["0x1"], tokens_touched=["0x2"], route_graph=[RouteEdge(protocol="uni", pool="p", token_in="WETH", token_out="USDC", liquidity_score=0.8)])

start = perf_counter()
for i in range(1000):
    pa.analyze(f"opp-{i}", "bench", route, "w", 10, 1, 1, 1000)
print({"iterations": 1000, "elapsed_s": round(perf_counter() - start, 4)})
