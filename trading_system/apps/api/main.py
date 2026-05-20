from fastapi import FastAPI, HTTPException

from core.config.settings import Settings
from core.models.domain import ExchangeTrustScore, OrderIntent, RiskMode
from exchange.coinbase.reconciliation.service import ExchangeStateReconciler
from onchain.contracts.registry.service import ContractRegistry
from onchain.models import ActionType, ContractProfile, ExecutionRoute, Opportunity, RouteEdge, SafetyState, TokenProfile
from onchain.security.approval_gates.approval_packet import ApprovalPacketBuilder
from onchain.security.contract_safety.engine import ContractSafetyEngine
from onchain.security.token_safety.engine import TokenSafetyEngine
from onchain.simulation.call_static.harness import CallStaticHarness
from onchain.simulation.path_simulator.analyzer import PathAnalyzer
from onchain.strategies.execution.opportunity_ranker import OpportunityRanker
from onchain.strategies.hedging.hybrid_hedge import HybridHedgeLinker
from onchain.strategies.treasury.profit_sweep import ProfitCaptureEngine
from risk.engine import RiskEngine, RiskPolicy
from strategies.registry.registry import load_strategies
from apps.api.ops_layer import router as ops_router

app = FastAPI(title="Trading System Control API")
settings = Settings.from_env()
risk_engine = RiskEngine(RiskPolicy())
reconciler = ExchangeStateReconciler()

contract_registry = ContractRegistry()
token_safety = TokenSafetyEngine()
contract_safety = ContractSafetyEngine(registry=contract_registry)
simulator = CallStaticHarness()
path_analyzer = PathAnalyzer(contract_registry, token_safety, contract_safety, simulator, risk_engine)
approval_builder = ApprovalPacketBuilder()
opportunity_ranker = OpportunityRanker()
hedge_linker = HybridHedgeLinker()
profit_capture = ProfitCaptureEngine()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/mode")
def mode() -> dict:
    return {"mode": settings.trading_mode}


@app.get("/strategies/catalog")
def strategy_catalog() -> dict:
    strategies = load_strategies()
    return {"count": len(strategies), "strategies": [s.metadata() for s in strategies]}


@app.post("/risk/mode/{mode}/enable")
def enable_risk_mode(mode: RiskMode) -> dict:
    risk_engine.enable_mode(mode)
    return {"enabled_modes": sorted(m.value for m in risk_engine.enabled_modes)}


@app.get("/reconciliation/trust-score")
def reconciliation_trust_score() -> dict:
    trust = reconciler.reconcile_open_orders(reconciler.snapshot.open_orders_remote)
    risk_engine.set_exchange_trust(trust)
    return {"trust_score": trust.value, "snapshot": reconciler.snapshot.__dict__}


@app.post("/risk/evaluate")
def evaluate(intent: OrderIntent, mark_price: float = 0.0) -> dict:
    allowed, reason = risk_engine.evaluate(intent, mark_price=mark_price)
    if not allowed:
        raise HTTPException(status_code=400, detail=reason)
    return {"allowed": allowed, "reason": reason, "exchange_trust": risk_engine.exchange_trust_score.value}


@app.post("/ops/unsafe/untrusted")
def set_untrusted() -> dict:
    risk_engine.set_exchange_trust(ExchangeTrustScore.UNTRUSTED)
    return {"ok": True}


@app.post("/onchain/contracts/register")
def register_contract(profile: ContractProfile) -> dict:
    contract_registry.register(profile)
    return {"ok": True, "key": f"{profile.chain}:{profile.address.lower()}"}


@app.post("/onchain/tokens/register")
def register_token(profile: TokenProfile) -> dict:
    token_safety.register_token(profile)
    return {"ok": True, "key": f"{profile.chain}:{profile.address.lower()}"}


@app.post("/onchain/opportunities/rank")
def rank_opportunities(opportunities: list[Opportunity]) -> dict:
    ranked = opportunity_ranker.rank(opportunities)
    return {"opportunities": [s.model_dump() for s in ranked]}


@app.post("/onchain/path/analyze")
def analyze_path(route: ExecutionRoute, expected_gross_edge: float, expected_gas_cost: float, expected_slippage_cost: float, capital_at_risk: float) -> dict:
    plan = path_analyzer.analyze(
        opportunity_id="adhoc",
        strategy_name="manual",
        route=route,
        wallet="hot-wallet",
        expected_gross_edge=expected_gross_edge,
        expected_gas_cost=expected_gas_cost,
        expected_slippage_cost=expected_slippage_cost,
        capital_at_risk=capital_at_risk,
    )
    return plan.model_dump()


@app.post("/onchain/approvals/build")
def build_approval(route: ExecutionRoute, expected_gross_edge: float, expected_gas_cost: float, expected_slippage_cost: float, capital_at_risk: float) -> dict:
    plan = path_analyzer.analyze(
        opportunity_id="adhoc",
        strategy_name="manual",
        route=route,
        wallet="hot-wallet",
        expected_gross_edge=expected_gross_edge,
        expected_gas_cost=expected_gas_cost,
        expected_slippage_cost=expected_slippage_cost,
        capital_at_risk=capital_at_risk,
    )
    payload = approval_builder.build(plan, wallet="hot-wallet", reason="onchain_action")
    return payload.model_dump()


@app.post("/onchain/hedge/coinbase")
def plan_hedge(symbol: str, delta: float, orderbook_depth: float, latency_ms: int = 80) -> dict:
    return hedge_linker.plan_coinbase_hedge(symbol, delta, orderbook_depth, latency_ms).__dict__


@app.post("/onchain/profit/sweep")
def sweep(realized_pnl: float) -> dict:
    return {k.value: v for k, v in profit_capture.record_realized(realized_pnl).items()}


@app.get("/onchain/bootstrap/base")
def bootstrap_base_examples() -> dict:
    contract_registry.register(
        ContractProfile(
            chain="base",
            address="0x1111111111111111111111111111111111111111",
            protocol="uniswap_v3",
            codehash="0xabcd1234ef00",
            verified_abi=True,
            selectors_allowlist={"0x414bf389"},
            safety_state=SafetyState.TRUSTED,
            risk_score=0.25,
        )
    )
    token_safety.register_token(
        TokenProfile(chain="base", address="0x4200000000000000000000000000000000000006", symbol="WETH", decimals=18, risk_score=0.2, safety_state=SafetyState.TRUSTED)
    )
    example_route = ExecutionRoute(
        action_type=ActionType.SWAP,
        chain="base",
        protocol="uniswap_v3",
        contracts_touched=["0x1111111111111111111111111111111111111111"],
        tokens_touched=["0x4200000000000000000000000000000000000006"],
        approvals_required=["WETH->router"],
        route_graph=[RouteEdge(protocol="uniswap_v3", pool="pool-weth-usdc", token_in="WETH", token_out="USDC", fee_bps=5, liquidity_score=0.85)],
        fallback_routes=["aerodrome_v2"],
    )
    plan = path_analyzer.analyze(
        opportunity_id="bootstrap-base-001",
        strategy_name="cex_dex_arb",
        route=example_route,
        wallet="hot-wallet",
        expected_gross_edge=23.0,
        expected_gas_cost=1.8,
        expected_slippage_cost=2.2,
        capital_at_risk=2_500,
    )
    return {"plan": plan.model_dump()}


app.include_router(ops_router)
