from core.models.domain import CapitalBucketType
from onchain.models import AtomicityClass, Opportunity, ActionType
from onchain.strategies.execution.opportunity_ranker import OpportunityRanker
from onchain.strategies.hedging.hybrid_hedge import HybridHedgeLinker
from onchain.strategies.treasury.profit_sweep import ProfitCaptureEngine


def test_hybrid_hedge_atomicity() -> None:
    plan = HybridHedgeLinker().plan_coinbase_hedge("ETH-USD", delta=2.0, orderbook_depth=500.0, latency_ms=50)
    assert plan.atomicity in {AtomicityClass.EFFECTIVELY_ATOMIC, AtomicityClass.SEMI_ATOMIC}


def test_profit_capture_sweep_distribution() -> None:
    engine = ProfitCaptureEngine()
    buckets = engine.record_realized(100.0)
    assert buckets[CapitalBucketType.LOCKED_RESERVE] == 60.0
    assert buckets[CapitalBucketType.CASH_BUFFER] == 20.0


def test_opportunity_ranker_rejects_stale_low_trust() -> None:
    opp = Opportunity(opportunity_id="1", strategy_name="tri_arb", chain="base", protocol="x", action_type=ActionType.SWAP, token_pair="WETH/USDC", gross_edge=5.0, capital_required=2000, confidence=0.55, age_ms=8_000)
    ranked = OpportunityRanker().rank([opp])
    assert not ranked[0].executable
