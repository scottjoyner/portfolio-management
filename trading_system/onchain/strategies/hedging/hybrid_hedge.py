from __future__ import annotations

from dataclasses import dataclass

from onchain.models import AtomicityClass


@dataclass
class HedgePlan:
    venue: str
    product_id: str
    side: str
    size: float
    estimated_slippage: float
    confidence: float
    atomicity: AtomicityClass
    failover_plan: str


class HybridHedgeLinker:
    def plan_coinbase_hedge(self, symbol: str, delta: float, orderbook_depth: float, latency_ms: int) -> HedgePlan:
        side = "SELL" if delta > 0 else "BUY"
        size = abs(delta)
        slip = size / max(orderbook_depth, 1.0) * 10
        confidence = max(0.0, 1.0 - latency_ms / 1_000 - slip / 100)
        if confidence > 0.85:
            atomicity = AtomicityClass.EFFECTIVELY_ATOMIC
        elif confidence > 0.7:
            atomicity = AtomicityClass.SEMI_ATOMIC
        elif confidence > 0.5:
            atomicity = AtomicityClass.SEQUENTIAL
        else:
            atomicity = AtomicityClass.UNSAFE_SEQUENTIAL
        return HedgePlan(
            venue="coinbase",
            product_id=symbol,
            side=side,
            size=size,
            estimated_slippage=slip,
            confidence=confidence,
            atomicity=atomicity,
            failover_plan="reduce onchain exposure and re-quote hedge immediately",
        )
