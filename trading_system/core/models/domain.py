from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from time import monotonic_ns
from pydantic import BaseModel, Field


class CapitalBucketType(str, Enum):
    LOCKED_RESERVE = "LOCKED_RESERVE"
    ACTIVE_TRADING = "ACTIVE_TRADING"
    MARKET_MAKING = "MARKET_MAKING"
    ACCUMULATION = "ACCUMULATION"
    HEDGING = "HEDGING"
    CASH_BUFFER = "CASH_BUFFER"


class RiskMode(str, Enum):
    ULTRA_CONSERVATIVE = "ULTRA_CONSERVATIVE"
    NORMAL = "NORMAL"
    AGGRESSIVE = "AGGRESSIVE"
    EXPERT_HIGH_RISK = "EXPERT_HIGH_RISK"
    LAB_HFT = "LAB_HFT"
    MARKET_MAKING_PRO = "MARKET_MAKING_PRO"
    DERIVATIVES_EXPERT = "DERIVATIVES_EXPERT"
    RESEARCH_ONLY = "RESEARCH_ONLY"


class ExchangeTrustScore(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNTRUSTED = "UNTRUSTED"


class CapitalBucket(BaseModel):
    name: str
    bucket_type: CapitalBucketType
    target_weight: float = Field(ge=0, le=1)
    min_weight: float = Field(ge=0, le=1, default=0)
    max_weight: float = Field(ge=0, le=1, default=1)
    locked: bool = False


class OrderIntent(BaseModel):
    strategy_id: str
    product_id: str
    side: str
    order_type: str
    size: float
    price: float | None = None
    bucket: CapitalBucketType = CapitalBucketType.ACTIVE_TRADING
    rationale: str
    risk_mode: RiskMode = RiskMode.NORMAL
    reduce_only: bool = False


class StructuredApprovalPayload(BaseModel):
    reason: str
    urgency: str
    expected_upside: float
    modeled_worst_case_downside: float
    capital_affected: float
    strategy_confidence: float = Field(ge=0, le=1)
    regime: str
    liquidity_state: str
    exchange_trust_score: ExchangeTrustScore
    rollback_plan: str


@dataclass(slots=True)
class LatencyTrace:
    feed_received_ns: int = 0
    normalize_done_ns: int = 0
    feature_done_ns: int = 0
    strategy_done_ns: int = 0
    risk_done_ns: int = 0
    submit_done_ns: int = 0
    ack_done_ns: int = 0
    fill_done_ns: int = 0

    @staticmethod
    def now_ns() -> int:
        return monotonic_ns()

    def as_us(self) -> dict[str, float]:
        def delta(a: int, b: int) -> float:
            return max(b - a, 0) / 1_000.0 if a and b else 0.0

        return {
            "feed_receive_latency_us": delta(self.feed_received_ns, self.normalize_done_ns),
            "normalization_latency_us": delta(self.normalize_done_ns, self.feature_done_ns),
            "feature_latency_us": delta(self.feature_done_ns, self.strategy_done_ns),
            "strategy_decision_latency_us": delta(self.strategy_done_ns, self.risk_done_ns),
            "risk_approval_latency_us": delta(self.risk_done_ns, self.submit_done_ns),
            "order_submit_latency_us": delta(self.submit_done_ns, self.ack_done_ns),
            "exchange_ack_latency_us": delta(self.ack_done_ns, self.fill_done_ns),
        }
