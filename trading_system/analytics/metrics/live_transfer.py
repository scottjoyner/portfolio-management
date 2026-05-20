from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SimulationAssumptions:
    latency_ms: float
    queue_fill_probability: float
    stale_quote_decay: float
    maker_ratio: float
    cancel_ratio: float
    rejection_rate: float
    outage_rate: float


@dataclass(frozen=True)
class RealismPenaltyBreakdown:
    latency_penalty: float
    fill_optimism_penalty: float
    stale_quote_penalty: float
    turnover_penalty: float
    rejection_penalty: float
    outage_penalty: float

    @property
    def total(self) -> float:
        return max(0.0, min(1.0, self.latency_penalty + self.fill_optimism_penalty + self.stale_quote_penalty + self.turnover_penalty + self.rejection_penalty + self.outage_penalty))


@dataclass(frozen=True)
class LiveTransferAssessment:
    strategy_id: str
    fragility_score: float
    live_transfer_confidence: float
    expected_live_return: float
    breakdown: RealismPenaltyBreakdown


class BacktestRealismScorer:
    """Penalizes optimistic simulation assumptions and estimates portability to live."""

    @staticmethod
    def realism_penalty(a: SimulationAssumptions) -> RealismPenaltyBreakdown:
        latency_penalty = min(0.2, max(0.0, (40.0 - a.latency_ms) / 400.0))
        fill_optimism_penalty = min(0.25, max(0.0, (a.queue_fill_probability - 0.55) * 0.5))
        stale_quote_penalty = min(0.2, max(0.0, (0.3 - a.stale_quote_decay) * 0.6))
        turnover_penalty = min(0.15, max(0.0, (a.cancel_ratio - 0.65) * 0.5))
        rejection_penalty = min(0.1, a.rejection_rate * 1.5)
        outage_penalty = min(0.1, a.outage_rate * 2.0)
        return RealismPenaltyBreakdown(
            latency_penalty=latency_penalty,
            fill_optimism_penalty=fill_optimism_penalty,
            stale_quote_penalty=stale_quote_penalty,
            turnover_penalty=turnover_penalty,
            rejection_penalty=rejection_penalty,
            outage_penalty=outage_penalty,
        )

    @staticmethod
    def assess_strategy(
        strategy_id: str,
        simulated_return: float,
        sharpe: float,
        assumptions: SimulationAssumptions,
        holding_horizon_hint: str = "intraday",
    ) -> LiveTransferAssessment:
        penalties = BacktestRealismScorer.realism_penalty(assumptions)
        horizon_penalty = 0.08 if "sub" in holding_horizon_hint.lower() else 0.03 if "intraday" in holding_horizon_hint.lower() else 0.01
        fragility = max(0.0, min(1.0, penalties.total + horizon_penalty + max(0.0, (2.0 - sharpe) * 0.03)))
        confidence = max(0.0, min(1.0, 1.0 - fragility))
        expected_live = simulated_return * (1.0 - penalties.total) * (0.85 + 0.15 * confidence)
        return LiveTransferAssessment(
            strategy_id=strategy_id,
            fragility_score=fragility,
            live_transfer_confidence=confidence,
            expected_live_return=expected_live,
            breakdown=penalties,
        )
