from __future__ import annotations

from onchain.models import Opportunity, OpportunityScore


class OpportunityRanker:
    def rank(self, opportunities: list[Opportunity], min_net_edge: float = 1.0) -> list[OpportunityScore]:
        scores: list[OpportunityScore] = []
        for opp in opportunities:
            decay_penalty = min(opp.gross_edge * 0.5, opp.age_ms / 1_000)
            modeled_cost = opp.capital_required * 0.0008
            net = opp.gross_edge - modeled_cost - decay_penalty
            trust = max(0.0, opp.confidence - opp.age_ms / 20_000)
            executable = net >= min_net_edge and trust >= 0.5
            scores.append(
                OpportunityScore(
                    opportunity=opp,
                    estimated_net_edge=net,
                    route_trust_score=trust,
                    fragility=max(0.0, 1 - trust),
                    executable=executable,
                    reject_reason=None if executable else "insufficient_net_or_trust",
                )
            )
        return sorted(scores, key=lambda s: s.estimated_net_edge, reverse=True)
