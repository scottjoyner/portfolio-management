from __future__ import annotations
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from .protocols import Direction, Opportunity, BracketSetup

PRIOR_ALPHA = 1.0
PRIOR_BETA = 1.0


@dataclass
class StrategyPosterior:
    name: str
    alpha: float = PRIOR_ALPHA
    beta: float = PRIOR_BETA
    trades: int = 0
    wins: int = 0

    @property
    def win_rate(self) -> float:
        return self.alpha / max(self.alpha + self.beta, 1e-9)

    @property
    def uncertainty(self) -> float:
        n = self.alpha + self.beta
        if n <= 2:
            return 1.0
        return math.sqrt(self.alpha * self.beta / (n * n * (n + 1)))

    @property
    def credible_lower(self) -> float:
        mean = self.win_rate
        se = self.uncertainty
        return max(0.0, mean - 1.645 * se)

    @property
    def weight(self) -> float:
        return self.credible_lower

    def update(self, won: bool):
        self.trades += 1
        if won:
            self.wins += 1
            self.alpha += 1.0
        else:
            self.beta += 1.0

    def merge(self, other: StrategyPosterior):
        self.alpha += other.alpha - PRIOR_ALPHA
        self.beta += other.beta - PRIOR_BETA
        self.trades += other.trades
        self.wins += other.wins


class BayesianSignalBlender:
    def __init__(self, prior_alpha: float = PRIOR_ALPHA, prior_beta: float = PRIOR_BETA,
                 decay_half_life: int = 50):
        self.prior_alpha = prior_alpha
        self.prior_beta = prior_beta
        self.decay_half_life = decay_half_life
        self._posteriors: Dict[str, StrategyPosterior] = {}
        self._history: List[Dict] = []
        self._regime_posteriors: Dict[str, StrategyPosterior] = {}

    def get_or_create(self, name: str) -> StrategyPosterior:
        if name not in self._posteriors:
            self._posteriors[name] = StrategyPosterior(
                name=name, alpha=self.prior_alpha, beta=self.prior_beta
            )
        return self._posteriors[name]

    def record(self, strategy: str, won: bool, r_multiple: float = 0.0,
               regime: str = "unknown"):
        post = self.get_or_create(strategy)
        post.update(won)
        self._history.append({
            "strategy": strategy,
            "won": won,
            "r": r_multiple,
            "regime": regime,
            "ts": time.time(),
        })
        reg_key = f"{strategy}__{regime}"
        if reg_key not in self._regime_posteriors:
            self._regime_posteriors[reg_key] = StrategyPosterior(
                name=reg_key, alpha=self.prior_alpha, beta=self.prior_beta
            )
        self._regime_posteriors[reg_key].update(won)
        if len(self._history) > 10000:
            self._history = self._history[-5000:]

    def weight(self, strategy: str, regime: str = "unknown") -> float:
        base = self._posteriors.get(strategy)
        if base is None or base.trades < 1:
            return 1.0
        base_w = base.weight
        reg_key = f"{strategy}__{regime}"
        regime_post = self._regime_posteriors.get(reg_key)
        if regime_post and regime_post.trades >= 3:
            regime_w = regime_post.weight
            blend_n = min(base.trades, 50) / 50.0
            return base_w * (1 - blend_n) + regime_w * blend_n
        return base_w

    def blend_signals(self, opportunities: List[Opportunity],
                      regime: str = "unknown") -> List[Opportunity]:
        blended = []
        for opp in opportunities:
            w = self.weight(opp.strategy_name, regime)
            opp.score = opp.score * w
            opp.meta["bayesian_weight"] = round(w, 3)
            opp.meta["bayesian_win_rate"] = round(
                self._posteriors.get(opp.strategy_name, StrategyPosterior(name="")).win_rate, 3
            )
            blended.append(opp)
        blended.sort(key=lambda o: o.score, reverse=True)
        return blended

    def top_strategies(self, regime: str = "unknown", n: int = 5) -> List[Dict]:
        scores = []
        for name, post in self._posteriors.items():
            if post.trades < 3:
                continue
            w = self.weight(name, regime)
            scores.append({
                "name": name,
                "weight": round(w, 3),
                "win_rate": round(post.win_rate, 3),
                "trades": post.trades,
                "uncertainty": round(post.uncertainty, 3),
            })
        scores.sort(key=lambda x: x["weight"], reverse=True)
        return scores[:n]

    def strategy_universe(self, regime: str = "unknown",
                          min_weight: float = 0.3) -> List[str]:
        return [
            s["name"] for s in self.top_strategies(regime, 50)
            if s["weight"] >= min_weight
        ]

    def to_dict(self) -> Dict:
        return {
            name: {
                "win_rate": round(p.win_rate, 3),
                "trades": p.trades,
                "wins": p.wins,
                "uncertainty": round(p.uncertainty, 3),
                "weight_credible_lower": round(p.weight, 3),
            }
            for name, p in self._posteriors.items()
        }


class StrategyConfidenceAggregator:
    def __init__(self, blender: Optional[BayesianSignalBlender] = None):
        self.blender = blender or BayesianSignalBlender()

    def aggregate(self, opportunities: List[Opportunity],
                  regime: str = "unknown") -> List[Opportunity]:
        if not opportunities:
            return []

        by_direction: Dict[str, List[Opportunity]] = {"long": [], "short": []}
        for opp in opportunities:
            by_direction.setdefault(opp.direction.value, []).append(opp)

        results = []
        for direction, opps in by_direction.items():
            blended = self.blender.blend_signals(opps, regime)
            if not blended:
                continue

            total_weight = sum(o.score for o in blended)
            if total_weight <= 0:
                continue

            direction_agreement = len([o for o in blended if o.direction.value == direction]) / max(len(blended), 1)

            best = blended[0]
            best.meta["direction_agreement"] = round(direction_agreement, 3)
            best.meta["strategy_count"] = len(blended)
            best.meta["ensemble_score"] = round(
                best.score * (1 + direction_agreement * 0.2), 3
            )
            best.score = best.meta["ensemble_score"]
            results.append(best)

        results.sort(key=lambda o: o.score, reverse=True)
        return results
