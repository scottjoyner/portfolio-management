from __future__ import annotations
import json
import math
import os
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple
from collections import deque

from .protocols import Direction, Bar, BracketSetup, BaseStrategy, Opportunity

RANKING_STATE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "ranking_state.json"
)


@dataclass
class StrategyStats:
    name: str
    trades: int = 0
    wins: int = 0
    total_pnl: float = 0.0
    win_rate: float = 0.0
    sharpe: float = 0.0
    avg_confidence: float = 0.0
    max_drawdown: float = 0.0
    peak_pnl: float = 0.0
    recent_returns: List[float] = field(default_factory=list)
    capital_weight: float = 1.0


class StrategyRanking:
    def __init__(self, top_n: int = 5, min_trades: int = 5,
                 lookback_max: int = 100, decay_half_life: int = 20,
                 rebalance_bars: int = 48):
        self.top_n = top_n
        self.min_trades = min_trades
        self.lookback_max = lookback_max
        self.decay_half_life = decay_half_life
        self.rebalance_bars = rebalance_bars
        self._bars_since_rebalance: int = 0

        self._stats: Dict[str, StrategyStats] = {}
        self._ranked: List[Tuple[str, float]] = []

    def record_trade(self, strategy_name: str, pnl: float, confidence: float):
        if strategy_name not in self._stats:
            self._stats[strategy_name] = StrategyStats(name=strategy_name)

        stat = self._stats[strategy_name]
        stat.trades += 1
        stat.avg_confidence = (stat.avg_confidence * (stat.trades - 1) + confidence) / stat.trades
        stat.recent_returns.append(pnl)
        if len(stat.recent_returns) > self.lookback_max:
            stat.recent_returns = stat.recent_returns[-self.lookback_max:]

        if pnl > 0:
            stat.wins += 1
        stat.total_pnl += pnl
        stat.win_rate = stat.wins / max(stat.trades, 1)

        if stat.total_pnl > stat.peak_pnl:
            stat.peak_pnl = stat.total_pnl
        dd = (stat.peak_pnl - stat.total_pnl) / max(abs(stat.peak_pnl), 0.01) if stat.peak_pnl != 0 else 0
        stat.max_drawdown = max(stat.max_drawdown, dd)

        returns = stat.recent_returns
        if len(returns) >= 5:
            mean_r = sum(returns) / len(returns)
            var = sum((r - mean_r) ** 2 for r in returns) / len(returns)
            std = math.sqrt(var) if var > 0 else 0.001
            rf = 0.0
            stat.sharpe = (mean_r - rf) / std * math.sqrt(252) if std > 0 else 0.0

    def rank_all(self) -> List[Tuple[str, float]]:
        scores = []
        for name, stat in self._stats.items():
            if stat.trades < self.min_trades:
                continue

            decay_factor = 2 ** (-len(stat.recent_returns) / max(self.decay_half_life, 1))
            recent = stat.recent_returns[-min(20, max(1, len(stat.recent_returns))):]
            recent_perf = sum(recent) / max(len(recent), 1) if recent else 0

            wr_score = (stat.win_rate - 0.5) * 50
            sharpe_score = max(-10, min(10, stat.sharpe)) * 10
            pnl_score = max(-20, min(20, stat.total_pnl * 10))
            recent_score = recent_perf * 50
            dd_penalty = max(0, stat.max_drawdown - 0.2) * -50
            conf_boost = (stat.avg_confidence - 0.3) * 20
            volume_bonus = min(10, math.log2(max(1, stat.trades)) * 3)

            score = (wr_score * 0.25 + sharpe_score * 0.2 +
                     pnl_score * 0.15 + recent_score * 0.2 +
                     dd_penalty * 0.1 + conf_boost * 0.05 +
                     volume_bonus * 0.05) * decay_factor

            scores.append((name, round(score, 3)))
            stat.capital_weight = max(0.0, score / 100 + 0.5)

        scores.sort(key=lambda x: x[1], reverse=True)
        self._ranked = scores[:self.top_n]
        return self._ranked

    def should_rebalance(self) -> bool:
        self._bars_since_rebalance += 1
        if self._bars_since_rebalance >= self.rebalance_bars:
            self._bars_since_rebalance = 0
            return True
        return False

    def rebalance_weights(self, total_weight: float = 1.0) -> Dict[str, float]:
        ranked = self.rank_all()
        if not ranked:
            return {}

        raw_weights = {}
        for name, score in ranked:
            raw_weights[name] = max(0.05, (score - ranked[-1][1] + 1) / max(ranked[0][1] - ranked[-1][1] + 1, 1))

        total = sum(raw_weights.values())
        if total <= 0:
            return {}

        normalized = {}
        for name, w in raw_weights.items():
            strats_for_name = [s for s in self._stats if s.startswith(name.split("_v")[0])]
            n_strats = max(len(strats_for_name), 1)
            normalized[name] = w / total * total_weight / n_strats

        return normalized

    def top_strategies(self) -> List[str]:
        return [s[0] for s in self._ranked]

    def to_dict(self) -> Dict:
        stats = {}
        for name, s in self._stats.items():
            stats[name] = {
                "name": s.name, "trades": s.trades, "wins": s.wins,
                "total_pnl": s.total_pnl, "win_rate": s.win_rate,
                "sharpe": s.sharpe, "avg_confidence": s.avg_confidence,
                "max_drawdown": s.max_drawdown, "peak_pnl": s.peak_pnl,
                "recent_returns": s.recent_returns,
                "capital_weight": s.capital_weight,
            }
        return {"stats": stats, "ranked": self._ranked}

    @classmethod
    def from_dict(cls, data: Dict) -> StrategyRanking:
        inst = cls()
        for name, sd in data.get("stats", {}).items():
            s = StrategyStats(**sd)
            inst._stats[name] = s
        inst._ranked = data.get("ranked", [])
        return inst

    def save(self, path: str = RANKING_STATE_PATH):
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                json.dump(self.to_dict(), f, indent=2)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("Failed to save ranking state: %s", e)

    def load(self, path: str = RANKING_STATE_PATH):
        try:
            if os.path.exists(path):
                with open(path) as f:
                    data = json.load(f)
                for name, sd in data.get("stats", {}).items():
                    s = StrategyStats(**sd)
                    self._stats[name] = s
                self._ranked = data.get("ranked", [])
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("Failed to load ranking state: %s", e)

    def summary(self) -> Dict:
        return {
            "ranked": [{"name": n, "score": s} for n, s in self._ranked],
            "total_tracked": len(self._stats),
            "min_trades_threshold": self.min_trades,
        }


class TopRankedStrategyWrapper(BaseStrategy):
    def __init__(self, inner: BaseStrategy, ranking: StrategyRanking):
        self._inner = inner
        self._ranking = ranking
        self._name = f"ranked_{inner.name()}"

    def name(self) -> str:
        return self._name

    def set_product_id(self, product_id: str):
        if hasattr(self._inner, 'set_product_id'):
            self._inner.set_product_id(product_id)

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        ranked_names = self._ranking.top_strategies()
        base_name = self._inner.name()
        if not ranked_names or base_name not in ranked_names:
            return None
        return self._inner.on_bar(bar, history)


class StrategyRankingFilter:
    def __init__(self, ranking: StrategyRanking):
        self._ranking = ranking

    def filter_opportunities(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        top = set(self._ranking.top_strategies())
        return [o for o in opportunities if o.strategy_name in top]

    def weight_opportunities(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        weights = self._ranking.rebalance_weights()
        for opp in opportunities:
            w = weights.get(opp.strategy_name, 0.0)
            opp.confidence = min(0.99, opp.confidence * (0.5 + w))
        return opportunities
