"""Confidence Matrix — multi-strategy signal aggregation.

Groups signals from all 10 strategies by asset and direction,
then computes an aggregate confidence score using:
  1. Strategy independence (same-family strategies don't double-count)
  2. Historical backtest performance per strategy-asset pair
  3. Asset-class-specific strategy weighting

Output: a single aggregated signal per (asset, direction) with
a boosted or penalized confidence score.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from strategy_engine import Signal as StrategySignal

logger = logging.getLogger("confidence_matrix")

# Strategy independence groups: strategies in the same group use
# mathematically similar logic and should not count as independent signals
INDEPENDENCE_GROUPS: Dict[str, Set[str]] = {
    "trend": {"ema_cross", "macd", "trix", "adx", "psar", "hma", "aroon"},
    "momentum": {"rsi_revert", "cmo", "williams_r", "zscore_revert", "force_idx"},
    "volatility": {"boll_break", "vwap_revert", "keltner", "donchian"},
    "volume": {"vol_mom", "obv_div", "chaikin_mf", "vpt"},
    "prediction_market": {"kalshi", "polymarket"},
}

# Flat lookup: strategy_name -> group_name
STRATEGY_GROUP: Dict[str, str] = {}
for group, members in INDEPENDENCE_GROUPS.items():
    for m in members:
        STRATEGY_GROUP[m] = group

# Default backtest weights when bt_cache has no data for a strategy
DEFAULT_STRATEGY_WEIGHTS: Dict[str, float] = {
    "ema_cross": 0.6,
    "rsi_revert": 0.5,
    "boll_break": 0.5,
    "zscore_revert": 0.4,
    "vol_mom": 0.5,
    "macd": 0.6,
    "vwap_revert": 0.4,
    "obv_div": 0.5,
    "cmo": 0.5,
    "trix": 0.5,
    "adx": 0.6,
    "keltner": 0.5,
    "chaikin_mf": 0.5,
    "williams_r": 0.5,
    "psar": 0.4,
    "hma": 0.6,
    "force_idx": 0.5,
    "vpt": 0.5,
    "donchian": 0.5,
    "aroon": 0.6,
    "kalshi": 0.5,
    "polymarket": 0.5,
}

# Asset class boost multipliers
CLASS_BOOST = {
    "safe": {"trend": 1.3, "momentum": 0.7, "volatility": 0.8, "volume": 1.0, "prediction_market": 0.9},
    "growth": {"trend": 1.1, "momentum": 1.1, "volatility": 1.0, "volume": 1.0, "prediction_market": 1.0},
    "speculative": {"trend": 0.8, "momentum": 1.3, "volatility": 1.2, "volume": 1.1, "prediction_market": 1.2},
}


@dataclass
class AggregatedSignal:
    """Aggregated signal from multiple strategies."""
    asset: str
    direction: str              # "BUY" | "SELL"
    confidence: float           # 0-1, boosted by agreement
    raw_confidence: float       # 0-1, average before boosting
    agreeing_groups: int        # Number of independent groups agreeing
    total_groups: int           # Total independent groups that fired
    strategy_count: int         # Total raw strategy signals
    strategies: List[str]       # All contributing strategy names
    best_reason: str            # Most informative reason string
    asset_class: str = ""


class ConfidenceMatrix:
    """Aggregate and boost signals across strategies."""

    def __init__(self, bt_cache: Optional[Dict[str, dict]] = None):
        self.bt_cache = bt_cache or {}

    def aggregate(
        self,
        signals: List[StrategySignal],
        asset_class: str = "growth",
        currency: str = "",
    ) -> List[AggregatedSignal]:
        """Group signals by (BUY/SELL) and compute aggregate confidence.

        Returns a list with at most 2 entries (one BUY, one SELL), sorted
        by confidence descending.
        """
        if not signals:
            return []

        # Group by direction
        groups: Dict[str, List[StrategySignal]] = {
            "BUY": [s for s in signals if s.action == "BUY"],
            "SELL": [s for s in signals if s.action == "SELL"],
        }

        results: List[AggregatedSignal] = []
        for direction, dir_signals in groups.items():
            if not dir_signals:
                continue

            # Count unique strategy names and groups
            strategy_names = [s.strategy for s in dir_signals]
            unique_names = list(set(strategy_names))
            unique_groups = set()
            for name in unique_names:
                grp = STRATEGY_GROUP.get(name)
                if grp:
                    unique_groups.add(grp)

            # Compute weighted confidence
            total_weight = 0.0
            weighted_conf = 0.0
            best_reason = ""
            best_conf = 0.0

            for s in dir_signals:
                # Get strategy weight from bt_cache or defaults
                weight = self._strategy_weight(s.strategy, currency)
                class_boost = self._class_boost(s.strategy, asset_class)
                effective_weight = weight * class_boost

                weighted_conf += s.confidence * effective_weight
                total_weight += effective_weight

                if s.confidence > best_conf:
                    best_conf = s.confidence
                    best_reason = s.reason

            avg_conf = weighted_conf / total_weight if total_weight > 0 else 0.0

            # Boost confidence based on independent group agreement
            total_possible_groups = len(INDEPENDENCE_GROUPS)
            agreeing = len(unique_groups)
            if agreeing >= 2:
                # Each additional independent group adds 15% boost
                boost = 1.0 + (agreeing - 1) * 0.15
                avg_conf = min(avg_conf * boost, 1.0)
            elif agreeing == 0:
                avg_conf *= 0.5  # No group signal — penalize

            # Add a small bonus for strategy count diversity
            if len(unique_names) >= 3:
                avg_conf = min(avg_conf * 1.1, 1.0)

            results.append(AggregatedSignal(
                asset=currency,
                direction=direction,
                confidence=round(avg_conf, 4),
                raw_confidence=round(weighted_conf / total_weight if total_weight > 0 else 0.0, 4),
                agreeing_groups=agreeing,
                total_groups=total_possible_groups,
                strategy_count=len(unique_names),
                strategies=unique_names,
                best_reason=best_reason,
                asset_class=asset_class,
            ))

        results.sort(key=lambda a: a.confidence, reverse=True)
        return results

    def _strategy_weight(self, strategy: str, currency: str) -> float:
        """Get weight from backtest cache or use default."""
        cache_key = f"{strategy}/{currency}"
        cached = self.bt_cache.get(cache_key)
        if cached and isinstance(cached, dict):
            win_rate = cached.get("win_rate", 0)
            sharpe = cached.get("sharpe_ratio", 0)
            pf = cached.get("profit_factor", 1.0)
            if win_rate > 0 and sharpe > 0:
                return min(0.3 + win_rate * 0.4 + sharpe * 0.3, 1.0)
        return DEFAULT_STRATEGY_WEIGHTS.get(strategy, 0.5)

    def _class_boost(self, strategy: str, asset_class: str) -> float:
        grp = STRATEGY_GROUP.get(strategy, "momentum")
        return CLASS_BOOST.get(asset_class, CLASS_BOOST["growth"]).get(grp, 1.0)


def format_aggregated(sig: AggregatedSignal) -> str:
    return (
        f"  {sig.direction} {sig.asset} | conf={sig.confidence:.1%} "
        f"(raw={sig.raw_confidence:.1%}, {sig.agreeing_groups}/{sig.total_groups} groups, "
        f"{sig.strategy_count} strategies: {', '.join(sig.strategies)})\n"
        f"    {sig.best_reason}"
    )
