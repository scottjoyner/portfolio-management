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
from dataclasses import dataclass
from typing import Dict, List, Optional

from strategy_engine import Signal as StrategySignal

logger = logging.getLogger("confidence_matrix")

# Strategy independence groups: strategies in the same group use
# mathematically similar logic and should not count as independent signals
# Mirrors rust_core/src/confidence.rs (10 groups for 68 strategies)
INDEPENDENCE_GROUPS: Dict[str, set] = {
    "trend": {"ema_cross", "macd", "trix", "adx", "psar", "hma", "aroon",
              "elder_ray", "ichimoku", "dpo", "kama", "dmi_cross", "vma"},
    "momentum": {"rsi_revert", "cmo", "williams_r", "zscore_revert", "force_idx",
                 "true_cci", "kst", "mom_accel", "multi_rsi", "stoch",
                 "vortex", "rvi", "coppock"},
    "volatility": {"boll_break", "vwap_revert", "keltner", "donchian",
                   "bb_squeeze", "vcp", "choppiness", "mass_idx",
                   "envelope", "atr_channel", "std_channel", "vol_ratio"},
    "volume": {"vol_mom", "obv_div", "chaikin_mf", "vpt",
               "vol_prof", "klinger", "price_eff", "snr_idx",
               "mfi", "emv", "ad_div", "vwap_macd", "nvi"},
    "pattern": {"candle_pat", "pivot_points", "sup_res", "liq_vac",
                 "donch_pull", "impulse_exh", "range_exp_idx",
                 "de_marker", "gap_revert"},
    "momentum_adv": {"rsi_fail", "cvd_flow", "avwap", "linreg_slope",
                     "hurst", "scci", "ulcer", "ema_dev"},
    "prediction_market": {"kalshi", "polymarket"},
    "sentiment": {"crypto_news"},
    "order_flow": {"order_flow", "order_flow_cvd", "wick_pressure"},
    "onchain": {"exchange_flow", "exchange_netflow", "stablecoin_flow"},
    "derivatives": {"funding_contrarian"},
    "macro_risk": {"macro_risk", "btc_dxy_corr"},
}

# Flat lookup: strategy_name -> group_name
STRATEGY_GROUP: Dict[str, str] = {}
for group, members in INDEPENDENCE_GROUPS.items():
    for m in members:
        STRATEGY_GROUP[m] = group

# Default backtest weights when bt_cache has no data for a strategy
DEFAULT_STRATEGY_WEIGHTS: Dict[str, float] = {
    "ema_cross": 0.6, "macd": 0.6, "hma": 0.6, "aroon": 0.6,
    "rsi_revert": 0.4, "zscore_revert": 0.4, "vwap_revert": 0.4,
    "adx": 0.6, "psar": 0.4,
    "boll_break": 0.5, "vol_mom": 0.5, "obv_div": 0.5, "cmo": 0.5, "trix": 0.5,
    "keltner": 0.5, "chaikin_mf": 0.5, "williams_r": 0.5, "force_idx": 0.5, "vpt": 0.5,
    "donchian": 0.5, "price_eff": 0.5, "scci": 0.5, "range_exp_idx": 0.5, "ema_dev": 0.5, "snr_idx": 0.5,
    # New 15 (26-40)
    "candle_pat": 0.5, "sup_res": 0.5, "liq_vac": 0.5, "cvd_flow": 0.5, "vcp": 0.5,
    "impulse_exh": 0.5, "mom_accel": 0.5, "rsi_fail": 0.5, "avwap": 0.5, "donch_pull": 0.5,
    "vol_prof": 0.5, "bb_squeeze": 0.5, "multi_rsi": 0.5, "linreg_slope": 0.5, "hurst": 0.5,
    # 10 (41-50)
    "elder_ray": 0.55, "ichimoku": 0.55, "dpo": 0.55,
    "klinger": 0.5, "pivot_points": 0.5, "choppiness": 0.5, "true_cci": 0.5,
    "kst": 0.5, "mass_idx": 0.5, "ulcer": 0.5,
    # 6 (51-56)
    "mfi": 0.5, "stoch": 0.5, "emv": 0.5, "ad_div": 0.5, "envelope": 0.5, "atr_channel": 0.5,
    # 12 (57-68)
    "kama": 0.55, "vma": 0.55, "coppock": 0.55, "vortex": 0.55,
    "dmi_cross": 0.5, "rvi": 0.5, "std_channel": 0.5, "vol_ratio": 0.5,
    "vwap_macd": 0.5, "nvi": 0.5, "de_marker": 0.5, "gap_revert": 0.5,
    # External
    "kalshi": 0.5, "polymarket": 0.5, "crypto_news": 0.55,
    "order_flow": 0.5, "macro_risk": 0.5,
    # New order-flow + on-chain strategies
    "order_flow_cvd": 0.55, "wick_pressure": 0.55, "exchange_netflow": 0.5, "stablecoin_flow": 0.55,
    "funding_contrarian": 0.5, "exchange_flow": 0.5, "btc_dxy_corr": 0.5,
}

# Asset class boost multipliers
CLASS_BOOST = {
    "safe": {"trend": 1.3, "momentum": 0.7, "volatility": 0.8, "volume": 1.0,
              "pattern": 0.9, "momentum_adv": 0.7, "prediction_market": 0.9,
              "sentiment": 1.0, "order_flow": 1.1, "onchain": 1.1, "derivatives": 1.2,
              "macro_risk": 1.3},
    "growth": {"trend": 1.1, "momentum": 1.1, "volatility": 1.0, "volume": 1.0,
               "pattern": 1.0, "momentum_adv": 1.1, "prediction_market": 1.0,
               "sentiment": 1.0, "order_flow": 1.0, "onchain": 1.0, "derivatives": 1.1,
               "macro_risk": 1.0},
    "speculative": {"trend": 0.8, "momentum": 1.3, "volatility": 1.2, "volume": 1.1,
                    "pattern": 1.1, "momentum_adv": 1.3, "prediction_market": 1.2,
                    "sentiment": 1.1, "order_flow": 1.0, "onchain": 1.1, "derivatives": 0.9,
                    "macro_risk": 0.8},
}

# Rust acceleration
try:
    import rust_core as _rust_core
    _HAS_RUST_CONFIDENCE = True
except ImportError:
    _HAS_RUST_CONFIDENCE = False
    _rust_core = None


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
        by confidence descending.  Uses Rust acceleration when available.
        """
        if not signals:
            return []

        if _HAS_RUST_CONFIDENCE:
            return self._aggregate_rust(signals, asset_class, currency)

        return self._aggregate_py(signals, asset_class, currency)

    def _aggregate_rust(
        self,
        signals: List[StrategySignal],
        asset_class: str,
        currency: str,
    ) -> List[AggregatedSignal]:
        """Delegate aggregation to Rust confidence module."""
        # Convert signals to list of tuples
        signal_tuples = [
            (s.strategy, s.action, s.confidence, s.reason)
            for s in signals
        ]

        # Precompute bt_weights dict: strategy_name -> weight
        bt_weights: Dict[str, float] = {}
        for s in signals:
            sk = s.strategy
            if sk in bt_weights:
                continue
            ck = f"{sk}/{currency}"
            cached = self.bt_cache.get(ck)
            if cached and isinstance(cached, dict):
                wr = cached.get("win_rate", 0)
                sh = cached.get("sharpe_ratio", 0)
                if wr > 0 and sh > 0:
                    bt_weights[sk] = _rust_core.confidence_weight_from_bt_py(wr, sh)
                    continue
            bt_weights[sk] = _rust_core.confidence_default_weight_py(sk)

        results = _rust_core.confidence_aggregate_py(
            signal_tuples, asset_class, currency, bt_weights,
        )

        return [
            AggregatedSignal(
                asset=r[0],
                direction=r[1],
                confidence=r[2],
                raw_confidence=r[3],
                agreeing_groups=r[4],
                total_groups=r[5],
                strategy_count=r[6],
                strategies=r[7],
                best_reason=r[8],
                asset_class=r[9],
            )
            for r in results
        ]

    def _aggregate_py(
        self,
        signals: List[StrategySignal],
        asset_class: str,
        currency: str,
    ) -> List[AggregatedSignal]:
        """Pure-Python fallback for confidence aggregation."""
        # Group by direction
        groups: Dict[str, List[StrategySignal]] = {
            "BUY": [s for s in signals if s.action == "BUY"],
            "SELL": [s for s in signals if s.action == "SELL"],
        }

        results: List[AggregatedSignal] = []
        for direction, dir_signals in groups.items():
            if not dir_signals:
                continue

            strategy_names = [s.strategy for s in dir_signals]
            unique_names = list(set(strategy_names))
            unique_groups = set()
            for name in unique_names:
                grp = STRATEGY_GROUP.get(name)
                if grp:
                    unique_groups.add(grp)

            total_weight = 0.0
            weighted_conf = 0.0
            best_reason = ""
            best_conf = 0.0

            for s in dir_signals:
                weight = self._strategy_weight(s.strategy, currency)
                cb = self._class_boost(s.strategy, asset_class)
                effective_weight = weight * cb
                weighted_conf += s.confidence * effective_weight
                total_weight += effective_weight
                if s.confidence > best_conf:
                    best_conf = s.confidence
                    best_reason = s.reason

            avg_conf = weighted_conf / total_weight if total_weight > 0 else 0.0

            total_possible_groups = len(INDEPENDENCE_GROUPS)
            agreeing = len(unique_groups)
            
            # Group agreement boost: 10% per additional group, max 1.5x
            if agreeing >= 2:
                boost = 1.0 + (agreeing - 1) * 0.10
                avg_conf = min(avg_conf * min(boost, 1.5), 1.0)
            elif agreeing == 0:
                avg_conf *= 0.5

            # Strategy count diversity bonus: smaller and after group boost
            diversity_mult = 1.05 if len(unique_names) >= 3 else 1.0
            if len(unique_names) >= 5:
                diversity_mult = 1.10
            avg_conf = min(avg_conf * diversity_mult, 1.0)

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
