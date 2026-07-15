"""
Confidence Modifier Module for Trading Signals.

This module provides a suite of modifiers that can be applied to trading signals
to improve confidence scores based on market conditions, historical performance,
sentiment analysis, and global consensus.

Modifiers included:
1. Liquidity Tiering: Penalize signals on low-liquidity products.
2. Spread Adjustment: Subtract spread costs from raw confidence.
3. Consecutive Signal Confirmation: Boost confidence for repeated signals.
4. Win-Rate Tracking: Weight confidence by historical accuracy per strategy/product.
5. Regime Confidence Gating: Cap confidence based on market regime.
6. Cross-Correlation Penalty: Penalize signals that conflict with market leaders.
7. Sentiment Integration: Boost/penalize based on news sentiment scores.
8. Global Consensus: Boost confidence if the signal aligns with the majority of market movements.
9. Robustness Scoring: Penalize isolated signals on low-liquidity pairs.
"""

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any


@dataclass
class ConfidenceModifierResult:
    original_confidence: float
    modified_confidence: float
    modifiers_applied: List[str]
    notes: List[str] = field(default_factory=list)


class ConfidenceEngine:
    def __init__(
        self,
        liquidity_tiers: Optional[Dict[str, int]] = None,
        win_rates: Optional[Dict[Tuple[str, str], float]] = None,
        regime_caps: Optional[Dict[str, float]] = None,
    ):
        # Liquidity tiers: {product_id: tier (1-5)}
        self.liquidity_tiers = liquidity_tiers if liquidity_tiers is not None else {}
        # Win rates: {(strategy_name, product_id): win_rate (0.0-1.0)}
        self.win_rates = win_rates if win_rates is not None else {}
        # Regime caps: {regime_name: max_confidence}
        self.regime_caps = (
            regime_caps
            if regime_caps is not None
            else {"volatile": 0.4, "quiet": 0.6, "trending": 1.0}
        )

        # State for consecutive signals: {(product_id, strategy_name): last_signal_action}
        self.consecutive_signals: Dict[Tuple[str, str], str] = {}

    def apply_modifiers(
        self,
        signal: Any,
        market_data: Dict[str, Any],
        regime: str = "neutral",
        market_leaders: Optional[List[str]] = None,
        sentiment_score: float = 0.0,
        global_consensus: float = 0.0,
    ) -> ConfidenceModifierResult:
        """
        Applies all confidence modifiers to a signal.

        All modifiers are applied as MULTIPLIERS on the base confidence.
        A single final clamp at 1.0 prevents cumulative overflow.
        """
        base_confidence = float(getattr(signal, "strength", 0.5))
        multipliers: List[float] = [1.0]  # start with identity
        modifiers_applied: List[str] = []
        notes: List[str] = []

        # 1. Liquidity Tiering & Robustness (penalty multiplier)
        tier = self.liquidity_tiers.get(signal.symbol, 3)
        if tier >= 4:
            mult = 1.0 - (tier - 3) * 0.15  # 0.85, 0.70, 0.55...
            multipliers.append(mult)
            modifiers_applied.append("liquidity_tier")
            notes.append(f"Low liquidity tier {tier} penalty applied.")

        # 2. Spread Adjustment (subtractive, then normalize)
        spread = float(market_data.get("spread", 0.0))
        if spread > 0:
            spread_penalty = spread * 100  # convert to bps equivalent
            # convert to multiplier: 1 - spread_penalty (cap at 0.95)
            mult = max(0.7, 1.0 - spread_penalty)
            multipliers.append(mult)
            modifiers_applied.append("spread_adj")
            notes.append(f"Spread adjustment ({spread * 10000:.1f}bps) applied.")

        # 3. Consecutive Signal Confirmation (small boost)
        key = (signal.symbol, signal.strategy)
        last_action = self.consecutive_signals.get(key)
        if last_action == signal.action:
            multipliers.append(1.05)  # 5% boost instead of +0.1
            modifiers_applied.append("consecutive")
            notes.append("Consecutive signal boost applied.")
        else:
            self.consecutive_signals[key] = signal.action

        # 4. Win-Rate Tracking
        win_rate = self.win_rates.get((signal.strategy, signal.symbol))
        if win_rate is not None and win_rate > 0:
            multipliers.append(max(0.3, win_rate))  # floor at 0.3
            modifiers_applied.append("win_rate")
            notes.append(f"Win-rate weighting ({win_rate:.2f}) applied.")

        # 5. Sentiment Integration (multiplier)
        if sentiment_score != 0:
            action_val = 1 if signal.action == "BUY" else -1
            if (sentiment_score > 0 and action_val == 1) or (
                sentiment_score < 0 and action_val == -1
            ):
                # Aligned: up to 20% boost (was 40%)
                mult = 1.0 + min(abs(sentiment_score) * 0.2, 0.2)
                multipliers.append(mult)
                modifiers_applied.append("sentiment")
                notes.append(f"Sentiment boost ({sentiment_score:.2f}) applied.")
            else:
                # Misaligned: penalty
                multipliers.append(0.85)
                modifiers_applied.append("sentiment_p")
                notes.append(f"Sentiment penalty ({sentiment_score:.2f}) applied.")

        # 6. Global Consensus (multiplier)
        if global_consensus > 0.6:
            multipliers.append(1.15)  # 15% boost (was +0.3 additive)
            modifiers_applied.append("consensus")
            notes.append(f"Global consensus boost ({global_consensus:.1%}) applied.")
        elif global_consensus < 0.4 and global_consensus > 0:
            multipliers.append(0.85)
            modifiers_applied.append("consensus_p")
            notes.append(f"Global consensus penalty ({global_consensus:.1%}) applied.")

        # 7. Regime Confidence Gating (hard cap applied at end)
        cap = self.regime_caps.get(regime, 1.0)

        # 8. Cross-Correlation Penalty
        if market_leaders:
            leader_changes = []
            for leader in market_leaders:
                data = market_data.get(leader, {})
                leader_changes.append(float(data.get("change_pct", 0)))

            avg_leader_change = (
                sum(leader_changes) / len(leader_changes) if leader_changes else 0
            )

            leader_change = avg_leader_change
            if abs(leader_change) > 1.0:
                leader_change /= 100.0

            if signal.action == "BUY" and leader_change < -0.01:
                multipliers.append(0.85)
                modifiers_applied.append("correlation")
                notes.append("Cross-correlation penalty applied (leaders dumping).")
            elif signal.action == "SELL" and leader_change > 0.01:
                multipliers.append(0.85)
                modifiers_applied.append("correlation")
                notes.append("Cross-correlation penalty applied (leaders pumping).")

        # Apply all multipliers then single final clamp
        final_conf = base_confidence
        for m in multipliers:
            final_conf *= m
        final_conf = min(final_conf, cap)  # regime cap
        final_conf = max(0.0, min(1.0, final_conf))  # hard clamp 0-1

        return ConfidenceModifierResult(
            original_confidence=base_confidence,
            modified_confidence=final_conf,
            modifiers_applied=modifiers_applied,
            notes=notes,
        )
