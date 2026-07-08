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

        Args:
            signal: The raw signal object (must have symbol, action, strength, strategy)
            market_data: Dictionary of market data (price, volume_24h, spread, etc.)
            regime: Current market regime (e.g., 'volatile', 'trending')
            market_leaders: List of symbols for cross-correlation check (e.g., ['BTC-USD'])
            sentiment_score: Sentiment score from news analysis (-1.0 to +1.0)
            global_consensus: Percentage of all pairs signaling the same direction (0.0 to 1.0)

        Returns:
            ConfidenceModifierResult
        """
        confidence = float(getattr(signal, "strength", 0.5))
        original_confidence = confidence
        modifiers_applied: List[str] = []
        notes: List[str] = []

        # 1. Liquidity Tiering & Robustness
        tier = self.liquidity_tiers.get(signal.symbol, 3)
        if tier >= 4:
            confidence *= 1 - (tier - 3) * 0.2
            modifiers_applied.append("liquidity_tier")
            notes.append(f"Low liquidity tier {tier} penalty applied.")

        # 2. Spread Adjustment
        spread = float(market_data.get("spread", 0.0))
        if spread > 0:
            confidence -= spread
            modifiers_applied.append("spread_adjustment")
            notes.append(f"Spread adjustment ({spread * 10000:.1f}bps) applied.")

        # 3. Consecutive Signal Confirmation
        key = (signal.symbol, signal.strategy)
        last_action = self.consecutive_signals.get(key)
        if last_action == signal.action:
            confidence = min(1.0, confidence + 0.1)
            modifiers_applied.append("consecutive_confirmation")
            notes.append("Consecutive signal boost applied.")
        else:
            self.consecutive_signals[key] = signal.action

        # 4. Win-Rate Tracking (only if explicitly configured)
        win_rate = self.win_rates.get((signal.strategy, signal.symbol))
        if win_rate is not None:
            confidence *= win_rate
            modifiers_applied.append("win_rate_tracking")
            notes.append(f"Win-rate weighting ({win_rate:.2f}) applied.")

        # 5. Sentiment Integration
        if sentiment_score != 0:
            # Boost confidence if sentiment aligns with signal direction
            # Assume signal.action is 'BUY' (1) or 'SELL' (-1)
            action_val = 1 if signal.action == "BUY" else -1
            if (sentiment_score > 0 and action_val == 1) or (
                sentiment_score < 0 and action_val == -1
            ):
                confidence = min(1.0, confidence + (abs(sentiment_score) * 0.4))
                modifiers_applied.append("sentiment_integration")
                notes.append(f"Sentiment boost ({sentiment_score:.2f}) applied.")
            else:
                confidence *= 0.8
                modifiers_applied.append("sentiment_penalty")
                notes.append(f"Sentiment penalty ({sentiment_score:.2f}) applied.")

        # 6. Global Consensus
        if global_consensus > 0.6:
            confidence = min(1.0, confidence + 0.3)
            modifiers_applied.append("global_consensus")
            notes.append(f"Global consensus boost ({global_consensus:.1%}) applied.")
        elif global_consensus < 0.4 and global_consensus > 0:
            confidence *= 0.8
            modifiers_applied.append("global_consensus_penalty")
            notes.append(f"Global consensus penalty ({global_consensus:.1%}) applied.")

        # 7. Regime Confidence Gating
        cap = self.regime_caps.get(regime, 1.0)
        if confidence > cap:
            confidence = cap
            modifiers_applied.append("regime_gate")
            notes.append(f"Regime cap ({cap}) applied for {regime} regime.")

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
                confidence *= 0.8
                modifiers_applied.append("cross_correlation")
                notes.append(
                    "Cross-correlation penalty applied (market leaders dumping)."
                )
            elif signal.action == "SELL" and leader_change > 0.01:
                confidence *= 0.8
                modifiers_applied.append("cross_correlation")
                notes.append(
                    "Cross-correlation penalty applied (market leaders pumping)."
                )

        return ConfidenceModifierResult(
            original_confidence=original_confidence,
            modified_confidence=confidence,
            modifiers_applied=modifiers_applied,
            notes=notes,
        )
