"""
Signal Processor - Confidence Modifier Pipeline

Wraps the WorkerEngine and applies confidence modifiers (liquidity tiering,
spread adjustment, sentiment, regime gating, etc.) between strategy signal
generation and risk evaluation.
"""

from __future__ import annotations

import logging
from typing import Any

from trading_system.signal_confidence import ConfidenceEngine, ConfidenceModifierResult
from trading_system.strategies.base.interfaces import StrategySignal

logger = logging.getLogger(__name__)


class SignalProcessor:
    """Applies confidence modifiers to raw strategy signals."""

    def __init__(
        self, engine: Any, confidence_engine: ConfidenceEngine | None = None
    ) -> None:
        self.engine = engine
        self.confidence_engine = confidence_engine or ConfidenceEngine()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate_market_state(
        self, product_id: str, market_state: dict[str, Any], mode: str = "paper"
    ) -> list[dict[str, Any]]:
        """Evaluate strategies for a product and apply confidence modifiers."""
        raw_signals = self.engine.evaluate_market_state(product_id, market_state, mode)

        # Build a minimal signal object that ConfidenceEngine expects
        class _Signal:
            def __init__(self, s: dict[str, Any]) -> None:
                self.symbol = s["signal"].product_id
                self.action = "BUY" if s["signal"].score > 0 else "SELL"
                self.strength = s["signal"].confidence
                self.strategy = s["strategy_id"]

        modified: list[dict[str, Any]] = []
        for raw in raw_signals:
            sig_obj = _Signal(raw)
            result = self.confidence_engine.apply_modifiers(
                signal=sig_obj,
                market_data=market_state,
                regime=market_state.get("regime", "neutral"),
                market_leaders=market_state.get("market_leaders"),
                sentiment_score=market_state.get("sentiment_score", 0.0),
                global_consensus=market_state.get("global_consensus", 0.0),
            )

            # Attach modifier info to the signal dict so downstream can inspect it
            raw["confidence_modifier"] = result
            raw["signal"].confidence = result.modified_confidence
            modified.append(raw)

        return modified

    def evaluate_order(
        self, signal: dict[str, Any], market_state: dict[str, Any], mode: str = "paper"
    ) -> tuple[bool, str]:
        """Delegate to the engine's risk evaluation."""
        return self.engine.evaluate_order(signal, market_state, mode)
