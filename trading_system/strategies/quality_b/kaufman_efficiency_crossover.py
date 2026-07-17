"""Kaufman Efficiency Ratio regime-adaptive crossover.

Follows the drift in trending regimes (efficiency magnitude high) and fades it
in ranging regimes (efficiency low). The Efficiency Ratio measures trend
strength; we trade with strong directional moves and revert choppy ones.
"""
from __future__ import annotations

import math

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_b.indicators import adapt_to_regime, kaufman_efficiency


class KaufmanEfficiencyCrossover(BaseSignalStrategy):
    def __init__(self, window: int = 20, thr: float = 0.3, regime_period: int = 30, regime_thr: float = 0.3) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="KaufmanEfficiencyCrossover",
                strategy_type="trend_strength",
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=0.0, warmup_period=window + regime_period),
        )
        self.window = window
        self.thr = thr
        self.regime_period = regime_period
        self.regime_thr = regime_thr

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        if len(closes) < self.window + self.regime_period:
            return None
        er = kaufman_efficiency(closes, self.window)
        if er is None or abs(er) < self.thr:
            return None
        window = closes[-(self.window + 1):]
        direction = 1.0 if window[-1] >= window[0] else -1.0
        follow = direction * abs(er)
        score = adapt_to_regime(follow, closes, self.regime_period, self.regime_thr)
        score = max(-1.0, min(1.0, score))
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"Kaufman ER={er:.3f} regime-adaptive follow={follow:.3f}",
            confidence=min(1.0, abs(er)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"efficiency_ratio": round(er, 4)},
        )
