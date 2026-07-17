"""Ehler's Fisher Transform on a de-noised stochastic oscillator.

Mean-reversion on a smoothed (de-noised) stochastic. The Fisher transform
maps a bounded oscillator to a Gaussian-like distribution, sharpening
turning points. We emit a continuous directional signal every bar:
nearly oversold (< -thr) -> long (expect bounce), nearly overbought
(> +thr) -> short (expect fade). Magnitude scales with distance from 0.
"""
from __future__ import annotations

import math
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


def _stochastic(closes: list[float], i: int, period: int) -> float | None:
    if i < period or i >= len(closes):
        return None
    window = closes[i - period:i + 1]
    lo, hi = min(window), max(window)
    if hi - lo <= 1e-12:
        return 0.5
    return (closes[i] - lo) / (hi - lo)


def _fisher(x: float) -> float:
    x = max(-0.999, min(0.999, x))
    return 0.5 * math.log((1.0 + x) / (1.0 - x))


class FisherTransformStochStrategy(BaseSignalStrategy):
    """Mean-reversion on the Fisher transform of a smoothed stochastic."""

    def __init__(self, period: int = 10, smooth: int = 4, trigger: float = 1.2, min_score: float = 0.6) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="FisherTransformStochStrategy",
                strategy_type="mean_reversion",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "closes"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period + smooth + 2),
        )
        self.period = period
        self.smooth = smooth
        self.trigger = trigger
        self.min_score = min_score
        self._buf: list[float] = []

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        if len(closes) < self.config.warmup_period + 1:
            return None
        i = len(closes) - 1
        raw = _stochastic(closes, i, self.period)
        if raw is None:
            return None
        self._buf.append(raw)
        if len(self._buf) > self.smooth:
            self._buf.pop(0)
        if len(self._buf) < self.smooth:
            return None
        avg = sum(self._buf) / len(self._buf)
        fish = _fisher(avg * 2.0 - 1.0)

        if fish <= -self.trigger:
            score = min(1.0, (abs(fish) - self.trigger) / 2.0 + 0.2)
        elif fish >= self.trigger:
            score = -min(1.0, (abs(fish) - self.trigger) / 2.0 + 0.2)
        else:
            return None
        if abs(score) < self.min_score or abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=max(-1.0, min(1.0, score)),
            reason=f"fisher={fish:.3f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"fisher": round(fish, 4)},
        )
