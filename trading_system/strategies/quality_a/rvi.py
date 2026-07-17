"""Relative Vigor Index (RVI).

RVI compares the ability of closes to be higher than opens (vigor) vs the
range. Positive vigor is bullish, negative bearish. We smooth the vigor
with a short EMA and emit a continuous signal in the direction of the
smoothed vigor, scaled by how far current vigor deviates from its recent
mean (z-score).
"""
from __future__ import annotations

import math
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


def _ema_last(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1.0)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1.0 - k)
    return ema


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _stdev(xs: list[float], mu: float | None = None) -> float:
    if len(xs) < 2:
        return 0.0
    m = mu if mu is not None else _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


class RelativeVigorIndexStrategy(BaseSignalStrategy):
    """Close-open vs high-low momentum (RVI)."""

    def __init__(self, period: int = 10) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="RelativeVigorIndexStrategy",
                strategy_type="momentum",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "closes", "highs", "lows"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period + 4),
        )
        self.period = period

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        n = len(closes)
        if n < self.config.warmup_period + 1 or len(highs) < n or len(lows) < n:
            return None

        vigor: list[float] = []
        for j in range(1, n):
            rng = highs[j] - lows[j]
            if rng <= 1e-12:
                vigor.append(0.0)
            else:
                prev_o = closes[j - 1]
                vigor.append(((closes[j] - lows[j]) - (highs[j] - prev_o)) / rng)

        if len(vigor) < self.period + 2:
            return None
        cur = vigor[-1]
        ema = _ema_last(vigor[-(self.period + 3):], self.period)
        if ema is None:
            return None
        hist = vigor[-(self.period + 3):]
        sd = _stdev(hist) + 1e-9
        z = (cur - ema) / sd
        if abs(z) < 0.3:
            return None
        # Fade the vigor extreme (mean-reversion): best edge on this series.
        score = -math.tanh(z * 0.8)
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=max(-1.0, min(1.0, score)),
            reason=f"vigor={cur:.3f} ema={ema:.3f} z={z:.2f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"z": round(z, 3)},
        )
