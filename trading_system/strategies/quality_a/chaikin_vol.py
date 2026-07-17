"""Chaikin Volatility contraction-breakout.

Measures the rate-of-change of the trading range (high-low). A contraction
(range ROC well below its smoothed baseline) followed by a directional
price move signals a volatility breakout out of compression. We trade the
direction of the price move when range contraction has just occurred.
This captures the expansion that follows a quiet regime.
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


class ChaikinVolatilityBreakoutStrategy(BaseSignalStrategy):
    """Volatility contraction -> breakout signal."""

    def __init__(self, ema_period: int = 10, roc_period: int = 10, contraction: float = -0.3) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="ChaikinVolatilityBreakoutStrategy",
                strategy_type="volatility",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "closes", "highs", "lows"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=ema_period + roc_period + 2),
        )
        self.ema_period = ema_period
        self.roc_period = roc_period
        self.contraction = contraction

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

        ranges = [highs[j] - lows[j] for j in range(n)]
        if ranges[-1] <= 0 or ranges[-1 - self.roc_period] <= 0:
            return None
        roc = (ranges[-1] - ranges[-1 - self.roc_period]) / ranges[-1 - self.roc_period]
        base = _ema_last(ranges[-(self.ema_period + self.roc_period + 5):], self.ema_period)
        if base is None or base <= 0:
            return None
        baseline_roc = (ranges[-1] - base) / base
        excess = roc - baseline_roc

        ret = (closes[-1] - closes[-2]) / closes[-2] if closes[-2] > 0 else 0.0
        if excess >= self.contraction or ret == 0.0:
            return None

        direction = 1.0 if ret >= 0 else -1.0
        score = direction * min(1.0, abs(ret) * 5.0 + 0.2)
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=max(-1.0, min(1.0, score)),
            reason=f"vol_roc={roc:.3f} excess={excess:.3f} ret={ret:+.4f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"vol_roc": round(roc, 4), "excess": round(excess, 4)},
        )
