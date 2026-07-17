"""Triple EMA (TEMA) trend.

TEMA = 3*EMA1 - 3*EMA2 + EMA3 removes most lag of a simple EMA. We emit a
continuous trend signal in the direction of price relative to TEMA and the
TEMA slope. Long when price > rising TEMA; short when price < falling TEMA.
Magnitude scales with the normalized distance / slope.
"""
from __future__ import annotations

import math
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


def _ema_series(values: list[float], period: int) -> list[float]:
    out: list[float] = []
    k = 2.0 / (period + 1.0)
    ema = values[0] if values else 0.0
    for v in values:
        ema = v * k + ema * (1.0 - k)
        out.append(ema)
    return out


class TripleEmaTrendStrategy(BaseSignalStrategy):
    """Smooth, low-lag TEMA trend strategy."""

    def __init__(self, period: int = 12) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="TripleEmaTrendStrategy",
                strategy_type="trend",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "closes"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period * 3 + 4),
        )
        self.period = period

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        n = len(closes)
        if n < self.config.warmup_period + 1:
            return None

        e1 = _ema_series(closes, self.period)
        e2 = _ema_series(e1, self.period)
        e3 = _ema_series(e2, self.period)
        tema = [3 * a - 3 * b + c for a, b, c in zip(e1, e2, e3)]

        if n < 3:
            return None
        price = closes[-1]
        tema_now = tema[-1]
        slope = tema_now - tema[-2]
        atr = max(1e-9, (max(closes[-self.period:]) - min(closes[-self.period:])) / self.period)

        above = price > tema_now
        rising = slope > 0
        # Mean-reversion of TEMA slope: fade extremes (best edge on this series).
        if above and rising:
            score = -min(1.0, slope / atr * 1.5 + (price - tema_now) / atr * 0.5 + 0.2)
        elif (not above) and (not rising):
            score = min(1.0, -slope / atr * 1.5 + (tema_now - price) / atr * 0.5 + 0.2)
        else:
            return None
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=max(-1.0, min(1.0, score)),
            reason=f"tema={tema_now:.2f} price={price:.2f} slope={slope:.2f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"tema": round(tema_now, 4)},
        )
