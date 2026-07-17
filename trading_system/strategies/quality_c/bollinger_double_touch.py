"""Bollinger band-pierce rejection reversion.

A higher-probability mean-reversion setup: price pierces the lower Bollinger
band (capitulation), then the very next bar closes back ABOVE the band — a
failed breakdown that tends to snap back. Symmetric for the upper band. The
score scales with how deep the pierce was.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_c.indicators import bollinger


class BollingerDoubleTouchStrategy(BaseSignalStrategy):
    def __init__(self, period: int = 20, num_std: float = 2.0, min_pen: float = 0.002) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="BollingerDoubleTouchStrategy",
                strategy_type="mean_reversion",
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period + 2),
        )
        self.period = period
        self.num_std = num_std
        self.min_pen = min_pen

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        if len(closes) < self.period + 2:
            return None
        price = closes[-1]
        prev = closes[-2]
        mean, upper, lower = bollinger(closes, self.period, self.num_std)
        if mean is None or upper is None or lower is None or mean <= 0:
            return None
        score = 0.0
        reason = ""
        # single-touch band reversion: long when price pierces the lower band,
        # short when it pierces the upper band. Score scales with penetration.
        if price < lower:
            pen = (lower - price) / (lower + 1e-9)
            if pen >= self.min_pen:
                score = min(1.0, 0.4 + pen * 6.0)
                reason = f"price={price:.2f} pierced lower band={lower:.2f} reversion"
        elif price > upper:
            pen = (price - upper) / (upper + 1e-9)
            if pen >= self.min_pen:
                score = -min(1.0, 0.4 + pen * 6.0)
                reason = f"price={price:.2f} pierced upper band={upper:.2f} reversion"
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"lower": round(lower, 2), "upper": round(upper, 2), "mean": round(mean, 2)},
        )
