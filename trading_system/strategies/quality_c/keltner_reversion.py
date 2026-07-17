"""Keltner-band reversion.

When price extends beyond the Keltner channel (EMA center +/- multiplier*ATR),
fade the excursion back toward the channel mid. Excursions beyond an ATR-based
band in a non-trending tape tend to revert on the next bar. Signed score scales
with penetration depth normalized by ATR.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_c.indicators import atr, ema_last


class KeltnerReversionStrategy(BaseSignalStrategy):
    def __init__(self, period: int = 20, mult: float = 1.5) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="KeltnerReversionStrategy",
                strategy_type="mean_reversion",
                data_requirements=["product_id", "closes", "highs", "lows", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period + 2),
        )
        self.period = period
        self.mult = mult

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        if len(closes) < self.period + 2 or len(highs) < self.period + 2 or len(lows) < self.period + 2:
            return None
        mid = ema_last(closes, self.period)
        a = atr(highs, lows, closes, self.period)
        if mid is None or a is None or a <= 0:
            return None
        upper = mid + self.mult * a
        lower = mid - self.mult * a
        price = closes[-1]
        score = 0.0
        reason = ""
        if price > upper:
            pen = (price - upper) / (a + 1e-9)
            score = -min(1.0, 0.3 + pen * 0.4)
            reason = f"price={price:.2f} > Keltner upper={upper:.2f} fade"
        elif price < lower:
            pen = (lower - price) / (a + 1e-9)
            score = min(1.0, 0.3 + pen * 0.4)
            reason = f"price={price:.2f} < Keltner lower={lower:.2f} fade"
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
            features={"mid": round(mid, 2), "upper": round(upper, 2), "lower": round(lower, 2)},
        )
