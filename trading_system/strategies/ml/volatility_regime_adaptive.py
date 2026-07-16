from __future__ import annotations

import math

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class VolatilityRegimeAdaptiveStrategy(BaseSignalStrategy):
    """Rolling-variance regime switch: trend-follow in calm, mean-revert in noisy."""

    def __init__(self, window: int = 40) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VolatilityRegimeAdaptive",
                strategy_type="adaptive",
                data_requirements=["product_id", "close", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.25, cooldown_seconds=30, warmup_period=window),
        )
        self.window = window

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if not closes and "close" in market_state:
            closes = [market_state["close"]]
        if len(closes) < self.window:
            return None

        win = closes[-self.window:]
        mean = sum(win) / len(win)
        var = sum((c - mean) ** 2 for c in win) / len(win)
        sigma = math.sqrt(var) + 1e-9

        last = win[-1]
        prev = win[-2]
        ret = (last - prev) / (prev + 1e-9)

        vol_baseline = sigma / (mean + 1e-9)
        if vol_baseline > 0.01:
            regime = "mean_reversion"
            z = (last - mean) / sigma
            score = max(-1.0, min(1.0, -z / 3.0))
            reason = f"high-vol regime mean-revert z={z:.2f}"
        else:
            regime = "trend_following"
            slope_score = max(-1.0, min(1.0, ret * 50.0))
            score = slope_score
            reason = f"low-vol regime trend-follow ret={ret:.5f}"

        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"regime": regime, "vol_baseline": round(vol_baseline, 6)},
        )
