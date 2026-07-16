from __future__ import annotations

import math

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


def _least_squares_slope(xs: list[float], ys: list[float]) -> tuple[float, float]:
    n = len(xs)
    sx = sum(xs)
    sy = sum(ys)
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in zip(xs, ys))
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-12:
        return 0.0, 0.0
    slope = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n
    return slope, intercept


class OnlineLinearRegressionMomentumStrategy(BaseSignalStrategy):
    """Online least-squares slope with volume confirmation."""

    def __init__(self, window: int = 40) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="OnlineLinearRegressionMomentum",
                strategy_type="momentum",
                live_supported=True,
                data_requirements=["product_id", "close", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.3, cooldown_seconds=30, warmup_period=window),
        )
        self.window = window

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        volumes = market_state.get("volumes") or []
        if not closes and "close" in market_state:
            closes = [market_state["close"]]
        if len(closes) < self.window:
            return None

        win_c = closes[-self.window:]
        xs = list(range(len(win_c)))
        slope, _ = _least_squares_slope(xs, win_c)
        mean_price = sum(win_c) / len(win_c)
        slope_norm = slope / (mean_price + 1e-9)

        vol_confirm = True
        if len(volumes) >= self.window:
            win_v = volumes[-self.window:]
            last_v = win_v[-1]
            avg_v = sum(win_v) / len(win_v)
            vol_confirm = last_v >= avg_v

        score = max(-1.0, min(1.0, slope_norm * 50.0))
        if abs(score) <= self.config.threshold or not vol_confirm:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        direction = "bullish" if score > 0 else "bearish"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"OLS slope_norm={slope_norm:.5f} {direction} vol_confirm={vol_confirm}",
            confidence=min(1.0, abs(slope_norm) * 50.0),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"slope_norm": round(slope_norm, 6), "vol_confirm": vol_confirm},
        )
