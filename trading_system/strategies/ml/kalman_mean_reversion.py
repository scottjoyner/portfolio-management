from __future__ import annotations

import math

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class KalmanAdaptiveMeanReversionStrategy(BaseSignalStrategy):
    """1D Kalman-filter adaptive mean reversion kept in self-contained state."""

    def __init__(self, window: int = 50) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="KalmanAdaptiveMeanReversion",
                strategy_type="mean_reversion",
                data_requirements=["product_id", "close", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=30, warmup_period=window),
        )
        self.window = window
        self.x = 0.0
        self.p = 1.0
        self.q = 1e-4
        self.r = 0.01
        self.history: list[float] = []

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

        price = closes[-1]
        for p in closes[-self.window:]:
            self.x += self.q
            k = self.x / (self.x + self.r)
            self.x = (1 - k) * self.x
            self.x += k * (p - (self.x))
            resid = p - self.x
            self.r = 0.95 * self.r + 0.05 * (resid * resid) if self.r > 0 else 0.01

        est = self.x
        if len(self.history) >= self.window:
            var = sum((h - est) ** 2 for h in self.history) / max(1, len(self.history))
        else:
            var = self.p
        self.history.append(price)
        if len(self.history) > self.window:
            self.history.pop(0)

        sigma = math.sqrt(var) + 1e-9
        z = (price - est) / sigma
        if abs(z) < 2.0:
            return None

        score = max(-1.0, min(1.0, -z / 4.0))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"Kalman est={est:.2f} price={price:.2f} z={z:.2f} (>2sigma rev)",
            confidence=min(1.0, abs(z) / 4.0),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"estimate": round(est, 4), "sigma": round(sigma, 4), "z": round(z, 3)},
        )
