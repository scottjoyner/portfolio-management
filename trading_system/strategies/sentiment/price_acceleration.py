from __future__ import annotations

import math

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class PriceAccelerationSentimentStrategy(BaseSignalStrategy):
    """Sentiment-momentum from price acceleration proxied from microstructure.

    Second derivative (acceleration) of price combined with volume signals
    euphoria (strong positive accel + high volume = exhaustion to fade) and
    exhaustion (sharp negative accel washing out = buy). Fades euphoria and
    buys capitulation.
    """

    def __init__(self, window: int = 24, accel_thresh: float = 0.0008, vol_mult: float = 1.3) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="PriceAccelerationSentiment",
                strategy_type="sentiment",
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=180, warmup_period=window),
        )
        self.window = max(6, window)
        self.accel_thresh = accel_thresh
        self.vol_mult = vol_mult
        self._vol_mean: float | None = None

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        volumes = market_state.get("volumes") or []
        if len(closes) < self.window or len(volumes) < self.window:
            return None

        c = closes[-3:]
        v = volumes[-3:]
        if len(c) < 3:
            return None

        r1 = c[1] / c[0] - 1.0 if c[0] else 0.0
        r2 = c[2] / c[1] - 1.0 if c[1] else 0.0
        accel = r2 - r1

        vol = float(v[-1])
        if self._vol_mean is None:
            self._vol_mean = sum(volumes[-self.window:]) / self.window
            return None
        self._vol_mean = 0.95 * self._vol_mean + 0.05 * vol
        vol_high = self._vol_mean > 0 and vol > self.vol_mult * self._vol_mean

        score = 0.0
        reason = ""
        if accel > self.accel_thresh and vol_high:
            score = -0.7
            reason = f"euphoria accel={accel:+.5f} w/ high vol -> fade"
        elif accel < -self.accel_thresh and vol_high:
            score = 0.7
            reason = f"exhaustion accel={accel:+.5f} w/ high vol -> buy washout"
        else:
            return None

        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=reason,
            confidence=min(1.0, abs(accel) * 1000.0 + 0.2),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"acceleration": round(accel, 6), "vol_high": vol_high},
        )
