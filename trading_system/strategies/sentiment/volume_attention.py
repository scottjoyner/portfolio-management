from __future__ import annotations

import math

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class VolumeAttentionMomentumStrategy(BaseSignalStrategy):
    """Volume-attention momentum proxied from volume z-score + directional move.

    Detects unusual volume surges (z-score of volume over a rolling baseline)
    coinciding with directional price moves = market "attention" events. Trades
    continuation when volume confirms the direction.
    """

    def __init__(self, window: int = 30, vol_z_thresh: float = 2.0, move_thresh: float = 0.002) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VolumeAttentionMomentum",
                strategy_type="sentiment",
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=120, warmup_period=window),
        )
        self.window = max(5, window)
        self.vol_z_thresh = vol_z_thresh
        self.move_thresh = move_thresh
        self._vol_mean: float | None = None
        self._vol_var: float = 0.0

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

        close = closes[-1]
        prev = closes[-2]
        move = close / prev - 1.0 if prev else 0.0

        vol = float(volumes[-1])
        win_vol = volumes[-self.window:]
        if self._vol_mean is None:
            self._vol_mean = sum(win_vol) / len(win_vol)
            self._vol_var = sum((v - self._vol_mean) ** 2 for v in win_vol) / len(win_vol)
            return None

        mean = self._vol_mean
        var = self._vol_var
        self._vol_mean = 0.95 * mean + 0.05 * vol
        self._vol_var = 0.95 * var + 0.05 * (vol - mean) ** 2

        if self._vol_mean <= 0:
            return None
        sigma = math.sqrt(max(self._vol_var, 1e-12))
        z = (vol - self._vol_mean) / sigma

        if z < self.vol_z_thresh:
            return None
        if abs(move) < self.move_thresh:
            return None

        score = max(-1.0, min(1.0, math.copysign(min(1.0, abs(move) * 100.0 + z / 5.0), move)))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"volume z={z:.2f} confirms move={move:+.4f}: attention continuation",
            confidence=min(1.0, 0.5 + z / 10.0),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"volume_z": round(z, 3), "move": round(move, 5)},
        )
