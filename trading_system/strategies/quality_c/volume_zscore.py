"""Volume-zscore reversion.

Detects volume spikes (z-score of recent volume > threshold) combined with a
small adverse price move (fade). When volume balloons but price fails to make a
large directional move, the move is often exhausted and reverts on the next
bar. Long on volume-spike-down-fade, short on volume-spike-up-fade.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_c.indicators import stdev


class VolumeZscoreReversionStrategy(BaseSignalStrategy):
    def __init__(self, period: int = 20, z_thr: float = 2.0, move_thr: float = 0.002) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VolumeZscoreReversionStrategy",
                strategy_type="mean_reversion",
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period + 2),
        )
        self.period = period
        self.z_thr = z_thr
        self.move_thr = move_thr

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        volumes = market_state.get("volumes") or []
        if len(closes) < self.period + 2 or len(volumes) < self.period + 2:
            return None
        vol_window = volumes[-self.period:]
        vmean = sum(vol_window) / len(vol_window)
        vsd = stdev(vol_window, vmean)
        if vsd <= 0:
            return None
        vz = (volumes[-1] - vmean) / vsd
        if vz < self.z_thr:
            return None
        move = (closes[-1] - closes[-2]) / closes[-2] if closes[-2] else 0.0
        score = 0.0
        reason = ""
        if move <= -self.move_thr:
            score = min(1.0, 0.4 + min(vz, 4.0) / 8.0)
            reason = f"volume z={vz:.1f} spike w/ down move {move:.4f} fade-up"
        elif move >= self.move_thr:
            score = -min(1.0, 0.4 + min(vz, 4.0) / 8.0)
            reason = f"volume z={vz:.1f} spike w/ up move {move:.4f} fade-down"
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
            features={"vol_z": round(vz, 2), "move": round(move, 5)},
        )
