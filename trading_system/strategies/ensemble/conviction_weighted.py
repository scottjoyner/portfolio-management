from __future__ import annotations

import math

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


def _sma(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _zscore(window: list[float]) -> float:
    if len(window) < 2:
        return 0.0
    mean = _sma(window)
    var = sum((c - mean) ** 2 for c in window) / len(window)
    sd = math.sqrt(var) + 1e-9
    return (window[-1] - mean) / sd


def _ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2.0 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema


class ConvictionWeightedCompositeStrategy(BaseSignalStrategy):
    """Combines directional sub-scores weighted by z-score magnitude conviction."""

    def __init__(self, warmup_period: int = 30) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="ConvictionWeightedComposite",
                strategy_type="ensemble_weighted",
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=20, warmup_period=warmup_period),
        )
        self.warmup_period = warmup_period

    def _directional(self, closes: list[float], volumes: list[float]) -> list[tuple[float, float]]:
        out: list[tuple[float, float]] = []
        if len(closes) >= 30:
            z = _zscore(closes[-30:])
            dir_z = max(-1.0, min(1.0, z / 3.0))
            conv = min(1.0, abs(z) / 3.0)
            out.append((dir_z, conv))
        if len(closes) >= 22:
            ema_fast = _ema(closes[-22:], 9)
            ema_slow = _ema(closes[-22:], 21)
            diff = (ema_fast - ema_slow) / (ema_slow + 1e-9)
            dir_e = max(-1.0, min(1.0, diff * 50.0))
            conv = min(1.0, abs(diff) * 50.0)
            out.append((dir_e, conv))
        if len(closes) >= 20:
            win = closes[-20:]
            mid = _sma(win)
            var = sum((c - mid) ** 2 for c in win) / len(win)
            sd = math.sqrt(var) + 1e-9
            dist = (closes[-1] - mid) / (2 * sd + 1e-9)
            dir_b = max(-1.0, min(1.0, -dist))
            conv = min(1.0, abs(dist))
            out.append((dir_b, conv))
        if len(volumes) >= 2 and len(closes) >= 2:
            v_avg = _sma(volumes[-10:])
            vol_ratio = (volumes[-1] / (v_avg + 1e-9))
            ret = (closes[-1] - closes[-2]) / (closes[-2] + 1e-9)
            dir_v = max(-1.0, min(1.0, ret * 50.0))
            conv = min(1.0, vol_ratio / 2.0)
            out.append((dir_v, conv))
        return out

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        volumes = market_state.get("volumes") or []
        if len(closes) < self.warmup_period:
            return None

        sub = self._directional(closes, volumes)
        total_conv = sum(w for _, w in sub) + 1e-9
        score = sum(s * w for s, w in sub) / total_conv

        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=max(-1.0, min(1.0, score)),
            reason=f"conviction-weighted composite score={score:.3f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"n_sub": len(sub), "total_conviction": round(total_conv, 4)},
        )
