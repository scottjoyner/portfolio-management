"""
Volatility-Cycle Alignment Strategy.

Detects where price sits within a longer (12x subsampled) timeframe's
Bollinger band and trades accordingly:

- Near the slow-TF band MEAN (mid) -> trade continuation in the direction of
  the slow-TF trend (the "cycle" is mid-swing, momentum should persist).
- Near a slow-TF band EXTREME (upper/lower) -> trade reversion back toward the
  mean (the cycle is stretched, expect snapback).

Derives the slow timeframe by subsampling the single ``closes`` series.
Pure-Python, deterministic.
"""
from __future__ import annotations

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


def _ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1.0)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1.0 - k)
    return ema


def _bollinger(series: list[float], period: int = 20, mult: float = 2.0):
    if len(series) < period:
        return None
    window = series[-period:]
    mean = sum(window) / period
    var = sum((v - mean) ** 2 for v in window) / period
    sd = var ** 0.5
    return mean, mean + mult * sd, mean - mult * sd, sd


def _subsample(closes: list[float], step: int) -> list[float]:
    if step <= 1:
        return list(closes)
    rev = closes[::-1][::step]
    return rev[::-1]


class VolatilityCycleAlignStrategy(BaseSignalStrategy):
    """Continuation near slow-TF band mean; reversion near band extremes."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VolatilityCycleAlignStrategy",
                strategy_type="trend",
                live_supported=False,
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.15, cooldown_seconds=30, warmup_period=60),
        )

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if len(closes) < 60:
            return None

        slow = market_state.get("closes_1d") or _subsample(closes, 6)
        if len(slow) < 20:
            return None

        bb = _bollinger(slow, 20, 2.0)
        if bb is None:
            return None
        mean, upper, lower, sd = bb
        if sd <= 0:
            return None

        price = float(market_state.get("close", closes[-1]))
        # Position within band: 0 = mean, +1 = upper, -1 = lower.
        pos = (price - mean) / (2.0 * sd)
        pos = max(-1.5, min(1.5, pos))

        # Slow-TF trend direction for continuation calls.
        ef = _ema(slow, 5)
        es = _ema(slow, 20)
        slow_dir = 0
        if ef is not None and es is not None:
            slow_dir = 1 if ef > es else (-1 if ef < es else 0)

        abs_pos = abs(pos)
        NEAR_MEAN = 0.35
        NEAR_EXTREME = 0.85

        score = 0.0
        mode = "neutral"
        if abs_pos <= NEAR_MEAN and slow_dir != 0:
            # Continuation: trade with the slow-TF trend.
            score = slow_dir * (0.35 + 0.4 * (1.0 - abs_pos / NEAR_MEAN))
            mode = "continuation"
        elif abs_pos >= NEAR_EXTREME:
            # Reversion: fade the stretch back toward the mean.
            stretch = min(1.0, (abs_pos - NEAR_EXTREME) / (1.5 - NEAR_EXTREME))
            score = -1.0 * (1 if pos > 0 else -1) * (0.4 + 0.5 * stretch)
            mode = "reversion"
        else:
            return None

        score = max(-1.0, min(1.0, score))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=(
                f"{mode}: band_pos={pos:.2f} slow_dir={slow_dir} "
                f"mean={mean:.2f} sd={sd:.4f} -> score={score:.3f}"
            ),
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status, "cyclealign"],
            features={
                "band_pos": round(pos, 4),
                "slow_dir": slow_dir,
                "band_mean": round(mean, 4),
                "band_sd": round(sd, 6),
                "mode": mode,
            },
        )
