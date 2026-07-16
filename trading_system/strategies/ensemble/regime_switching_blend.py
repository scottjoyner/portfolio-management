from __future__ import annotations

import math

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


def _sma(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2.0 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema


def _true_range(highs: list[float], lows: list[float], closes: list[float]) -> list[float]:
    tr = []
    for i in range(len(closes)):
        if i == 0:
            tr.append(highs[i] - lows[i] if highs and lows else 0.0)
        else:
            h = highs[i] if highs else closes[i]
            l = lows[i] if lows else closes[i]
            prev = closes[i - 1]
            tr.append(max(h - l, abs(h - prev), abs(l - prev)))
    return tr


class RegimeSwitchingBlendStrategy(BaseSignalStrategy):
    """Detects trend vs range via ADX proxy; blends momentum/reversion sub-signals."""

    def __init__(self, warmup_period: int = 30) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="RegimeSwitchingBlend",
                strategy_type="ensemble_regime",
                data_requirements=["product_id", "closes", "highs", "lows", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=25, warmup_period=warmup_period),
        )
        self.warmup_period = warmup_period

    def _adx_proxy(self, closes: list[float], highs: list[float], lows: list[float]) -> float:
        if len(closes) < 15:
            return 0.0
        tr = _true_range(highs, lows, closes)
        atr = _sma(tr[-14:])
        rng = max(closes) - min(closes)
        denom = rng + 1e-9
        return min(1.0, atr / denom)

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        volumes = market_state.get("volumes") or []
        if len(closes) < self.warmup_period:
            return None

        adx = self._adx_proxy(closes, highs, lows)
        trending = adx > 0.15

        if len(closes) >= 22:
            ema_fast = _ema(closes[-22:], 9)
            ema_slow = _ema(closes[-22:], 21)
            momentum = 1.0 if ema_fast > ema_slow else -1.0
        else:
            momentum = 0.0

        win = closes[-20:]
        mid = _sma(win)
        var = sum((c - mid) ** 2 for c in win) / len(win)
        sd = math.sqrt(var) + 1e-9
        reversion = max(-1.0, min(1.0, (mid - closes[-1]) / (2 * sd + 1e-9)))

        if trending:
            score = 0.75 * momentum + 0.25 * reversion
            regime = "trend"
        else:
            score = 0.75 * reversion + 0.25 * momentum
            regime = "range"

        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=max(-1.0, min(1.0, score)),
            reason=f"regime={regime} adx={adx:.3f} blended score={score:.3f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"regime": regime, "adx_proxy": round(adx, 4)},
        )
