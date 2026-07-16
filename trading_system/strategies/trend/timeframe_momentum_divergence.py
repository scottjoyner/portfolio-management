"""
Timeframe Momentum-Divergence Strategy.

Compares a fast (base) timeframe momentum state against a slow (12x
subsampled) timeframe trend derived from the single ``closes`` series.

- Fast TF overbought/oversold WHILE slow TF trends the opposite/aligned way:
    * Fast overbought + slow uptrend  -> fade the fast exhaustion (mean-revert
      short-term, small counter score) -- classic pullback-into-trend setup is
      handled by alignment; pure exhaustion against no slow trend -> fade.
- Fast momentum aligned with slow trend -> trend-follow (stronger score).

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


def _rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        chg = values[i] - values[i - 1]
        if chg >= 0:
            gains += chg
        else:
            losses -= chg
    avg_gain = gains / period
    avg_loss = losses / period
    for i in range(period + 1, len(values)):
        chg = values[i] - values[i - 1]
        gain = chg if chg > 0 else 0.0
        loss = -chg if chg < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _subsample(closes: list[float], step: int) -> list[float]:
    if step <= 1:
        return list(closes)
    rev = closes[::-1][::step]
    return rev[::-1]


def _slow_trend(series: list[float]) -> int:
    ef = _ema(series, 5)
    es = _ema(series, 20)
    if ef is None or es is None:
        return 0
    if ef > es:
        return 1
    if ef < es:
        return -1
    return 0


class TimeframeMomentumDivergenceStrategy(BaseSignalStrategy):
    """Fades fast-TF exhaustion vs slow-TF trend; trend-follows when aligned."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="TimeframeMomentumDivergenceStrategy",
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

        fast_rsi = _rsi(closes, 14)
        if fast_rsi is None:
            return None
        slow_dir = _slow_trend(slow)

        overbought = fast_rsi >= 70.0
        oversold = fast_rsi <= 30.0

        score = 0.0
        mode = "neutral"
        if overbought and slow_dir <= 0:
            # Fast exhaustion, slow not supporting -> fade (mean-reversion short).
            score = -0.4 - 0.5 * ((fast_rsi - 70.0) / 30.0)
            mode = "fade-overbought"
        elif oversold and slow_dir >= 0:
            # Fast washout, slow not falling -> fade up (mean-reversion long).
            score = 0.4 + 0.5 * ((30.0 - fast_rsi) / 30.0)
            mode = "fade-oversold"
        elif slow_dir > 0 and 45.0 <= fast_rsi < 70.0:
            # Aligned bullish momentum -> trend-follow long.
            score = 0.3 + 0.4 * ((fast_rsi - 45.0) / 25.0)
            mode = "trend-follow-long"
        elif slow_dir < 0 and 30.0 < fast_rsi <= 55.0:
            # Aligned bearish momentum -> trend-follow short.
            score = -0.3 - 0.4 * ((55.0 - fast_rsi) / 25.0)
            mode = "trend-follow-short"
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
                f"{mode}: fast_rsi={fast_rsi:.1f} slow_dir={slow_dir} "
                f"-> score={score:.3f}"
            ),
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status, "multitf"],
            features={
                "fast_rsi": round(fast_rsi, 3),
                "slow_dir": slow_dir,
                "mode": mode,
            },
        )
