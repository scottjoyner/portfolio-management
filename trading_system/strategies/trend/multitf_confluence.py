"""
Multi-Timeframe Trend Confluence Strategy.

Emulates multiple timeframes by subsampling the single ``closes`` series
provided by the system (full = base TF, every 4th bar = "4h", every 12th
bar = "12h").  A strong directional signal is only emitted when the trend
direction agrees across all three horizons; partial agreement produces a
proportionally weaker score.

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


def _subsample(closes: list[float], step: int) -> list[float]:
    """Return every ``step``-th bar, keeping the most recent bar aligned."""
    if step <= 1:
        return list(closes)
    # Anchor on the latest bar so the newest price is always represented.
    rev = closes[::-1][::step]
    return rev[::-1]


def _trend_dir(series: list[float], fast: int = 5, slow: int = 15) -> tuple[int, float]:
    """Return (direction, strength[0..1]) using an EMA fast/slow separation."""
    ef = _ema(series, fast)
    es = _ema(series, slow)
    if ef is None or es is None or es == 0:
        return 0, 0.0
    sep = (ef - es) / abs(es)
    if sep > 0:
        return 1, min(1.0, abs(sep) * 20.0)
    if sep < 0:
        return -1, min(1.0, abs(sep) * 20.0)
    return 0, 0.0


class MultiTFTrendConfluenceStrategy(BaseSignalStrategy):
    """Aligns EMA trend direction across base / 4x / 12x subsampled horizons."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="MultiTFTrendConfluenceStrategy",
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

        # Base + derived timeframes. Prefer explicit series if supplied.
        base = closes
        tf4 = market_state.get("closes_4h") or _subsample(closes, 4)
        tf12 = market_state.get("closes_1d") or _subsample(closes, 12)
        if len(tf4) < 15 or len(tf12) < 8:
            return None

        d0, s0 = _trend_dir(base)
        d4, s4 = _trend_dir(tf4)
        # Slow horizon has fewer samples -> use shorter EMA windows.
        d12, s12 = _trend_dir(tf12, fast=3, slow=8)

        dirs = [d0, d4, d12]
        agree_up = sum(1 for d in dirs if d > 0)
        agree_dn = sum(1 for d in dirs if d < 0)

        # Net direction and how many horizons agree.
        if agree_up > agree_dn:
            direction = 1
            n_agree = agree_up
        elif agree_dn > agree_up:
            direction = -1
            n_agree = agree_dn
        else:
            return None

        avg_strength = (s0 + s4 + s12) / 3.0
        # Confluence factor: 1 horizon ~0.4, 2 ~0.7, all 3 = 1.0
        conf_factor = {1: 0.4, 2: 0.7, 3: 1.0}[n_agree]
        score = direction * conf_factor * (0.4 + 0.6 * avg_strength)
        score = max(-1.0, min(1.0, score))

        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        label = "bullish" if direction > 0 else "bearish"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=(
                f"{n_agree}/3 timeframes agree {label} "
                f"(base={d0} 4x={d4} 12x={d12}) strength={avg_strength:.2f}"
            ),
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status, "multitf"],
            features={
                "dir_base": d0,
                "dir_4x": d4,
                "dir_12x": d12,
                "n_agree": n_agree,
                "avg_strength": round(avg_strength, 4),
            },
        )
