"""
Dynamic Time-Warping-ish Shape Match Reversal (stat_arb_2).

Compares the recent normalized price shape (z-scored window) to a fixed
historical "reversal template" using lightweight normalized Euclidean
distance (no full DTW). When the distance is minimal the pattern is
completing; we take a reversal position in the template's completion
direction. Pure-Python, windowed, deterministic.
"""
from __future__ import annotations

from math import sqrt

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata

from trading_system.strategies.base.interfaces import StrategySignal

_REVERSAL_TEMPLATE = [-0.6, -0.4, -0.2, 0.1, 0.5, 0.9, 0.7, 0.3]


def _zscore_window(series: list[float], n: int) -> list[float] | None:
    if len(series) < n:
        return None
    w = series[-n:]
    m = sum(w) / n
    var = sum((x - m) ** 2 for x in w) / n
    if var <= 1e-12:
        return None
    sd = sqrt(var)
    return [(x - m) / sd for x in w]


def _norm_euclid(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    a = a[-n:]
    b = b[-n:]
    return sqrt(sum((a[i] - b[i]) ** 2 for i in range(n)) / n)


class ShapeMatchReversal(BaseSignalStrategy):
    """Signal when recent normalized shape matches a reversal template."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="ShapeMatchReversal",
                strategy_type="stat_arb",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "score", "closes"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=20, warmup_period=20),
        )
        self._window = len(_REVERSAL_TEMPLATE)

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        shape = _zscore_window(closes, self._window)
        if shape is None:
            return None

        dist = _norm_euclid(shape, _REVERSAL_TEMPLATE)
        if dist > 0.35:
            return None

        completion_dir = 1.0 if _REVERSAL_TEMPLATE[-1] > _REVERSAL_TEMPLATE[0] else -1.0
        score = completion_dir * (1.0 - dist)
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        reason = f"shape_match dist={dist:.3f} template_dir={completion_dir:+.1f} completing"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", self.metadata_model.products[0])),
            score=max(-1.0, min(1.0, score)),
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"distance": dist, "template_dir": completion_dir},
        )

    @staticmethod
    def _now() -> float:
        from time import monotonic

        return monotonic()
