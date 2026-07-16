from __future__ import annotations

import math
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _stdev(xs: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def _detrended_rms(segment: list[float]) -> float:
    """Root-mean-square residual after removing a linear trend from a segment."""
    n = len(segment)
    if n < 2:
        return 0.0
    xs = list(range(n))
    mx = _mean(xs)
    my = _mean(segment)
    den = sum((x - mx) ** 2 for x in xs)
    if den <= 1e-12:
        slope, intercept = 0.0, my
    else:
        slope = sum((xs[i] - mx) * (segment[i] - my) for i in range(n)) / den
        intercept = my - slope * mx
    sse = 0.0
    for i in range(n):
        fit = slope * xs[i] + intercept
        sse += (segment[i] - fit) ** 2
    return math.sqrt(sse / n)


def dfa_alpha(returns: list[float]) -> float | None:
    """Simplified Detrended Fluctuation Analysis scaling exponent (alpha).

    1. Build the cumulative-sum profile of the mean-centred series.
    2. For a small set of box sizes, split the profile into non-overlapping
       boxes, remove a linear trend from each, and average the RMS residual.
    3. The slope of log F(n) vs log n is the DFA exponent alpha.

    alpha ~ 0.5 => white-noise / random.  alpha < 0.5 => anti-persistent
    (mean-reverting).  alpha > 0.5 => persistent (trending).
    """
    n = len(returns)
    if n < 16:
        return None

    mu = _mean(returns)
    profile: list[float] = []
    acc = 0.0
    for r in returns:
        acc += r - mu
        profile.append(acc)

    # simplified box sizes 4..n//2 in a small geometric ladder
    sizes: list[int] = []
    s = 4
    while s <= n // 2:
        sizes.append(s)
        s *= 2
    if len(sizes) < 2:
        return None

    xs: list[float] = []
    ys: list[float] = []
    for box in sizes:
        n_boxes = len(profile) // box
        if n_boxes < 1:
            continue
        rms_vals: list[float] = []
        for b in range(n_boxes):
            seg = profile[b * box:(b + 1) * box]
            rms_vals.append(_detrended_rms(seg))
        f = _mean(rms_vals)
        if f > 1e-12:
            xs.append(math.log(box))
            ys.append(math.log(f))

    if len(xs) < 2:
        return None

    mx = _mean(xs)
    my = _mean(ys)
    den = sum((x - mx) ** 2 for x in xs)
    if den <= 1e-12:
        return None
    alpha = sum((xs[i] - mx) * (ys[i] - my) for i in range(len(xs))) / den
    return max(0.0, min(2.0, alpha))


class DFAAlphaRegimeStrategy(BaseSignalStrategy):
    """Detrended Fluctuation Analysis (DFA) scaling-exponent regime strategy.

    alpha > 0.5 => persistent (trend-follow the recent drift).
    alpha < 0.5 => anti-persistent (fade the recent drift).
    |alpha - 0.5| sets conviction, so a random walk emits nothing.
    """

    def __init__(self, window: int = 64) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="DFAAlphaRegimeStrategy",
                strategy_type="timeseries_regime",
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.15, cooldown_seconds=30, warmup_period=window),
        )
        self.window = window

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if len(closes) < self.window:
            return None

        window = closes[-self.window:]
        returns = [
            math.log(window[i] / window[i - 1])
            for i in range(1, len(window))
            if window[i] > 0 and window[i - 1] > 0
        ]
        if len(returns) < self.window - 2:
            return None

        alpha = dfa_alpha(returns)
        if alpha is None:
            return None

        look = min(10, len(returns))
        drift = sum(returns[-look:])
        rstd = _stdev(returns) + 1e-12
        norm_drift = drift / (rstd * math.sqrt(look))
        if abs(norm_drift) < 1e-9:
            return None

        regime_strength = max(-1.0, min(1.0, (alpha - 0.5) * 2.0))
        direction = 1.0 if norm_drift > 0 else -1.0

        if regime_strength >= 0:
            score = direction * abs(regime_strength) * min(1.0, abs(norm_drift))
            regime = "persistent"
        else:
            score = -direction * abs(regime_strength) * min(1.0, abs(norm_drift))
            regime = "anti_persistent"

        score = max(-1.0, min(1.0, score))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"DFA alpha={alpha:.3f} regime={regime} drift={norm_drift:.3f}",
            confidence=min(1.0, abs(regime_strength)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status, regime],
            features={
                "dfa_alpha": round(alpha, 4),
                "regime_strength": round(regime_strength, 4),
                "norm_drift": round(norm_drift, 4),
            },
        )
