from __future__ import annotations

import math
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _stdev(xs: list[float], mu: float | None = None) -> float:
    if len(xs) < 2:
        return 0.0
    m = _mean(xs) if mu is None else mu
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def _rescaled_range(series: list[float]) -> float | None:
    """Compute R/S for a single (log-return) sub-series."""
    n = len(series)
    if n < 4:
        return None
    mu = _mean(series)
    dev = 0.0
    cum: list[float] = []
    for x in series:
        dev += x - mu
        cum.append(dev)
    r = max(cum) - min(cum)
    s = _stdev(series, mu)
    if s <= 1e-12:
        return None
    return r / s


def hurst_exponent(returns: list[float]) -> float | None:
    """Estimate the Hurst exponent via rescaled-range (R/S) analysis.

    Splits the return series into non-overlapping chunks of several sizes and
    fits log(R/S) vs log(chunk_size) with an ordinary least-squares line; the
    slope is the Hurst exponent H in (0, 1).
    """
    n = len(returns)
    if n < 16:
        return None

    # Candidate chunk sizes (powers-of-two-ish) that divide the series.
    sizes: list[int] = []
    k = 8
    while k <= n // 2:
        sizes.append(k)
        k *= 2
    if len(sizes) < 2:
        return None

    xs: list[float] = []
    ys: list[float] = []
    for size in sizes:
        n_chunks = n // size
        rs_vals: list[float] = []
        for c in range(n_chunks):
            chunk = returns[c * size:(c + 1) * size]
            rs = _rescaled_range(chunk)
            if rs is not None and rs > 0:
                rs_vals.append(rs)
        if rs_vals:
            xs.append(math.log(size))
            ys.append(math.log(_mean(rs_vals)))

    if len(xs) < 2:
        return None

    mx = _mean(xs)
    my = _mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    if den <= 1e-12:
        return None
    h = num / den
    return max(0.0, min(1.0, h))


class HurstRegimeStrategy(BaseSignalStrategy):
    """Regime detection via the Hurst exponent (rescaled-range analysis).

    H > 0.5 => persistent / trending regime => align with recent drift
    (momentum bias).  H < 0.5 => anti-persistent / mean-reverting regime =>
    fade recent drift (reversion bias).  The magnitude of |H - 0.5| scales the
    conviction so a random-walk (H ~ 0.5) produces no trade.
    """

    def __init__(self, window: int = 64) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="HurstRegimeRescaledRangeStrategy",
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

        h = hurst_exponent(returns)
        if h is None:
            return None

        # recent normalized drift over the last ~10 bars
        look = min(10, len(returns))
        drift = sum(returns[-look:])
        rstd = _stdev(returns) + 1e-12
        norm_drift = drift / (rstd * math.sqrt(look))
        if abs(norm_drift) < 1e-9:
            return None

        regime_strength = (h - 0.5) * 2.0  # in [-1, 1]
        direction = 1.0 if norm_drift > 0 else -1.0

        if regime_strength >= 0:
            # trending: momentum -> align with drift
            score = direction * abs(regime_strength) * min(1.0, abs(norm_drift))
            regime = "trending"
        else:
            # mean-reverting: fade the drift
            score = -direction * abs(regime_strength) * min(1.0, abs(norm_drift))
            regime = "mean_reverting"

        score = max(-1.0, min(1.0, score))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"Hurst H={h:.3f} regime={regime} drift={norm_drift:.3f}",
            confidence=min(1.0, abs(regime_strength)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status, regime],
            features={
                "hurst": round(h, 4),
                "regime_strength": round(regime_strength, 4),
                "norm_drift": round(norm_drift, 4),
            },
        )
