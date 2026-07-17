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


def katz_fractal_dimension(series: list[float]) -> float | None:
    """Katz fractal dimension of a 1-D curve.

    D = log10(n) / (log10(n) + log10(d / L)) where
      L = total path length (sum of consecutive point distances),
      d = planar diameter (max distance from the first point),
      n = number of steps = L / mean_step.
    Values sit in ~[1, 2]: near 1 => smooth/directional (trending) curve,
    higher => jagged/rough (choppy/mean-reverting) curve.  The curve is built
    in a unit-normalised space so amplitude does not distort the dimension.
    """
    n_pts = len(series)
    if n_pts < 4:
        return None
    lo, hi = min(series), max(series)
    rng = hi - lo
    if rng <= 1e-12:
        return None
    # normalise amplitude to [0, 1]; x-axis normalised to [0, 1] across n_pts
    ys = [(v - lo) / rng for v in series]
    dx = 1.0 / (n_pts - 1)

    total_len = 0.0
    max_dist = 0.0
    for i in range(1, n_pts):
        total_len += math.hypot(dx, ys[i] - ys[i - 1])
    for i in range(1, n_pts):
        max_dist = max(max_dist, math.hypot(i * dx, ys[i] - ys[0]))
    if total_len <= 1e-12 or max_dist <= 1e-12:
        return None

    mean_step = total_len / (n_pts - 1)
    n = total_len / mean_step  # == n_pts - 1, but keeps Katz's formulation
    denom = math.log10(n) + math.log10(max_dist / total_len)
    if abs(denom) < 1e-12:
        return None
    d = math.log10(n) / denom
    return d


class KatzFractalBreakoutStrategy(BaseSignalStrategy):
    """Katz fractal-dimension breakout.

    Estimates the roughness of the recent price curve via the Katz fractal
    dimension (FD).  A *low* FD means the curve is smooth and directional -- a
    coherent trend -- so a fresh range breakout is likely to continue.  When
    FD is low AND price pokes above/below its recent Donchian channel we trade
    in the breakout direction; conviction scales with how smooth the curve is
    ((2 - FD) as a "directionality" score) times how far price has cleared the
    channel edge (in ATR-like units).  A rough (high-FD) curve suppresses the
    trade because breakouts in choppy tape are noise.
    """

    def __init__(self, window: int = 48, channel: int = 20) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="KatzFractalBreakout",
                strategy_type="regime_adaptive_fractal",
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.12, cooldown_seconds=25, warmup_period=window),
        )
        self.window = window
        self.channel = channel

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
        fd = katz_fractal_dimension(window)
        if fd is None:
            return None

        # directionality: near 1 for smooth trends, ~0 for very rough curves
        directionality = max(0.0, min(1.0, 2.0 - fd))

        # Donchian channel over the prior `channel` bars (excluding current bar)
        chan = window[-(self.channel + 1):-1]
        if len(chan) < self.channel:
            return None
        upper = max(chan)
        lower = min(chan)
        price = window[-1]

        # volatility unit for scaling breakout distance
        rets = [window[i] - window[i - 1] for i in range(1, len(window))]
        vol = _stdev(rets) + 1e-12

        if price > upper:
            clear = (price - upper) / vol
            direction = 1.0
        elif price < lower:
            clear = (lower - price) / vol
            direction = -1.0
        else:
            return None

        drive = min(1.0, clear / 2.0)
        score = max(-1.0, min(1.0, direction * directionality * drive))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=(
                f"katz FD={fd:.3f} directionality={directionality:.3f} "
                f"breakout clear={clear:.2f}sigma dir={'up' if direction > 0 else 'down'}"
            ),
            confidence=min(1.0, directionality * (0.4 + 0.6 * drive)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "katz_fd": round(fd, 4),
                "directionality": round(directionality, 4),
                "clear_sigma": round(clear, 4),
                "upper": round(upper, 6),
                "lower": round(lower, 6),
            },
        )
