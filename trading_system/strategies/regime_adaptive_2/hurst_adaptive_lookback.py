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


def _rescaled_range(series: list[float]) -> float | None:
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
    s = _stdev(series)
    if s <= 1e-12:
        return None
    return r / s


def hurst_exponent(returns: list[float]) -> float | None:
    """Rescaled-range (R/S) Hurst exponent estimate in (0, 1)."""
    n = len(returns)
    if n < 16:
        return None
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

    mx, my = _mean(xs), _mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    if den <= 1e-12:
        return None
    return max(0.0, min(1.0, num / den))


def _ema_period(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2.0 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1.0 - k)
    return ema


class HurstAdaptiveLookbackStrategy(BaseSignalStrategy):
    """Persistence-driven adaptive-lookback trend strategy.

    Rather than fixing an EMA lookback a priori, the strategy first measures
    the market's persistence via the Hurst exponent H and then *selects* the
    lookback window from it:

      * strongly persistent (H high)   -> use a LONG lookback (ride the trend);
      * near random-walk (H ~ 0.5)     -> use a MEDIUM lookback;
      * anti-persistent (H low)        -> use a SHORT, reactive lookback and
        invert the signal (fade the drift, since moves tend to revert).

    The chosen fast/slow EMA pair produces a normalised crossover score; H also
    gates conviction (a random walk yields little conviction).  This makes the
    indicator self-tuning across regimes -- a genuinely different construction
    from a static EMA-cross or a pure Hurst-regime classifier.
    """

    MIN_LB, MAX_LB = 6, 40

    def __init__(self, window: int = 80) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="HurstAdaptiveLookbackTrend",
                strategy_type="regime_adaptive_lookback",
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.12, cooldown_seconds=25, warmup_period=window),
        )
        self.window = window

    def _select_lookback(self, h: float) -> int:
        """Map H in (0, 1) to a slow-EMA lookback in [MIN_LB, MAX_LB]."""
        # H=0 -> MIN_LB, H=1 -> MAX_LB, linear interpolation
        lb = self.MIN_LB + (self.MAX_LB - self.MIN_LB) * h
        return int(max(self.MIN_LB, min(self.MAX_LB, round(lb))))

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
        rets = [
            math.log(window[i] / window[i - 1])
            for i in range(1, len(window))
            if window[i] > 0 and window[i - 1] > 0
        ]
        if len(rets) < 16:
            return None

        h = hurst_exponent(rets)
        if h is None:
            return None

        slow_lb = self._select_lookback(h)
        fast_lb = max(3, slow_lb // 3)

        span = window[-(slow_lb + 1):]
        if len(span) < slow_lb:
            return None
        ema_fast = _ema_period(span, fast_lb)
        ema_slow = _ema_period(span, slow_lb)
        diff = (ema_fast - ema_slow) / (abs(ema_slow) + 1e-9)

        raw = max(-1.0, min(1.0, diff * 60.0))

        persistence = (h - 0.5) * 2.0  # in [-1, 1]
        if persistence < 0:
            # anti-persistent: fade the crossover
            raw = -raw
            regime = "anti_persistent"
        elif persistence > 0:
            regime = "persistent"
        else:
            regime = "random_walk"

        conviction = abs(persistence)
        score = max(-1.0, min(1.0, raw * (0.25 + 0.75 * conviction)))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=(
                f"H={h:.3f} regime={regime} lookback={slow_lb}/{fast_lb} "
                f"ema_diff={diff:.4f}"
            ),
            confidence=min(1.0, conviction * (0.4 + 0.6 * min(1.0, abs(raw)))),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status, regime],
            features={
                "hurst": round(h, 4),
                "slow_lookback": slow_lb,
                "fast_lookback": fast_lb,
                "ema_diff": round(diff, 6),
                "persistence": round(persistence, 4),
            },
        )
