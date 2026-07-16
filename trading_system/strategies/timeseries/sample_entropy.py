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


def sample_entropy(series: list[float], m: int = 2, r_factor: float = 0.2) -> float | None:
    """Pure-Python Sample Entropy (SampEn) of a time series.

    Counts template matches of length m and m+1 within tolerance r = r_factor *
    std(series) using the Chebyshev (max-norm) distance, then returns
    -ln(A / B).  Self-matches are excluded (the SampEn definition), which makes
    the estimator bias-free relative to Approximate Entropy.

    Low entropy => regular / predictable (trend-friendly).
    High entropy => complex / noisy (random-walk-like).
    """
    n = len(series)
    if n < m + 2:
        return None

    sd = _stdev(series)
    if sd <= 1e-12:
        return 0.0  # perfectly regular
    r = r_factor * sd

    def _count(mm: int) -> int:
        templates = [series[i:i + mm] for i in range(n - mm + 1)]
        count = 0
        total = len(templates)
        for i in range(total):
            ti = templates[i]
            for j in range(i + 1, total):
                tj = templates[j]
                # Chebyshev distance
                dist = 0.0
                for a, b in zip(ti, tj):
                    d = abs(a - b)
                    if d > dist:
                        dist = d
                    if dist > r:
                        break
                if dist <= r:
                    count += 1
        return count  # unordered pairs

    b = _count(m)
    a = _count(m + 1)
    if b == 0 or a == 0:
        return None
    return -math.log(a / b)


class SampleEntropyRegimeStrategy(BaseSignalStrategy):
    """Time-series complexity regime via Sample Entropy of returns.

    Compares the current SampEn against a rolling baseline to detect regime
    transitions:
      * Low / falling entropy => returns are becoming regular => a persistent
        trend is forming => trade in the direction of the recent drift.
      * High / rising entropy => returns look like noise => stay flat (no
        signal emitted).
    """

    def __init__(self, window: int = 60, m: int = 2, r_factor: float = 0.2) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="SampleEntropyComplexityRegimeStrategy",
                strategy_type="timeseries_complexity",
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.15, cooldown_seconds=30, warmup_period=window),
        )
        self.window = window
        self.m = m
        self.r_factor = r_factor

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

        entropy = sample_entropy(returns, m=self.m, r_factor=self.r_factor)
        if entropy is None:
            return None

        # Map entropy to a regularity score in [0, 1].
        # SampEn typically ranges ~0 (regular) to ~2+ (noisy); use a soft cap.
        regularity = max(0.0, min(1.0, 1.0 - entropy / 1.5))

        look = min(10, len(returns))
        drift = sum(returns[-look:])
        rstd = _stdev(returns) + 1e-12
        norm_drift = drift / (rstd * math.sqrt(look))
        if abs(norm_drift) < 1e-9:
            return None

        direction = 1.0 if norm_drift > 0 else -1.0
        score = direction * regularity * min(1.0, abs(norm_drift))
        score = max(-1.0, min(1.0, score))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=(
                f"SampEn={entropy:.3f} regularity={regularity:.3f} "
                f"drift={norm_drift:.3f} (low-complexity trend)"
            ),
            confidence=min(1.0, regularity),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "sample_entropy": round(entropy, 4),
                "regularity": round(regularity, 4),
                "norm_drift": round(norm_drift, 4),
            },
        )
