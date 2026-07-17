"""
Fractional-Cointegration (ML-lite) Stat-Arb (stat_arb_2).

Estimates long-run relation price ≈ a*peer + b via rolling OLS normal
equations (pure-Python), then trades the residual z-score (pairs-like
but fractional hedge ratio). Needs `peer_closes`. Returns None when
`peer_closes` absent.
"""
from __future__ import annotations

from math import sqrt

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata

from trading_system.strategies.base.interfaces import StrategySignal


def _rolling_ols(y: list[float], x: list[float]) -> tuple[float, float] | None:
    n = min(len(y), len(x))
    if n < 5:
        return None
    y = y[-n:]
    x = x[-n:]
    sx = sum(x)
    sy = sum(y)
    sxx = sum(v * v for v in x)
    sxy = sum(x[i] * y[i] for i in range(n))
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-12:
        return None
    a = (n * sxy - sx * sy) / denom
    b = (sy - a * sx) / n
    return a, b


class FractionalCointegrationArb(BaseSignalStrategy):
    """Rolling OLS hedge residual z-score pairs-style reversion."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="FractionalCointegrationArb",
                strategy_type="stat_arb",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "score", "closes", "peer_closes"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=25, warmup_period=50),
        )
        self._window = 50

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        peer = market_state.get("peer_closes")
        if peer is None:
            return None
        if len(closes) < self._window or len(peer) < self._window:
            return None

        fit = _rolling_ols(closes[-self._window:], peer[-self._window:])
        if fit is None:
            return None
        a, b = fit

        n = self._window
        resid = [closes[-n + i] - (a * peer[-n + i] + b) for i in range(n)]
        m = sum(resid) / n
        var = sum((r - m) ** 2 for r in resid) / n
        if var <= 1e-12:
            return None
        z = (resid[-1] - m) / sqrt(var)
        score = -z * 0.5
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        reason = f"residual_z={z:.3f} hedge_a={a:.4f} mean={m:.4f} reverting"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", self.metadata_model.products[0])),
            score=max(-1.0, min(1.0, score)),
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"zscore": z, "hedge_a": a, "hedge_b": b, "resid_mean": m},
        )

    @staticmethod
    def _now() -> float:
        from time import monotonic

        return monotonic()
