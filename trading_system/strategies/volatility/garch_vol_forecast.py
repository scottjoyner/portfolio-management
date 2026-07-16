"""
GARCH(1,1)-Lite Volatility Forecast & Mean-Reversion Strategy.

Maintains a running variance estimate via a simple GARCH(1,1) update
    sigma_t^2 = omega + alpha * r_{t-1}^2 + beta * sigma_{t-1}^2
with alpha + beta < 1. The next-period variance forecast is compared
against the most recently realized squared return. When the market
under-estimates (forecast > realized) or over-estimates (forecast <
realized) volatility relative to the latest return, we trade a
mean-reversion of volatility, biased by the sign of the most recent
return. Pure-Python, deterministic, stateful.
"""
from math import log, sqrt

from trading_system.strategies.base.interfaces import StrategySignal

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


def _log_return(closes: list[float], i: int) -> float | None:
    if i < 1 or i >= len(closes):
        return None
    prev, cur = closes[i - 1], closes[i]
    if prev > 0 and cur > 0:
        return log(cur / prev)
    return None


class GarchLiteVolForecastStrategy(BaseSignalStrategy):
    """GARCH(1,1)-lite vol forecast vs realized vol mean-reversion signal."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="GarchLiteVolForecastStrategy",
                strategy_type="volatility",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "score", "closes", "volumes"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.12, cooldown_seconds=15, warmup_period=40),
        )
        self._omega = 1e-6
        self._alpha = 0.10
        self._beta = 0.85
        self._var = 1e-6
        self._seeded = False
        self._last_return = 0.0

    def _seed(self, closes: list[float]) -> None:
        n = min(len(closes) - 1, 100)
        if n < 2:
            return
        rets = []
        for i in range(1, n + 1):
            r = _log_return(closes, len(closes) - n + i - 1)
            if r is not None:
                rets.append(r)
        if len(rets) < 2:
            return
        mean = sum(rets) / len(rets)
        self._var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        self._seeded = True

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if len(closes) < 41:
            return None

        if not self._seeded:
            self._seed(closes)
            if not self._seeded:
                return None

        r = _log_return(closes, len(closes) - 1)
        if r is None:
            return None

        realized = r * r
        forecast = self._omega + self._alpha * self._last_return ** 2 + self._beta * self._var

        self._var = max(1e-9, forecast)
        self._last_return = r

        if forecast <= 0:
            return None

        ratio = realized / forecast
        score = (ratio - 1.0) * 0.5
        if r < 0:
            score = -score

        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        reason = (
            f"garch_forecast_var={forecast:.2e} realized={realized:.2e} "
            f"ratio={ratio:.2f} ret={r:+.4f}"
        )
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", self.metadata_model.products[0])),
            score=max(-1.0, min(1.0, score)),
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "forecast_var": forecast,
                "realized_var": realized,
                "ratio": ratio,
                "last_return": r,
            },
        )

    @staticmethod
    def _now() -> float:
        from time import monotonic

        return monotonic()
