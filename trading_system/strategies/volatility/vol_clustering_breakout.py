"""
Volatility-Clustering Breakout Strategy.

Detects sustained high-vol clusters by measuring the lag-1
autocorrelation of absolute log-returns. A high autocorreation after a
calm (low-vol) regime implies a vol cluster is forming: trade the
direction of the latest return (breakout). When the autocorrelation
collapses (the vol cluster exhausts), fade the most recent move.

Pure-Python, deterministic, stateful. Warmup >= 40 bars.
"""
from math import log, sqrt

from trading_system.strategies.base.interfaces import StrategySignal

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


def _abs_returns(closes: list[float]) -> list[float]:
    out: list[float] = []
    for i in range(1, len(closes)):
        prev, cur = closes[i - 1], closes[i]
        if prev > 0 and cur > 0:
            out.append(abs(log(cur / prev)))
    return out


def _autocorr_lag1(x: list[float]) -> float | None:
    n = len(x)
    if n < 4:
        return None
    mean = sum(x) / n
    num = 0.0
    den = 0.0
    for i in range(n):
        d = x[i] - mean
        den += d * d
        if i > 0:
            num += (x[i] - mean) * (x[i - 1] - mean)
    if den <= 0:
        return None
    return num / den


class VolClusteringBreakoutStrategy(BaseSignalStrategy):
    """Breakout on vol-cluster formation; fade when the cluster exhausts."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VolClusteringBreakoutStrategy",
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
            config=StrategyConfig(threshold=0.10, cooldown_seconds=12, warmup_period=40),
        )
        self._window = 40
        self._high_ac = 0.35
        self._low_ac = 0.10

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if len(closes) < self._window + 1:
            return None

        absr = _abs_returns(closes)
        if len(absr) < self._window:
            return None

        recent = absr[-self._window:]
        ac = _autocorr_lag1(recent)
        if ac is None:
            return None

        r = None
        if len(closes) >= 2:
            prev, cur = closes[-2], closes[-1]
            if prev > 0 and cur > 0:
                r = log(cur / prev)
        if r is None:
            return None

        score = 0.0
        if ac >= self._high_ac:
            score = r * (ac - self._low_ac) * 5.0
            action = "breakout"
        elif ac <= self._low_ac:
            score = -r * (self._high_ac - ac) * 5.0
            action = "fade"
        else:
            return None

        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        reason = (
            f"absret_autocorr={ac:.3f} last_ret={r:+.4f} action={action}"
        )
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", self.metadata_model.products[0])),
            score=max(-1.0, min(1.0, score)),
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"autocorr": ac, "last_return": r, "action": action},
        )

    @staticmethod
    def _now() -> float:
        from time import monotonic

        return monotonic()
