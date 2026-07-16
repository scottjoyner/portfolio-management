"""
Vol-Term Structure Carry Strategy (spot-vol proxied).

Estimates realized vol over a short and a long window (std of log-returns).
When short-term vol compresses relative to long-term vol (vol carry /
mean-reversion of volatility), signal a reversion toward the long-run vol
regime. Positive score = vol compression (fade the calm); negative = vol
expansion (fade the spike) — both are mean-reversion of the vol surface.
"""
from math import log, sqrt

from trading_system.strategies.base.interfaces import StrategySignal

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


def _realized_vol(closes: list[float], window: int) -> float | None:
    if len(closes) < window + 1:
        return None
    rets: list[float] = []
    for i in range(1, window + 1):
        prev = closes[-(i + 1)]
        cur = closes[-i]
        if prev > 0 and cur > 0:
            rets.append(log(cur / prev))
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    return sqrt(var)


class VolTermStructureCarryStrategy(BaseSignalStrategy):
    """Vol carry: trade compression/expansion of short vs long realized vol."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VolTermStructureCarryStrategy",
                strategy_type="volatility",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "score", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.15, cooldown_seconds=10, warmup_period=60),
        )
        self._short_window = 10
        self._long_window = 50

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        short_vol = _realized_vol(closes, self._short_window)
        long_vol = _realized_vol(closes, self._long_window)
        if short_vol is None or long_vol is None or long_vol <= 0:
            return None

        ratio = short_vol / long_vol
        score = (ratio - 1.0) * 2.0
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        direction = "compression" if ratio < 1.0 else "expansion"
        reason = (
            f"short_vol={short_vol:.4f} long_vol={long_vol:.4f} ratio={ratio:.2f} "
            f"vol {direction}"
        )
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", self.metadata_model.products[0])),
            score=max(-1.0, min(1.0, score)),
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"short_vol": short_vol, "long_vol": long_vol, "ratio": ratio},
        )

    @staticmethod
    def _now() -> float:
        from time import monotonic

        return monotonic()
