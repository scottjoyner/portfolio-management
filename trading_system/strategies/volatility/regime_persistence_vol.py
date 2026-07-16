"""
Regime-Persistence (Markov-ish) Volatility Signal.

Estimates the probability that the current volatility regime (HIGH vs
LOW) persists using recent transition counts between regimes. When a
regime shows strong persistence (high stay probability), we trade with
that regime's bias: in a persistent HIGH-vol regime fade the latest
exaggerated move (mean-reversion); in a persistent LOW-vol regime take
the latest directional move (trend continuation). Pure-Python,
deterministic, stateful. Warmup >= 40 bars.
"""
from math import log, sqrt

from trading_system.strategies.base.interfaces import StrategySignal

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


def _returns(closes: list[float]) -> list[float]:
    out: list[float] = []
    for i in range(1, len(closes)):
        prev, cur = closes[i - 1], closes[i]
        if prev > 0 and cur > 0:
            out.append(log(cur / prev))
    return out


def _median(x: list[float]) -> float:
    if not x:
        return 0.0
    s = sorted(x)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


class RegimePersistenceVolStrategy(BaseSignalStrategy):
    """Markov-ish vol-regime persistence: trade with the dominant regime bias."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="RegimePersistenceVolStrategy",
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
            config=StrategyConfig(threshold=0.15, cooldown_seconds=12, warmup_period=40),
        )
        self._window = 40
        self._mem = 20
        self._last_regime: int | None = None
        self._high_stay = 0
        self._high_total = 0
        self._low_stay = 0
        self._low_total = 0
        self._threshold: float | None = None

    def _regime_for(self, ret: float) -> int:
        assert self._threshold is not None
        return 1 if abs(ret) >= self._threshold else 0

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if len(closes) < self._window + 1:
            return None

        rets = _returns(closes)
        if len(rets) < self._window:
            return None

        recent = rets[-self._window:]
        if self._threshold is None:
            self._threshold = _median([abs(r) for r in recent]) or 1e-4

        cur_regime = self._regime_for(recent[-1])

        if self._last_regime is not None:
            if self._last_regime == 1:
                self._high_total += 1
                if cur_regime == 1:
                    self._high_stay += 1
            else:
                self._low_total += 1
                if cur_regime == 0:
                    self._low_stay += 1
        self._last_regime = cur_regime

        if self._high_total < 5 or self._low_total < 5:
            return None

        p_high = self._high_stay / self._high_total
        p_low = self._low_stay / self._low_total

        score = 0.0
        if cur_regime == 1 and p_high >= 0.6:
            r = recent[-1]
            score = -r * p_high
            bias = "high_persist_fade"
        elif cur_regime == 0 and p_low >= 0.6:
            r = recent[-1]
            score = r * p_low
            bias = "low_persist_trend"
        else:
            return None

        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        reason = (
            f"regime={'HIGH' if cur_regime else 'LOW'} p_high={p_high:.2f} "
            f"p_low={p_low:.2f} bias={bias}"
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
                "p_high_stay": p_high,
                "p_low_stay": p_low,
                "regime": cur_regime,
                "bias": bias,
            },
        )

    @staticmethod
    def _now() -> float:
        from time import monotonic

        return monotonic()
