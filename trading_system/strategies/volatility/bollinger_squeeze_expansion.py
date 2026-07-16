from time import monotonic

from trading_system.strategies.base.interfaces import StrategySignal

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


def _sma(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _stdev(values: list[float], mean: float) -> float:
    if not values:
        return 0.0
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return var ** 0.5


def _atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1 or len(highs) < period + 1 or len(lows) < period + 1:
        return None
    trs: list[float] = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


class BollingerSqueezeVolExpansionStrategy(BaseSignalStrategy):
    """Bollinger-band squeeze (low bandwidth) followed by band expansion breakout."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="BollingerSqueezeVolExpansionStrategy",
                strategy_type="volatility",
                live_supported=True,
                data_requirements=["product_id", "score", "close", "high", "low", "warmup_complete"],
                risk_mode_hint="AGGRESSIVE",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=5, warmup_period=40),
        )

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        close = market_state.get("close")
        if close is None and closes:
            close = closes[-1]

        if not closes or close is None:
            score = float(market_state.get("score", 0.0))
            if score <= self.config.threshold:
                return None
            self._last_emit_ts = monotonic()
            return self._mk_signal(score, 0.0, "fallback score path")

        # measure the squeeze on the window *prior* to the breakout bar
        if len(closes) < 21:
            return None
        prior = closes[-21:-1]
        mid = _sma(prior, 20)
        if mid is None:
            return None
        sd = _stdev(prior, mid)
        upper = mid + 2.0 * sd
        lower = mid - 2.0 * sd
        bandwidth = (upper - lower) / mid if mid else 0.0

        atr = _atr(highs, lows, closes, 14)
        if atr is None:
            return None

        # require a prior squeeze: bandwidth below 5% indicates compression
        squeezed = bandwidth < 0.05
        if not squeezed:
            return None

        if close > upper:
            score = min(1.0, (close - upper) / (atr + 1e-9) + 0.2)
            self._last_emit_ts = monotonic()
            return self._mk_signal(score, close, f"squeeze breakout up close {close:.2f} > upper {upper:.2f}")
        if close < lower:
            score = min(1.0, (lower - close) / (atr + 1e-9) + 0.2)
            self._last_emit_ts = monotonic()
            return self._mk_signal(score, close, f"squeeze breakout down close {close:.2f} < lower {lower:.2f}")
        return None

    def _mk_signal(self, score: float, close: float, reason: str) -> StrategySignal:
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(self.metadata_model.products[0]),
            score=score,
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=True,
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"close": close, "squeeze_expansion": True},
        )
