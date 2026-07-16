"""
Bollinger Bandwidth Reversion Strategy (spot-vol / "implied-ish" band proxy).

Bollinger bandwidth (upper-lower)/middle acts as a volatility-band proxy akin to
an implied-volatility percentile. We trade band *contraction -> expansion*
(reversion to higher vol after a squeeze) and band *expansion -> contraction*
(reversion to calm after a blowout). The signal is the signed deviation of the
current bandwidth from its trailing median.
"""
from math import sqrt

from trading_system.strategies.base.interfaces import StrategySignal

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


def _sma(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _stdev(values: list[float], period: int) -> float | None:
    mean = _sma(values, period)
    if mean is None:
        return None
    var = sum((v - mean) ** 2 for v in values[-period:]) / period
    return sqrt(var)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


class BollingerBandwidthReversionStrategy(BaseSignalStrategy):
    """Trade Bollinger-bandwidth expansion/contraction reversion (vol proxy)."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="BollingerBandwidthReversionStrategy",
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
            config=StrategyConfig(threshold=0.2, cooldown_seconds=12, warmup_period=60),
        )
        self._period = 20
        self._mult = 2.0
        self._hist = 40

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if len(closes) < self._period + self._hist:
            return None

        mid = _sma(closes, self._period)
        sd = _stdev(closes, self._period)
        if mid is None or sd is None or mid <= 0:
            return None
        bandwidth = (2.0 * self._mult * sd) / mid

        hist: list[float] = []
        for i in range(self._hist):
            m = _sma(closes[: len(closes) - i], self._period)
            s = _stdev(closes[: len(closes) - i], self._period)
            if m and s and m > 0:
                hist.append((2.0 * self._mult * s) / m)
        med = _median(hist)
        if med is None or med <= 0:
            return None

        dev = (bandwidth - med) / med
        score = max(-1.0, min(1.0, dev * 3.0))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        regime = "expansion" if dev > 0 else "contraction"
        reason = (
            f"bandwidth={bandwidth:.4f} median={med:.4f} dev={dev:.2f} vol {regime}"
        )
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", self.metadata_model.products[0])),
            score=score,
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"bandwidth": bandwidth, "median": med, "dev": dev},
        )

    @staticmethod
    def _now() -> float:
        from time import monotonic

        return monotonic()
