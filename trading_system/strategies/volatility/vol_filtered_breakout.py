"""
Vol-Filtered Breakout Strategy (spot-vol proxied).

A channel breakout (Donchian-style) signal that is only emitted when realized
vol is elevated above its own trailing median — i.e. we only trade breakouts
that occur under genuinely active (high-vol) regimes. In abnormally low-vol
regimes breakouts are faded instead. This gates the classic breakout on a vol
regime filter, avoiding chopped-out false breakouts in quiet markets.
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


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


class VolFilteredBreakoutStrategy(BaseSignalStrategy):
    """Breakout traded only when realized vol is elevated; fade when too quiet."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VolFilteredBreakoutStrategy",
                strategy_type="volatility",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "score", "closes", "highs", "lows", "warmup_complete"],
                risk_mode_hint="AGGRESSIVE",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=8, warmup_period=50),
        )
        self._vol_window = 30
        self._channel = 20

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        if len(closes) < self._channel + self._vol_window:
            return None
        close = closes[-1]
        if not highs or not lows:
            return None

        upper = max(highs[-self._channel:])
        lower = min(lows[-self._channel:])

        # trailing realized vol vs its own median
        vols: list[float] = []
        for w in range(1, self._vol_window + 1):
            v = _realized_vol(closes[: len(closes) - w + 1], 5)
            if v is not None:
                vols.append(v)
        med = _median(vols)
        cur_vol = _realized_vol(closes, 5)
        if med is None or cur_vol is None or med <= 0:
            return None

        vol_ratio = cur_vol / med
        if close > upper:
            if vol_ratio >= 1.0:
                score = min(1.0, (close - upper) / (upper * 0.01 + 1e-9))
                self._last_emit_ts = self._now()
                return self._mk(score, close, f"high-vol breakout above {upper:.2f} (vol_ratio={vol_ratio:.2f})")
            return None
        if close < lower:
            if vol_ratio >= 1.0:
                score = min(1.0, (lower - close) / (lower * 0.01 + 1e-9))
                self._last_emit_ts = self._now()
                return self._mk(score, close, f"high-vol breakdown below {lower:.2f} (vol_ratio={vol_ratio:.2f})")
            return None
        # abnormally low vol -> fade mean proximity to range extremes
        if vol_ratio < 0.6:
            mid = (upper + lower) / 2.0
            dist = (close - mid) / ((upper - lower) / 2.0 + 1e-9)
            score = max(-1.0, min(1.0, -dist * 0.5))
            if abs(score) <= self.config.threshold:
                return None
            self._last_emit_ts = self._now()
            return self._mk(score, close, f"low-vol fade toward mean (vol_ratio={vol_ratio:.2f})")
        return None

    def _mk(self, score: float, close: float, reason: str) -> StrategySignal:
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(self.metadata_model.products[0]),
            score=score,
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=True,
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"close": close, "vol_filtered": True},
        )

    @staticmethod
    def _now() -> float:
        from time import monotonic

        return monotonic()
