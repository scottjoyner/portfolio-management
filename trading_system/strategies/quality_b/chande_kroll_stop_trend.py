"""Chande Kroll stop regime-adaptive trend-follow.

Chande Kroll stop bands define the trend envelope. In trending regimes we follow
the breakout (long when price holds above the long stop, short below the short
stop); in ranging regimes we fade the distance from the band (mean-reversion).
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_b.indicators import adapt_to_regime, atr, highest, lowest


class ChandeKrollStopTrend(BaseSignalStrategy):
    def __init__(self, period: int = 10, multiplier: float = 3.0, stop_period: int = 20, regime_period: int = 30, regime_thr: float = 0.3) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="ChandeKrollStopTrend",
                strategy_type="trend_follow",
                data_requirements=["product_id", "closes", "highs", "lows", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=stop_period + period + regime_period),
        )
        self.period = period
        self.multiplier = multiplier
        self.stop_period = stop_period
        self.regime_period = regime_period
        self.regime_thr = regime_thr

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        if len(closes) < self.stop_period + self.period + self.regime_period or len(highs) < len(closes) or len(lows) < len(closes):
            return None
        a = atr(highs, lows, closes, self.period)
        if a is None or a <= 0:
            return None
        hh = highest(highs, self.stop_period)
        ll = lowest(lows, self.stop_period)
        if hh is None or ll is None:
            return None
        long_stop = hh - self.multiplier * a
        short_stop = ll + self.multiplier * a
        price = closes[-1]
        denom = (long_stop - short_stop)
        if denom <= 0:
            return None
        follow = 0.0
        reason = ""
        if price >= long_stop:
            follow = min(1.0, (price - long_stop) / (denom + 1e-9) + 0.5)
            reason = f"price={price:.2f} >= long_stop={long_stop:.2f} follow-up"
        elif price <= short_stop:
            follow = -min(1.0, (short_stop - price) / (denom + 1e-9) + 0.5)
            reason = f"price={price:.2f} <= short_stop={short_stop:.2f} follow-down"
        else:
            return None
        score = adapt_to_regime(follow, closes, self.regime_period, self.regime_thr)
        score = max(-1.0, min(1.0, score))
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=reason + " regime-adaptive",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"long_stop": round(long_stop, 2), "short_stop": round(short_stop, 2), "atr": round(a, 2)},
        )
