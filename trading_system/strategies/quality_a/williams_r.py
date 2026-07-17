"""Williams %R (W%) momentum reversal.

%R bounded in [-100, 0]. Oversold below -80, overbought above -20. We emit a
continuous mean-reversion signal: heavily oversold -> long (expect bounce),
heavily overbought -> short (expect fade). Magnitude scales with how deep
into the band the reading is.
"""
from __future__ import annotations

import math
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class WilliamsPctRStrategy(BaseSignalStrategy):
    """Williams %R bounded-momentum reversal strategy."""

    def __init__(self, period: int = 14, oversold: float = -80.0, overbought: float = -20.0) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="WilliamsPctRStrategy",
                strategy_type="momentum",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "closes", "highs", "lows"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period + 2),
        )
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    def _wr(self, closes: list[float], highs: list[float], lows: list[float], i: int) -> float | None:
        if i < self.period or i >= len(closes):
            return None
        hi = max(highs[i - self.period:i + 1])
        lo = min(lows[i - self.period:i + 1])
        if hi - lo <= 1e-12:
            return -50.0
        return (hi - closes[i]) / (hi - lo) * -100.0

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        n = len(closes)
        if n < self.config.warmup_period + 1 or len(highs) < n or len(lows) < n:
            return None

        wr = self._wr(closes, highs, lows, n - 1)
        if wr is None:
            return None

        if wr <= self.oversold:
            score = -min(1.0, (self.oversold - wr) / 40.0 + 0.2)
        elif wr >= self.overbought:
            score = min(1.0, (wr - self.overbought) / 40.0 + 0.2)
        else:
            return None
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=max(-1.0, min(1.0, score)),
            reason=f"%R={wr:.1f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"pct_r": round(wr, 2)},
        )
