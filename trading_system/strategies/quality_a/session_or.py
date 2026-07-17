"""Session opening-range breakout.

Treats the first N bars of a rolling window as the "session open range"
and trades a breakout of that range by the current bar. Direction follows
the breakout side; magnitude scales with how far price has cleared the
range relative to the range size. Continuous when price is outside range.
"""
from __future__ import annotations

import math
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class SessionOpeningRangeBreakoutStrategy(BaseSignalStrategy):
    """First-N-bar opening-range breakout on closes/highs/lows."""

    def __init__(self, open_bars: int = 12, lookback: int = 10) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="SessionOpeningRangeBreakoutStrategy",
                strategy_type="volatility",
                status="implemented",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                products=["BTC-USD"],
                data_requirements=["product_id", "closes", "highs", "lows"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=open_bars + 2),
        )
        self.open_bars = open_bars
        self.lookback = lookback

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

        start = max(0, n - self.open_bars - self.lookback)
        if n - start < self.open_bars + 1:
            return None
        rng_hi = max(highs[start:start + self.open_bars])
        rng_lo = min(lows[start:start + self.open_bars])
        rng = rng_hi - rng_lo
        if rng <= 1e-9:
            return None

        price = closes[-1]
        if price > rng_hi:
            score = min(1.0, (price - rng_hi) / rng + 0.1)
        elif price < rng_lo:
            score = -min(1.0, (rng_lo - price) / rng + 0.1)
        else:
            return None
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=max(-1.0, min(1.0, score)),
            reason=f"price={price:.2f} range=[{rng_lo:.2f},{rng_hi:.2f}]",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"range_hi": round(rng_hi, 4), "range_lo": round(rng_lo, 4)},
        )
