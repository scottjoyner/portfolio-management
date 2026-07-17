"""Price-channel breakout-pullback (failed-breakout fade).

Detect a Donchian-style breakout of the recent high/low channel: when the prior
bar closed beyond the channel but the current bar snaps back inside, the
breakout has FAILED and price tends to revert. Fade it — short the failed upside
breakout, long the failed downside breakout. Score scales with penetration depth.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class PriceChannelBreakoutPullbackStrategy(BaseSignalStrategy):
    def __init__(self, period: int = 20) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="PriceChannelBreakoutPullbackStrategy",
                strategy_type="mean_reversion",
                data_requirements=["product_id", "closes", "highs", "lows", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period + 3),
        )
        self.period = period

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        if len(closes) < self.period + 3 or len(highs) < self.period + 3 or len(lows) < self.period + 3:
            return None
        # channel of bars strictly before the prior close (so the prior close
        # can legitimately exceed it to constitute a breakout)
        ch_high = max(highs[-(self.period + 2): -2])
        ch_low = min(lows[-(self.period + 2): -2])
        price = closes[-1]
        prev = closes[-2]
        score = 0.0
        reason = ""
        # failed upside breakout -> fade short
        if prev > ch_high and price <= ch_high:
            pen = (prev - ch_high) / (ch_high + 1e-9)
            score = -min(1.0, 0.4 + pen * 5.0)
            reason = f"failed breakout prev={prev:.2f}>{ch_high:.2f}, close={price:.2f} fade-down"
        # failed downside breakout -> fade long
        elif prev < ch_low and price >= ch_low:
            pen = (ch_low - prev) / (ch_low + 1e-9)
            score = min(1.0, 0.4 + pen * 5.0)
            reason = f"failed breakout prev={prev:.2f}<{ch_low:.2f}, close={price:.2f} fade-up"
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"ch_high": round(ch_high, 2), "ch_low": round(ch_low, 2)},
        )
