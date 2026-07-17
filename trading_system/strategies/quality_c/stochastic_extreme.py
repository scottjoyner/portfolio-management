"""Stochastic(14,3) extreme reversion.

Classic slow stochastic: %K(14) smoothed by %D(3). Buy when the stochastic
crosses below 15 (oversold), sell when above 85 (overbought). The score scales
with how deep into the extreme zone the reading is.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_c.indicators import stochastic


class StochasticExtremeReversionStrategy(BaseSignalStrategy):
    def __init__(self, k_period: int = 5, d_period: int = 3, buy_level: float = 15.0, sell_level: float = 85.0) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="StochasticExtremeReversionStrategy",
                strategy_type="mean_reversion",
                data_requirements=["product_id", "closes", "highs", "lows", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=k_period + d_period + 1),
        )
        self.k_period = k_period
        self.d_period = d_period
        self.buy_level = buy_level
        self.sell_level = sell_level

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        if len(closes) < self.k_period + self.d_period:
            return None
        k, d = stochastic(closes, highs, lows, self.k_period, self.d_period)
        if k is None or d is None:
            return None
        score = 0.0
        reason = ""
        if d < self.buy_level:
            score = min(1.0, (self.buy_level - d) / self.buy_level + 0.3)
            reason = f"slow%D={d:.1f} < {self.buy_level:.0f} oversold reversion"
        elif d > self.sell_level:
            score = -min(1.0, (d - self.sell_level) / (100 - self.sell_level) + 0.3)
            reason = f"slow%D={d:.1f} > {self.sell_level:.0f} overbought reversion"
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
            features={"stoch_k": round(k, 2), "stoch_d": round(d, 2)},
        )
