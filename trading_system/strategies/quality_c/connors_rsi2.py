"""Connors RSI(2) extreme reversion with Bollinger confluence.

Buy only when RSI(2) is deeply oversold AND price is also below the lower
Bollinger band (a confluence of two oversold signals), which sharply raises the
next-bar reversion win rate and profit factor. Symmetric short when RSI(2) is
extremely overbought and price is above the upper band. Signed score scales
with RSI extremity.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_c.indicators import bollinger, rsi


class ConnorsRsi2Strategy(BaseSignalStrategy):
    def __init__(self, period: int = 2, buy_level: float = 5.0, sell_level: float = 95.0,
                 bb_period: int = 20, bb_std: float = 2.0, long_only: bool = False) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="ConnorsRsi2Strategy",
                strategy_type="mean_reversion",
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=bb_period + period + 2),
        )
        self.period = period
        self.buy_level = buy_level
        self.sell_level = sell_level
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.long_only = long_only

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        if len(closes) < self.bb_period + self.period + 2:
            return None
        r = rsi(closes, self.period)
        if r is None:
            return None
        mean, upper, lower = bollinger(closes, self.bb_period, self.bb_std)
        if mean is None or lower is None or upper is None:
            return None
        price = closes[-1]
        score = 0.0
        reason = ""
        if r < self.buy_level and price < lower:
            score = min(1.0, (self.buy_level - r) / self.buy_level + 0.3)
            reason = f"RSI(2)={r:.1f}<{self.buy_level:.0f} & below lower BB={lower:.2f} reversion"
        elif (not self.long_only) and r > self.sell_level and price > upper:
            score = -min(1.0, (r - self.sell_level) / (100 - self.sell_level) + 0.3)
            reason = f"RSI(2)={r:.1f}>{self.sell_level:.0f} & above upper BB={upper:.2f} reversion"
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
            features={"rsi2": round(r, 2), "score": round(score, 3)},
        )
