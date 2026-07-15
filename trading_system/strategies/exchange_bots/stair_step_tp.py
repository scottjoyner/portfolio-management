"""Stair-step take-profit exchange bot.

Wraps :class:`coinbase.src.rebalance_engine.StairStepEngine` (which delegates to
the Rust ``PyStairStepProfitTaker``) to skim a fraction of an existing LONG
position each time price crosses a grid take-profit level. The bot is purely
reactive: it only emits orders while a LONG position is open and is reset when
the position is closed.
"""
from __future__ import annotations

from typing import Optional

from pydantic import Field

from coinbase.src.rebalance_engine import StairStepEngine

from .base import BotConfig, ExchangeBotStrategy
from ..base.interfaces import StrategySignal


class StairStepTakeProfitConfig(BotConfig):
    """Configurable parameters for the stair-step take-profit strategy."""

    low: float = Field(default=0.0, description="Lower bound of the grid band")
    high: float = Field(default=1.0, description="Upper bound of the grid band")
    steps: int = Field(default=5, ge=2, description="Number of grid levels (>= 2)")
    take_profit_pct: float = Field(
        default=0.5, gt=0.0, description="Take-profit per step (0.5 == 0.5%)"
    )
    base_size_pct: float = Field(
        default=0.25,
        gt=0.0,
        le=1.0,
        description="Fraction of position sold per step",
    )
    trailing: bool = Field(default=False, description="Reset grid when position closes")
    drift_threshold: float = Field(default=0.0, description="Unused placeholder")


class StairStepTakeProfitStrategy(ExchangeBotStrategy):
    """Sell fractions of a LONG position across a stair-step grid."""

    config_model = StairStepTakeProfitConfig
    bot_config: StairStepTakeProfitConfig

    def __init__(
        self,
        strategy_id: str,
        strategy_type: str,
        bot_config: Optional[StairStepTakeProfitConfig] = None,
        metadata=None,
        config=None,
    ) -> None:
        super().__init__(
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            bot_config=bot_config or self.config_model(),
            metadata=metadata,
            config=config,
        )
        self._engine = StairStepEngine()
        self._registered: set[str] = set()

    # ------------------------------------------------------------------ signal
    def generate_signal(self, market_state: dict) -> Optional[StrategySignal]:
        if not market_state.get("warmup_complete", True):
            return None
        if self.is_disabled(market_state)[0]:
            return None

        product_id = str(market_state.get("product_id", "BTC-USD"))
        if not self._has_position(market_state, "LONG"):
            self._registered.discard(product_id)
            if self.bot_config.trailing:
                self._engine.reset(product_id)
            return None

        cfg = self.bot_config
        price = self._price(market_state)
        position = market_state["position"]

        if product_id not in self._registered:
            budget = float(position.get("size", 0.0) or 0.0) * price
            self._engine.add_symbol(
                product_id,
                cfg.low,
                cfg.high,
                cfg.steps,
                budget,
                cfg.take_profit_pct,
                cfg.base_size_pct,
            )
            self._registered.add(product_id)

        order = self._engine.on_price(product_id, price)
        if order is None:
            return None

        size = order.notional / price if price else 0.0
        self._record(product_id, order.side, order.price, size)
        return self._emit(
            order.side,
            f"stair-step {order.side} @ {order.price}",
            market_state,
        )
