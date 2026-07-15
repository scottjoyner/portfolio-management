"""Spot Martingale strategy (KuCoin Infinity Grid / Binance Martingale style).

This exchange-bot template layers fixed grid lines between a ``lower`` and
``upper`` price. As the price drops through an unfilled grid line the bot buys
an increasing quantity (``size = initial_size * multiplier ** layer``) to average
down. As the price rises back through a previously filled line it sells that
layer's quantity back. Each product is tracked independently.

The strategy is stateful: filled grid indices and the current layer are kept
per product in ``self._filled`` and ``self._layer``.
"""
from __future__ import annotations

from typing import Optional

from pydantic import Field, model_validator

from ..base.interfaces import StrategySignal
from .base import BotConfig, ExchangeBotStrategy


class SpotMartingaleConfig(BotConfig):
    """Configuration for :class:`SpotMartingaleStrategy`."""

    lower: float = Field(default=100.0, gt=0.0)
    upper: float = Field(default=200.0, gt=0.0)
    grids: int = Field(default=10, ge=2)
    initial_size: float = Field(default=0.01, gt=0.0)
    multiplier: float = Field(default=1.5, gt=0.0)
    max_layers: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _check_bounds(self) -> "SpotMartingaleConfig":
        if self.lower >= self.upper:
            raise ValueError("lower must be strictly less than upper")
        return self


class SpotMartingaleStrategy(ExchangeBotStrategy):
    """Martingale / infinity-grid spot strategy."""

    config_model = SpotMartingaleConfig

    def __init__(
        self,
        strategy_id: str = "spot_martingale",
        strategy_type: str = "exchange_bot",
        bot_config: Optional[SpotMartingaleConfig] = None,
        metadata=None,
        config=None,
    ) -> None:
        super().__init__(strategy_id, strategy_type, bot_config, metadata, config)
        self._lines: list[float] = self._build_lines()
        self._filled: dict[str, set[int]] = {}
        self._layer: dict[str, int] = {}

    # ------------------------------------------------------------------ helpers
    def _build_lines(self) -> list[float]:
        cfg = self.bot_config
        step = (cfg.upper - cfg.lower) / (cfg.grids - 1)
        # Index 0 is the top (upper); the last index is the bottom (lower).
        return [cfg.upper - i * step for i in range(cfg.grids)]

    def _ensure(self, product_id: str) -> None:
        if product_id not in self._filled:
            self._filled[product_id] = set()
        if product_id not in self._layer:
            self._layer[product_id] = 0

    def _size(self, layer: int) -> float:
        return self.bot_config.initial_size * (self.bot_config.multiplier ** layer)

    # ------------------------------------------------------------------ signal
    def generate_signal(self, market_state: dict) -> Optional[StrategySignal]:
        if not market_state.get("warmup_complete", True):
            return None
        if self.is_disabled(market_state)[0]:
            return None

        p = self._price(market_state)
        product_id = str(market_state.get("product_id", "BTC-USD"))
        self._ensure(product_id)
        filled = self._filled[product_id]
        lines = self._lines

        if p < self.bot_config.lower or p > self.bot_config.upper:
            return None

        # DOWN-cross: deepest unfilled grid line at or below the price -> BUY.
        buy_i: Optional[int] = None
        for i in range(len(lines) - 1, -1, -1):
            if lines[i] >= p and i not in filled:
                buy_i = i
                break
        if buy_i is not None:
            if self.bot_config.max_layers > 0 and buy_i >= self.bot_config.max_layers:
                return None
            size = self._size(buy_i)
            self._record(product_id, "BUY", lines[buy_i], size)
            filled.add(buy_i)
            self._layer[product_id] = buy_i
            return self._emit(
                "BUY", f"martingale down-cross buy grid {buy_i}", market_state
            )

        # UP-cross: deepest filled grid line at or above the price -> SELL.
        sell_i: Optional[int] = None
        for i in range(len(lines) - 1, -1, -1):
            if lines[i] <= p and i in filled:
                sell_i = i
                break
        if sell_i is not None:
            size = self._size(sell_i)
            self._record(product_id, "SELL", lines[sell_i], size)
            filled.discard(sell_i)
            self._layer[product_id] = sell_i
            return self._emit(
                "SELL", f"martingale up-cross sell grid {sell_i}", market_state
            )

        return None
