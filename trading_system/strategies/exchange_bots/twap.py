"""Time-Weighted Average Price (TWAP) execution algorithm.

A stateful, position-aware exchange bot that splits a total notional order
into ``slices`` child orders evenly spaced over ``duration_seconds``. Each
product is tracked independently. The algorithm emits one child order per
slice via :meth:`ExchangeBotStrategy.generate_signal` and the recorded order
intent is surfaced through the base class ``order_intents`` hook.

``market_state`` contract is documented in
:mod:`trading_system.strategies.exchange_bots.base`.
"""
from __future__ import annotations

from typing import Optional

from .base import BotConfig, ExchangeBotStrategy
from ..base.interfaces import StrategySignal


class TwapConfig(BotConfig):
    """Configuration for the TWAP execution algorithm."""

    duration_seconds: float = 60.0
    total_usd: float = 1000.0
    side: str = "BUY"
    slices: int = 10


class TwapStrategy(ExchangeBotStrategy):
    """Time-Weighted Average Price execution bot.

    Splits ``total_usd`` into ``slices`` equal-notional child orders spread
    evenly across ``duration_seconds``. BUY emits a positive score, SELL a
    negative one. When ``timestamp`` is absent from ``market_state`` the bot
    falls back to an internal per-product step counter so it can be driven
    deterministically in tests / replay.
    """

    config_model = TwapConfig

    def __init__(
        self,
        strategy_id: str = "TwapStrategy",
        strategy_type: str = "execution",
        bot_config: Optional[BotConfig] = None,
        metadata=None,
        config=None,
    ) -> None:
        super().__init__(
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            bot_config=bot_config,
            metadata=metadata,
            config=config,
        )
        self._start_ts: dict[str, float] = {}
        self._slice_idx: dict[str, int] = {}
        self._step: dict[str, int] = {}
        self._slice_usd: float = 0.0
        self._slice_int: float = 0.0

    def generate_signal(self, market_state: dict) -> Optional[StrategySignal]:
        if not market_state.get("warmup_complete", True):
            return None
        if self.is_disabled(market_state)[0]:
            return None

        cfg = self.bot_config
        if cfg.slices < 1 or cfg.total_usd <= 0 or cfg.duration_seconds <= 0:
            return None

        side = cfg.side
        if side not in ("BUY", "SELL"):
            return None

        self._slice_usd = cfg.total_usd / cfg.slices
        self._slice_int = cfg.duration_seconds / cfg.slices

        product = str(market_state.get("product_id", "BTC-USD"))
        price = self._price(market_state)
        if price <= 0:
            return None

        timestamp = market_state.get("timestamp")
        if timestamp is None:
            step = self._step.get(product, 0) + 1
            self._step[product] = step
            idx = self._slice_idx.get(product, 0)
            interval = 1.0
            elapsed = step - 1.0
        else:
            if product not in self._start_ts:
                self._start_ts[product] = float(timestamp)
                self._slice_idx[product] = 0
            idx = self._slice_idx.get(product, 0)
            interval = self._slice_int
            elapsed = float(timestamp) - self._start_ts[product]

        if idx >= cfg.slices:
            return None

        if elapsed >= idx * interval:
            size = self._slice_usd / price
            self._record(product, side, price, size)
            self._slice_idx[product] = idx + 1
            reason = (
                f"TWAP slice {idx + 1}/{cfg.slices} {side} "
                f"{size:.6f} @ {price:.2f}"
            )
            return self._emit(side, reason, market_state)

        return None
