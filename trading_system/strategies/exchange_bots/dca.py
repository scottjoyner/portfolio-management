"""Dollar-cost-averaging (DCA) exchange-bot strategy.

A simple, deterministic accumulation bot: it buys a fixed USD notional at a
fixed cadence (``interval_seconds``) up to ``max_buys`` times. An optional
``trigger_drop_pct`` gates each interval so that a buy only fires when the
price has dropped at least that fraction below the running reference price.

See :mod:`trading_system.strategies.exchange_bots.base` for the shared base
class, config contract, and the ``market_state`` schema.
"""
from __future__ import annotations

from typing import Optional

from pydantic import Field

from ..base.interfaces import StrategySignal
from .base import BotConfig, ExchangeBotStrategy


class DcaConfig(BotConfig):
    """Configurable parameters for :class:`DcaStrategy`."""

    interval_seconds: float = Field(default=86400.0, gt=0.0)
    amount_usd: float = Field(default=100.0)
    max_buys: int = Field(default=10, ge=0)
    trigger_drop_pct: float = Field(default=0.0, ge=0.0, le=1.0)
    base_price: float = Field(default=0.0, ge=0.0)


class DcaStrategy(ExchangeBotStrategy):
    """Accumulate a fixed USD notional at a fixed cadence (with optional dip gate)."""

    config_model: type[DcaConfig] = DcaConfig
    bot_config: DcaConfig

    def __init__(
        self,
        strategy_id: str = "dca",
        strategy_type: str = "dca",
        bot_config: Optional[DcaConfig] = None,
        **kwargs,
    ) -> None:
        super().__init__(strategy_id, strategy_type, bot_config=bot_config, **kwargs)
        self._last_buy_ts: dict[str, float] = {}
        self._buys: dict[str, int] = {}
        self._ref: dict[str, float] = {}
        self._step: dict[str, int] = {}

    # ------------------------------------------------------------------ helpers
    def _buy(
        self, product: str, price: float, ref: float, ts: float, ms: dict, reason: str
    ) -> StrategySignal:
        """Record + emit a BUY for ``product`` and update per-product state."""
        size = self.bot_config.amount_usd / price
        self._ref[product] = ref
        self._last_buy_ts[product] = ts
        self._buys[product] = self._buys.get(product, 0) + 1
        self._record(product, "BUY", price, size)
        return self._emit("BUY", reason, ms)

    # ------------------------------------------------------------------ signal
    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if not market_state.get("warmup_complete", True):
            return None
        if self.is_disabled(market_state)[0]:
            return None

        product = str(market_state.get("product_id", "BTC-USD"))
        price = self._price(market_state)
        if price <= 0:
            return None
        if self.bot_config.amount_usd <= 0:
            return None

        # Timestamp when present; otherwise fall back to a monotonic step counter.
        if market_state.get("timestamp") is not None:
            ts = self._ts(market_state)
        else:
            self._step[product] = self._step.get(product, 0) + 1
            ts = float(self._step[product])

        cfg = self.bot_config
        if self._buys.get(product, 0) >= cfg.max_buys:
            return None

        # First buy for this product.
        if product not in self._last_buy_ts:
            ref = cfg.base_price if cfg.base_price > 0 else price
            return self._buy(product, price, ref, ts, market_state, "dca first buy")

        # Subsequent buys: only once the interval has elapsed.
        elapsed = ts - self._last_buy_ts[product]
        if elapsed < cfg.interval_seconds:
            return None

        if cfg.trigger_drop_pct > 0:
            ref = self._ref.get(product, price)
            if price > ref * (1.0 - cfg.trigger_drop_pct):
                # Dip not deep enough: skip this interval without resetting timer.
                return None

        return self._buy(product, price, price, ts, market_state, "dca interval buy")
