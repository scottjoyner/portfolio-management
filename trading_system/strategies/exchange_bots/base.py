"""Configurable exchange-bot strategies (KuCoin / Binance-style templates).

These are stateful, position-aware strategies that plug into the
``trading_system/strategies`` registry (they subclass
:class:`BaseSignalStrategy`). Each bot is driven by a pydantic config
(``BotConfig`` subclass) and decides orders from a ``market_state`` dict.

``market_state`` contract (dict passed to ``generate_signal``)::

    {
        "product_id": str,
        "price": float,                  # current mid / last price
        "position": {                    # open position, if any
            "side": "LONG" | "SHORT",
            "size": float,               # quantity
            "entry_price": float,
            "unrealized_pnl": float,     # optional
        },
        "cash": float,                   # available notional for buys (optional)
        "holdings": {product_id: float}, # current portfolio value by product (rebalance)
        "timestamp": float,              # epoch seconds (interval / slice bots)
        "warmup_complete": bool,
        "score": float,                  # optional, for compatibility
    }

A bot returns a :class:`StrategySignal` from ``generate_signal`` (score sign
encodes direction: positive => buy, negative => sell) and emits the precise
order via ``order_intents(signal, market_state)`` (a list of order dicts with
``side``, ``type``, ``price``, ``size_hint``).
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from ..base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from ..base.interfaces import StrategySignal


class BotConfig(BaseModel):
    """Base configurable parameters shared by all exchange-bot strategies."""

    enabled: bool = True
    products: list[str] = Field(default_factory=lambda: ["BTC-USD"])
    capital_fraction: float = Field(default=0.1, gt=0.0, le=1.0)
    min_size: float = Field(default=0.0001, gt=0.0)
    cooldown_seconds: float = Field(default=0.0, ge=0.0)


class ExchangeBotStrategy(BaseSignalStrategy):
    """Base class for configurable exchange-bot templates.

    Subclasses set ``config_model`` to their pydantic config class, implement
    ``generate_signal`` (returning a :class:`StrategySignal` whose score sign
    encodes direction) and ``order_intents`` (returning the precise order
    dicts). Per-product decisions are kept in ``self._decisions`` so the same
    instance can serve multiple products.
    """

    config_model: type[BotConfig] = BotConfig
    bot_config: BotConfig

    def __init__(
        self,
        strategy_id: str,
        strategy_type: str,
        bot_config: Optional[BotConfig] = None,
        metadata: Optional[StrategyMetadata] = None,
        config: Optional[StrategyConfig] = None,
    ) -> None:
        meta = metadata or StrategyMetadata(
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            data_requirements=["product_id", "price"],
            risk_mode_hint="NORMAL",
            capital_bucket="ACTIVE_TRADING",
        )
        cfg = config or StrategyConfig()
        super().__init__(metadata=meta, config=cfg)
        self.bot_config = bot_config or self.config_model()
        self._decisions: dict[str, tuple[str, float, float]] = {}

    # ------------------------------------------------------------------ helpers
    def _price(self, ms: dict) -> float:
        return float(ms.get("price") or 0.0)

    def _ts(self, ms: dict) -> float:
        return float(ms.get("timestamp", 0.0))

    def _has_position(self, ms: dict, side: Optional[str] = None) -> bool:
        p = ms.get("position")
        if not p:
            return False
        if float(p.get("size", 0) or 0) <= 0:
            return False
        if side and str(p.get("side", "")).upper() != side.upper():
            return False
        return True

    def _record(self, product_id: str, side: str, price: float, size: float) -> None:
        self._decisions[product_id] = (side, price, size)

    def _emit(
        self,
        side: str,
        reason: str,
        ms: dict,
        confidence: float = 0.8,
        score: Optional[float] = None,
    ) -> StrategySignal:
        s = score if score is not None else (0.8 if side == "BUY" else -0.8)
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(ms.get("product_id", "BTC-USD")),
            score=s,
            reason=reason,
            confidence=confidence,
            warmup_passed=bool(ms.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type],
        )

    # ------------------------------------------------------------------ hooks
    def is_disabled(self, market_state: dict) -> tuple[bool, str]:
        if not self.bot_config.enabled:
            return True, "bot disabled by config"
        return False, "enabled"

    def order_intents(self, signal: StrategySignal, market_state: dict) -> list[dict]:
        intent = self._decisions.get(signal.product_id)
        if not intent:
            return []
        side, price, size = intent
        return [
            {
                "strategy_id": self.strategy_id,
                "product_id": signal.product_id,
                "side": side,
                "type": "limit",
                "price": price,
                "size_hint": max(self.bot_config.min_size, size),
                "time_in_force": "GTC",
            }
        ]
