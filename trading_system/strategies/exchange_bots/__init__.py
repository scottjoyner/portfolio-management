"""Configurable exchange-bot strategies (KuCoin / Binance-style templates).

See :mod:`trading_system.strategies.exchange_bots.base` for the shared base
class, pydantic config contract, and the ``market_state`` schema.
"""
from __future__ import annotations

from .base import BotConfig, ExchangeBotStrategy

__all__ = ["BotConfig", "ExchangeBotStrategy"]
