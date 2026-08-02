"""Compatibility import for the canonical paper exchange implementation.

The supported engine lives under :mod:`trading_system.apps.paper_exchange`.
Keeping this module as a re-export prevents ``PYTHONPATH`` ordering from
selecting an obsolete in-memory stub with a different API and accounting
behavior.
"""

from trading_system.apps.paper_exchange.engine import (
    PaperExchangeEngine,
    PaperFill,
    PaperOrder,
    PaperPosition,
)

__all__ = [
    "PaperExchangeEngine",
    "PaperFill",
    "PaperOrder",
    "PaperPosition",
]
