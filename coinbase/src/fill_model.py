from __future__ import annotations

import logging
import random
from dataclasses import dataclass, field
from typing import Dict, Optional

log = logging.getLogger(__name__)

# Typical slippage bps by volume tier (24h USD volume)
SLIPPAGE_TABLE: list[tuple[float, float, float]] = [
    (50_000_000, 0.3, 0.5),    # Mega-cap: BTC, ETH
    (10_000_000, 0.5, 1.0),    # Large-cap: SOL, XRP
    (1_000_000, 1.0, 2.0),     # Mid-cap: ADA, DOT, LINK
    (100_000, 2.0, 4.0),       # Small-cap
    (0, 4.0, 8.0),             # Micro-cap
]


@dataclass
class FillEstimate:
    entry_price: float = 0.0
    exit_price: float = 0.0
    entry_slippage_bps: float = 0.0
    exit_slippage_bps: float = 0.0
    fill_seconds: float = 0.0
    partial_fill_pct: float = 1.0


class FillModel:
    """Realistic fill simulation for paper trading.

    Models:
      - Slippage based on 24h volume tier (wider for smaller pairs)
      - Random variance around base slippage
      - Partial fills on low-volume pairs (some orders don't fill)
      - Fill delay based on liquidity
      - Maker/taker fee assignment consistent with execution quality

    Usage:
        fill = FillModel()
        estimate = fill.estimate(product_id, side, qty, price, volume_24h)
        # Use estimate.entry_price / .exit_price instead of raw price
    """

    def __init__(self, seed: Optional[int] = None):
        self._rng = random.Random(seed)

    def estimate(
        self,
        product_id: str,
        side: str,
        qty: float,
        price: float,
        volume_24h: float,
    ) -> FillEstimate:
        volume_tier = self._volume_tier(volume_24h)
        base_slippage, max_slippage = volume_tier

        # Larger orders relative to market volume get worse fills.
        notional = max(qty * price, 0.0)
        size_impact = 1.0
        if volume_24h > 0:
            participation = notional / max(volume_24h, 1e-9)
            # 0.1% of daily volume starts to matter; 1% becomes expensive.
            size_impact = 1.0 + min(3.0, participation * 25.0)

        # Random variance: 0.5x to 1.5x of base slippage
        slippage_mult = 0.5 + self._rng.random()
        slippage_bps = min(base_slippage * slippage_mult * size_impact, max_slippage)

        # Bid-ask spread proxy: slippage / 2
        half_spread_bps = slippage_bps / 2.0

        # Entry: market order buys at ask (higher), sells at bid (lower)
        side_u = side.upper() if side else ""
        if side_u == "BUY":
            entry_slippage = half_spread_bps * (0.8 + self._rng.random() * 0.4)
            entry_price = price * (1.0 + entry_slippage / 10_000.0)
        else:
            entry_slippage = half_spread_bps * (0.8 + self._rng.random() * 0.4)
            entry_price = price * (1.0 - entry_slippage / 10_000.0)

        # Exit: opposite side, additional slippage
        if side_u == "BUY":
            exit_slippage = half_spread_bps * (0.8 + self._rng.random() * 0.4) * 1.1
            exit_price = price * (1.0 - exit_slippage / 10_000.0)
        else:
            exit_slippage = half_spread_bps * (0.8 + self._rng.random() * 0.4) * 1.1
            exit_price = price * (1.0 + exit_slippage / 10_000.0)

        # Fill delay based on volume
        if volume_24h >= 50_000_000:
            fill_seconds = 0.1 + self._rng.random() * 0.3
        elif volume_24h >= 10_000_000:
            fill_seconds = 0.3 + self._rng.random() * 1.0
        elif volume_24h >= 1_000_000:
            fill_seconds = 0.5 + self._rng.random() * 2.0
        elif volume_24h >= 100_000:
            fill_seconds = 1.0 + self._rng.random() * 5.0
        else:
            fill_seconds = 2.0 + self._rng.random() * 10.0

        # Partial fill risk for thin markets
        if volume_24h < 100_000:
            partial_fill_pct = 0.7 + self._rng.random() * 0.3
        elif volume_24h < 1_000_000:
            partial_fill_pct = 0.85 + self._rng.random() * 0.15
        else:
            partial_fill_pct = 0.95 + self._rng.random() * 0.05

        return FillEstimate(
            entry_price=round(entry_price, 6),
            exit_price=round(exit_price, 6),
            entry_slippage_bps=round(entry_slippage, 2),
            exit_slippage_bps=round(exit_slippage, 2),
            fill_seconds=round(fill_seconds, 2),
            partial_fill_pct=round(min(1.0, partial_fill_pct), 4),
        )

    def is_maker(self, maker_pct: float) -> bool:
        """Randomly determine if this fill was a maker (limit) or taker (market)."""
        return self._rng.random() < maker_pct

    def _volume_tier(self, volume_24h: float) -> tuple[float, float]:
        for threshold, min_s, max_s in SLIPPAGE_TABLE:
            if volume_24h >= threshold:
                return min_s, max_s
        return SLIPPAGE_TABLE[-1][1], SLIPPAGE_TABLE[-1][2]
