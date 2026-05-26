from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class BookLevel:
    price: float
    size: float
    order_count: int = 0


class OrderBook:
    def __init__(self, product_id: str, levels: int = 50) -> None:
        self.product_id = product_id
        self.levels = levels
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}

    def update(self, side: str, price: float, size: float) -> None:
        target = self._bids if side == "buy" else self._asks
        if size == 0:
            target.pop(price, None)
        else:
            target[price] = size

    def snapshot(self, bids: list[list[float]], asks: list[list[float]]) -> None:
        self._bids = {px: sz for px, sz, *_ in bids}
        self._asks = {px: sz for px, sz, *_ in asks}

    def top_of_book(self) -> dict[str, Any]:
        best_bid = max(self._bids) if self._bids else 0.0
        best_ask = min(self._asks) if self._asks else 0.0
        return {
            "bid_px": best_bid,
            "bid_sz": self._bids.get(best_bid, 0.0),
            "ask_px": best_ask,
            "ask_sz": self._asks.get(best_ask, 0.0),
            "spread": best_ask - best_bid if best_bid and best_ask else 0.0,
            "mid": (best_bid + best_ask) / 2.0 if best_bid and best_ask else 0.0,
        }

    def depth(self, levels: int = 10) -> dict[str, list[BookLevel]]:
        bid_pxs = sorted(self._bids.keys(), reverse=True)[:levels]
        ask_pxs = sorted(self._asks.keys())[:levels]
        return {
            "bids": [BookLevel(px, self._bids[px]) for px in bid_pxs],
            "asks": [BookLevel(px, self._asks[px]) for px in ask_pxs],
        }

    def accumulate_depth(self, side: str, levels: int = 10) -> float:
        target = self._bids if side == "buy" else self._asks
        sorted_pxs = sorted(target.keys(), reverse=(side == "buy"))[:levels]
        return sum(target[px] for px in sorted_pxs)

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()
