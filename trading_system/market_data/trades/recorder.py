from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class TradeRecord:
    product_id: str
    side: str
    price: float
    size: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    trade_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "product_id": self.product_id,
            "side": self.side,
            "price": self.price,
            "size": self.size,
            "timestamp": self.timestamp.isoformat(),
            "trade_id": self.trade_id,
        }


class TradeRecorder:
    def __init__(self, max_records: int = 10_000) -> None:
        self.max_records = max_records
        self._trades: list[TradeRecord] = []

    def record(self, product_id: str, side: str, price: float, size: float, trade_id: str = "") -> TradeRecord:
        trade = TradeRecord(
            product_id=product_id,
            side=side,
            price=price,
            size=size,
            trade_id=trade_id,
        )
        self._trades.append(trade)
        if len(self._trades) > self.max_records:
            self._trades = self._trades[-self.max_records:]
        return trade

    def recent(self, count: int = 100) -> list[TradeRecord]:
        return self._trades[-count:]

    def by_product(self, product_id: str, count: int = 100) -> list[TradeRecord]:
        filtered = [t for t in self._trades if t.product_id == product_id]
        return filtered[-count:]

    def clear(self) -> None:
        self._trades.clear()
