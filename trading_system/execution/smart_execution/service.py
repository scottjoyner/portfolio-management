from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

log = logging.getLogger(__name__)


class SmartOrderType(Enum):
    TWAP = "twap"
    ICBERG = "iceberg"
    PEGGED = "pegged"


@dataclass
class TWAPSlice:
    size: Decimal
    executed: bool = False


@dataclass
class TWAPOrder:
    total_size: Decimal
    slices: int
    interval_seconds: float
    remaining_slices: int = 0
    slices_data: list[TWAPSlice] = field(default_factory=list)
    started_at: float = 0.0

    def __post_init__(self) -> None:
        self.remaining_slices = self.slices
        slice_size = self.total_size / Decimal(str(self.slices))
        self.slices_data = [TWAPSlice(size=slice_size) for _ in range(self.slices)]

    def next_slice(self) -> Decimal | None:
        now = time.time()
        if self.started_at == 0:
            self.started_at = now

        elapsed = now - self.started_at
        expected_slices = int(elapsed / self.interval_seconds)

        if expected_slices >= self.slices:
            return None

        if expected_slices >= 0:
            for i in range(expected_slices + 1):
                if i < len(self.slices_data) and not self.slices_data[i].executed:
                    self.slices_data[i].executed = True
                    self.remaining_slices -= 1
                    return self.slices_data[i].size

        return None


@dataclass
class IcebergOrder:
    total_size: Decimal
    visible_size: Decimal
    current_visible: Decimal = Decimal("0")
    filled: Decimal = Decimal("0")

    def next_chunk(self) -> Decimal | None:
        remaining = self.total_size - self.filled
        if remaining <= Decimal("0"):
            return None
        self.current_visible = min(self.visible_size, remaining)
        return self.current_visible

    def update_filled(self, amount: Decimal) -> None:
        self.filled += amount


@dataclass
class PeggedOrder:
    product_id: str
    size: Decimal
    offset_bps: Decimal
    side: str
    last_price: Decimal = Decimal("0")
    last_placed_price: Decimal = Decimal("0")

    def target_price(self, reference_price: Decimal) -> Decimal:
        self.last_price = reference_price
        offset = reference_price * self.offset_bps / Decimal("10000")
        if self.side == "buy":
            self.last_placed_price = reference_price - offset
        else:
            self.last_placed_price = reference_price + offset
        return self.last_placed_price


@dataclass
class SmartExecutionEngine:
    twap_orders: dict[str, TWAPOrder] = field(default_factory=dict)
    iceberg_orders: dict[str, IcebergOrder] = field(default_factory=dict)

    def create_twap(self, order_id: str, total_size: Decimal, slices: int, interval_seconds: float) -> TWAPOrder:
        order = TWAPOrder(total_size=total_size, slices=slices, interval_seconds=interval_seconds)
        self.twap_orders[order_id] = order
        return order

    def create_iceberg(self, order_id: str, total_size: Decimal, visible_size: Decimal) -> IcebergOrder:
        order = IcebergOrder(total_size=total_size, visible_size=visible_size)
        self.iceberg_orders[order_id] = order
        return order
