from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

log = logging.getLogger(__name__)


class OrderStatus(Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


@dataclass
class TrackedOrder:
    order_id: str
    product_id: str
    side: OrderSide
    order_type: OrderType
    size: float
    price: float | None = None
    stop_price: float | None = None
    status: OrderStatus = OrderStatus.PENDING
    filled_size: float = 0.0
    avg_fill_price: float = 0.0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    exchange_order_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderManager:
    orders: dict[str, TrackedOrder] = field(default_factory=dict)

    def add_order(self, order: TrackedOrder) -> None:
        self.orders[order.order_id] = order
        log.info("order_added id=%s product=%s side=%s", order.order_id, order.product_id, order.side.value)

    def get_order(self, order_id: str) -> TrackedOrder | None:
        return self.orders.get(order_id)

    def update_status(self, order_id: str, status: OrderStatus) -> None:
        order = self.orders.get(order_id)
        if order:
            order.status = status
            order.updated_at = datetime.now(timezone.utc)

    def update_fill(self, order_id: str, filled_size: float, fill_price: float) -> None:
        order = self.orders.get(order_id)
        if order:
            order.filled_size += filled_size
            total_cost_before = order.avg_fill_price * (order.filled_size - filled_size)
            order.avg_fill_price = (total_cost_before + fill_price * filled_size) / order.filled_size if order.filled_size > 0 else fill_price
            order.updated_at = datetime.now(timezone.utc)
            if order.filled_size >= order.size:
                order.status = OrderStatus.FILLED
            else:
                order.status = OrderStatus.PARTIALLY_FILLED

    def cancel_order(self, order_id: str) -> None:
        order = self.orders.get(order_id)
        if order and order.status in (OrderStatus.PENDING, OrderStatus.OPEN):
            order.status = OrderStatus.CANCELLED
            order.updated_at = datetime.now(timezone.utc)

    def open_orders(self) -> list[TrackedOrder]:
        return [o for o in self.orders.values() if o.status in (OrderStatus.PENDING, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)]

    def orders_for_product(self, product_id: str) -> list[TrackedOrder]:
        return [o for o in self.orders.values() if o.product_id == product_id]
