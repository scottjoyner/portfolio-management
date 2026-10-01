from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class PositionLimit:
    product_id: str
    max_size: Decimal
    max_notional: Decimal
    max_side: str = "both"

    def allows_side(self, side: str) -> bool:
        if self.max_side == "both":
            return True
        return side.lower() == self.max_side.lower()


@dataclass
class LimitManager:
    limits: dict[str, PositionLimit] = field(default_factory=dict)
    current_positions: dict[str, Decimal] = field(default_factory=dict)

    def set_limit(self, limit: PositionLimit) -> None:
        self.limits[limit.product_id] = limit

    def remove_limit(self, product_id: str) -> None:
        self.limits.pop(product_id, None)

    def update_position(self, product_id: str, size: Decimal) -> None:
        self.current_positions[product_id] = size

    def check_order(self, product_id: str, side: str, size: Decimal, price: Decimal) -> tuple[bool, str]:
        limit = self.limits.get(product_id)
        if limit is None:
            # Fail closed. "No limit configured" used to mean "no limit", so any
            # product missing from the map -- a new symbol, a config load
            # failure, a BTC-USD vs BTC/USD naming mismatch -- traded uncapped.
            return False, f"no limit configured for {product_id}; refusing rather than trading uncapped"

        if not limit.allows_side(side):
            return False, f"side {side} not allowed for {product_id}"

        # A zero or negative size or price makes notional zero or negative, which
        # passes every notional comparison below. Refuse the input rather than
        # doing the arithmetic on it.
        try:
            if size <= 0:
                return False, f"order size {size} must be positive"
            if price <= 0:
                return False, f"order price {price} must be positive"
        except TypeError:
            return False, "order size and price must be numeric"

        notional = size * price
        if notional > limit.max_notional:
            return False, f"order notional {notional} exceeds limit {limit.max_notional}"

        current = self.current_positions.get(product_id, Decimal("0"))
        new_size = current + size if side == "buy" else current - size
        if abs(new_size) > limit.max_size:
            return False, f"position size {new_size} would exceed limit {limit.max_size}"

        return True, ""
