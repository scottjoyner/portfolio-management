from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass
class LPPosition:
    position_id: str
    chain: str
    protocol: str
    pool_address: str
    amount_usd: Decimal = Decimal("0")
    token0: str = ""
    token1: str = ""
    fee_bps: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LPManager:
    positions: dict[str, LPPosition] = field(default_factory=dict)

    def add_position(self, position: LPPosition) -> None:
        self.positions[position.position_id] = position

    def remove_position(self, position_id: str) -> None:
        self.positions.pop(position_id, None)

    def total_liquidity_usd(self) -> Decimal:
        return sum((p.amount_usd for p in self.positions.values()), Decimal("0"))
