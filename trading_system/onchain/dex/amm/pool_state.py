from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(slots=True)
class PoolState:
    reserve0: Decimal
    reserve1: Decimal
    fee_bps: Decimal
    block_number: int
