from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class SwapExecution:
    tx_hash: str = ""
    status: str = "pending"


@dataclass
class SwapExecutor:
    def execute(self, route: object, amount_in: Decimal, min_amount_out: Decimal) -> SwapExecution:
        return SwapExecution()
