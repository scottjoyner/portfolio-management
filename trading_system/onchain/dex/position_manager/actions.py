from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(slots=True)
class PositionAction:
    action_type: str
    position_id: str
    amount_usd: Decimal
    reason: str
