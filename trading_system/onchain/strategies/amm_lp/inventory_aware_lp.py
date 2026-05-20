from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class InventoryAwareLPConfig(BaseModel):
    skew_limit: Decimal = Field(default=Decimal("0.35"), ge=0, le=1)


class InventoryAwareLP:
    name = "InventoryAwareLP"

    def __init__(self, config: InventoryAwareLPConfig):
        self.config = config

    def should_pause_accumulation(self, skew: Decimal) -> bool:
        return abs(skew) > self.config.skew_limit
