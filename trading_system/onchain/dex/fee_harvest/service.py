from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class FeeHarvest:
    position_id: str
    token0_fees: Decimal = Decimal("0")
    token1_fees: Decimal = Decimal("0")
    estimated_gas: Decimal = Decimal("0")
    harvestable: bool = False


@dataclass
class FeeHarvestService:
    _positions: dict[str, FeeHarvest] = field(default_factory=dict)

    def track(self, position_id: str, token0_fees: Decimal, token1_fees: Decimal) -> None:
        self._positions[position_id] = FeeHarvest(
            position_id=position_id,
            token0_fees=token0_fees,
            token1_fees=token1_fees,
            harvestable=token0_fees > 0 or token1_fees > 0,
        )

    def harvestable_positions(self) -> list[FeeHarvest]:
        return [p for p in self._positions.values() if p.harvestable]
