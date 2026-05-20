from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from onchain.dex.clmm.range_selector import select_range
from onchain.dex.clmm.schemas import RangeSelectionInput


class ConcentratedLiquidityMMConfig(BaseModel):
    rebalance_distance_bps: Decimal = Field(default=Decimal("35"), gt=0)
    min_net_benefit_usd: Decimal = Field(default=Decimal("10"), gt=0)
    max_gas_usd: Decimal = Field(default=Decimal("60"), gt=0)


class ConcentratedLiquidityMM:
    name = "ConcentratedLiquidityMM"

    def __init__(self, config: ConcentratedLiquidityMMConfig):
        self.config = config

    def generate_action(self, inp: RangeSelectionInput) -> dict:
        candidate = select_range(inp)
        return {"action": "rebalance_position", "range": candidate.model_dump(), "explain": "vol and gas aware CLMM rebalance"}
