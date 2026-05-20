from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from execution.hybrid.hedge_planner import build_coinbase_hedge_plan
from onchain.dex.clmm.schemas import HedgeMode


class DeltaNeutralLPHedgedConfig(BaseModel):
    hedge_band_usd: Decimal = Field(default=Decimal("1000"), gt=0)


class DeltaNeutralLPHedged:
    name = "DeltaNeutralLPHedged"

    def __init__(self, config: DeltaNeutralLPHedgedConfig):
        self.config = config

    def hedge_plan(self, delta_usd: Decimal):
        return build_coinbase_hedge_plan(delta_usd, HedgeMode.DELTA_NEUTRAL, Decimal("3"))
