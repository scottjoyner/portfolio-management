from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class AttributionResult:
    strategy_id: str
    total_pnl: Decimal = Decimal("0")
    alpha_pnl: Decimal = Decimal("0")
    beta_pnl: Decimal = Decimal("0")
    fee_cost: Decimal = Decimal("0")


@dataclass
class AttributionService:
    def compute(self, strategy_id: str, portfolio_returns: list[Decimal], benchmark_returns: list[Decimal]) -> AttributionResult:
        return AttributionResult(strategy_id=strategy_id)
