from __future__ import annotations

from decimal import Decimal

from onchain.dex.clmm.schemas import StressScenarioInput, StressScenarioResult


def run_stress_case(inp: StressScenarioInput) -> StressScenarioResult:
    pnl = Decimal("1000") - inp.spot_shock_pct * Decimal("20") - inp.gas_multiplier * Decimal("30")
    return StressScenarioResult(
        name=inp.name,
        pnl_usd=pnl,
        max_drawdown_usd=abs(min(pnl, Decimal("0"))),
        time_out_of_range_pct=min(Decimal("100"), abs(inp.spot_shock_pct) * Decimal("2")),
        hedge_failures=1 if inp.vol_multiplier > Decimal("2") else 0,
        fragility_score=min(Decimal("1"), inp.vol_multiplier / Decimal("3")),
    )
