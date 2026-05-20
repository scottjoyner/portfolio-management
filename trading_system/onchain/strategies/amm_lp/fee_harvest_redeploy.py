from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from onchain.dex.clmm.schemas import ProfitSweepDecision


class FeeHarvestRedeployConfig(BaseModel):
    min_harvest_usd: Decimal = Field(default=Decimal("25"), gt=0)
    min_compound_edge_usd: Decimal = Field(default=Decimal("10"), gt=0)
    sweep_threshold_usd: Decimal = Field(default=Decimal("500"), gt=0)


def should_compound_fees(harvest_usd: Decimal, compound_edge_usd: Decimal, gas_usd: Decimal, threshold: Decimal) -> bool:
    return harvest_usd > gas_usd and compound_edge_usd > threshold


def sweep_priority_score(realized_gain_usd: Decimal, vol_spike: Decimal, hwm_gap: Decimal) -> Decimal:
    return realized_gain_usd / Decimal("1000") + vol_spike * Decimal("0.8") + hwm_gap


def allocate_realized_gains(realized_gain_usd: Decimal, sweep_ratio: Decimal = Decimal("0.7")) -> tuple[Decimal, Decimal]:
    sweep = realized_gain_usd * sweep_ratio
    return sweep, realized_gain_usd - sweep


def protect_high_water_mark(current_equity: Decimal, high_water_mark: Decimal, lock_ratio: Decimal = Decimal("0.5")) -> Decimal:
    excess = max(Decimal("0"), current_equity - high_water_mark)
    return excess * lock_ratio


def evaluate_profit_sweep(realized_gain_usd: Decimal, gas_regime_score: Decimal, compound_edge_usd: Decimal) -> ProfitSweepDecision:
    sweep, compound = allocate_realized_gains(realized_gain_usd, Decimal("0.8") if gas_regime_score > Decimal("0.6") else Decimal("0.6"))
    do_compound = should_compound_fees(realized_gain_usd, compound_edge_usd, gas_regime_score * Decimal("30"), Decimal("10"))
    return ProfitSweepDecision(
        should_sweep=realized_gain_usd > Decimal("100"),
        should_compound=do_compound,
        sweep_amount_usd=sweep,
        compound_amount_usd=compound if do_compound else Decimal("0"),
        destination_bucket="CASH_BUFFER",
        reason="gas-aware sweep policy",
    )
