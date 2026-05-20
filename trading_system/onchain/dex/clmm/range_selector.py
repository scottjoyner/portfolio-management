from __future__ import annotations

from decimal import Decimal

from .math import price_to_tick
from .schemas import RangeCandidate, RangeSelectionInput


def widen_for_volatility(base_width_bps: Decimal, realized_vol: Decimal) -> Decimal:
    return base_width_bps * (Decimal("1") + realized_vol)


def widen_for_gas(width_bps: Decimal, gas_regime_score: Decimal) -> Decimal:
    return width_bps * (Decimal("1") + gas_regime_score * Decimal("0.5"))


def skew_range_for_inventory(lower_tick: int, upper_tick: int, inventory_skew: Decimal, tick_spacing: int) -> tuple[int, int]:
    skew_ticks = int(inventory_skew * Decimal("10") * tick_spacing)
    return lower_tick + skew_ticks, upper_tick + skew_ticks


def retreat_range_under_stress(mark_tick: int, tick_spacing: int, stress_level: Decimal) -> tuple[int, int]:
    width = int((Decimal("200") + stress_level * Decimal("500")) * tick_spacing)
    return mark_tick - width, mark_tick + width


def score_range_candidate(candidate: RangeCandidate) -> Decimal:
    return candidate.expected_fee_density * candidate.expected_utilization * candidate.confidence_score - candidate.expected_il_pressure - candidate.expected_hedge_drag


def optimize_range_candidates(candidates: list[RangeCandidate]) -> RangeCandidate:
    return max(candidates, key=score_range_candidate)


def select_range(data: RangeSelectionInput, base_width_bps: Decimal = Decimal("200")) -> RangeCandidate:
    width = widen_for_gas(widen_for_volatility(base_width_bps, data.realized_vol), data.gas_regime_score)
    half = width / Decimal("20000")
    low_price = data.mark_price * (Decimal("1") - half)
    high_price = data.mark_price * (Decimal("1") + half)
    lower = price_to_tick(low_price, data.tick_spacing, 18, 18)
    upper = price_to_tick(high_price, data.tick_spacing, 18, 18)
    lower, upper = skew_range_for_inventory(lower, upper, data.inventory_skew, data.tick_spacing)
    return RangeCandidate(
        lower_tick=lower,
        upper_tick=upper,
        width_bps=width,
        expected_utilization=max(Decimal("0"), Decimal("0.8") - data.short_vol * Decimal("0.2")),
        expected_fee_density=Decimal("1") + data.short_vol,
        expected_rebalance_pressure=data.short_vol,
        expected_il_pressure=data.realized_vol,
        expected_hedge_drag=data.short_vol * Decimal("0.5"),
        confidence_score=Decimal("0.9") if data.market_regime in {"CALM", "NORMAL"} else Decimal("0.6"),
    )
