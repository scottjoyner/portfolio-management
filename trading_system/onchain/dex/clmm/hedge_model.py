from __future__ import annotations

from decimal import Decimal

from .schemas import HedgeMode


def compute_target_hedge(current_delta_usd: Decimal, mode: HedgeMode, partial_ratio: Decimal = Decimal("1"), directional_bias_usd: Decimal = Decimal("0")) -> Decimal:
    if mode == HedgeMode.NONE:
        return Decimal("0")
    if mode == HedgeMode.DELTA_NEUTRAL:
        return -current_delta_usd
    if mode == HedgeMode.PARTIAL_HEDGE:
        return -current_delta_usd * partial_ratio
    if mode == HedgeMode.DIRECTIONAL_BIAS:
        return -current_delta_usd + directional_bias_usd
    if mode == HedgeMode.EMERGENCY_FLATTEN:
        return -current_delta_usd
    return Decimal("0")


def compute_hedge_band(realized_vol: Decimal, base_band_usd: Decimal, gas_score: Decimal) -> Decimal:
    return base_band_usd * (Decimal("1") + realized_vol) * (Decimal("1") + gas_score)


def should_hedge(current_delta_usd: Decimal, band_usd: Decimal, cooldown_elapsed: bool = True) -> bool:
    return cooldown_elapsed and abs(current_delta_usd) > band_usd


def hedge_urgency_score(delta_usd: Decimal, band_usd: Decimal, vol: Decimal) -> Decimal:
    if band_usd <= 0:
        return Decimal("1")
    raw = abs(delta_usd) / band_usd * (Decimal("1") + vol)
    return min(Decimal("1"), raw)


def hedge_notional_after_costs(target_hedge_usd: Decimal, est_cost_usd: Decimal, min_profitable_usd: Decimal) -> Decimal:
    return target_hedge_usd if abs(target_hedge_usd) > est_cost_usd + min_profitable_usd else Decimal("0")


def hedge_break_even_move(cost_usd: Decimal, hedge_notional_usd: Decimal) -> Decimal:
    if hedge_notional_usd == 0:
        return Decimal("0")
    return abs(cost_usd / hedge_notional_usd)


def hedge_slippage_budget(volatility: Decimal, max_bps: Decimal) -> Decimal:
    return min(max_bps, max_bps * (Decimal("0.5") + volatility))


def post_hedge_exposure(current_delta_usd: Decimal, executed_hedge_usd: Decimal) -> Decimal:
    return current_delta_usd + executed_hedge_usd
