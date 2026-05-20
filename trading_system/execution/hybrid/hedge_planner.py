from __future__ import annotations

from decimal import Decimal

from onchain.dex.clmm.hedge_model import compute_target_hedge, hedge_notional_after_costs
from onchain.dex.clmm.schemas import HedgeMode, HedgePlan


def build_coinbase_hedge_plan(current_delta_usd: Decimal, mode: HedgeMode, est_cost_usd: Decimal) -> HedgePlan:
    target = compute_target_hedge(current_delta_usd, mode)
    notional = hedge_notional_after_costs(target, est_cost_usd, Decimal("5"))
    return HedgePlan(
        hedge_mode=mode,
        base_asset="ETH",
        quote_asset="USD",
        target_delta_usd=current_delta_usd,
        hedge_notional_usd=notional,
        max_slippage_bps=Decimal("15"),
        urgency_score=min(Decimal("1"), abs(current_delta_usd) / Decimal("10000")),
        cooldown_seconds=30,
    )


def estimate_coinbase_hedge_cost(notional_usd: Decimal, fee_bps: Decimal = Decimal("2"), slippage_bps: Decimal = Decimal("3")) -> Decimal:
    return notional_usd * (fee_bps + slippage_bps) / Decimal("10000")
