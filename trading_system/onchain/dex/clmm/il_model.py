from __future__ import annotations

from decimal import Decimal


def il_vs_hold_benchmark(start_price: Decimal, end_price: Decimal, start_token0: Decimal, start_token1: Decimal) -> Decimal:
    hold = start_token0 * end_price + start_token1
    k = start_token0 * start_token1
    lp_value = Decimal("2") * (k * end_price).sqrt()
    return lp_value - hold


def lp_pnl_decomposition(
    start_value_usd: Decimal,
    end_value_usd: Decimal,
    fee_income_usd: Decimal,
    realized_hedge_pnl_usd: Decimal,
    unrealized_hedge_pnl_usd: Decimal,
    gas_drag_usd: Decimal,
    rebalance_drag_usd: Decimal,
    slippage_drag_usd: Decimal,
    hold_benchmark_usd: Decimal,
) -> dict[str, Decimal]:
    net = end_value_usd - start_value_usd + fee_income_usd + realized_hedge_pnl_usd + unrealized_hedge_pnl_usd - gas_drag_usd - rebalance_drag_usd - slippage_drag_usd
    lp_edge = net - hold_benchmark_usd
    return {
        "net_pnl_usd": net,
        "lp_edge_vs_hold_usd": lp_edge,
        "fee_to_il_ratio": fee_income_usd / abs(lp_edge) if lp_edge != 0 else Decimal("0"),
    }


def hedge_adjusted_lp_pnl(lp_pnl_usd: Decimal, realized_hedge_pnl_usd: Decimal, unrealized_hedge_pnl_usd: Decimal) -> Decimal:
    return lp_pnl_usd + realized_hedge_pnl_usd + unrealized_hedge_pnl_usd


def gas_adjusted_net_pnl(pnl_usd: Decimal, gas_drag_usd: Decimal) -> Decimal:
    return pnl_usd - gas_drag_usd
