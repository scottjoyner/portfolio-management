from __future__ import annotations

from decimal import Decimal, getcontext
import math

getcontext().prec = 42

TICK_BASE = Decimal("1.0001")


def _to_decimal(v: float | int | Decimal) -> Decimal:
    return v if isinstance(v, Decimal) else Decimal(str(v))


def tick_to_price(tick: int, decimals0: int, decimals1: int) -> Decimal:
    raw = TICK_BASE ** Decimal(tick)
    scale = Decimal(10) ** Decimal(decimals0 - decimals1)
    return raw * scale


def price_to_tick(price: Decimal | float, tick_spacing: int, decimals0: int, decimals1: int) -> int:
    p = _to_decimal(price)
    scale = Decimal(10) ** Decimal(decimals0 - decimals1)
    normalized = p / scale
    tick = int(math.log(float(normalized), float(TICK_BASE)))
    return (tick // tick_spacing) * tick_spacing


def _sqrt_price(price: Decimal | float) -> Decimal:
    return _to_decimal(price).sqrt()


def liquidity_from_amount0(amount0: Decimal, sqrt_pa: Decimal, sqrt_pb: Decimal) -> Decimal:
    if sqrt_pb <= sqrt_pa:
        raise ValueError("invalid range")
    return amount0 * sqrt_pa * sqrt_pb / (sqrt_pb - sqrt_pa)


def liquidity_from_amount1(amount1: Decimal, sqrt_pa: Decimal, sqrt_pb: Decimal) -> Decimal:
    if sqrt_pb <= sqrt_pa:
        raise ValueError("invalid range")
    return amount1 / (sqrt_pb - sqrt_pa)


def liquidity_from_amounts(
    amount0: Decimal,
    amount1: Decimal,
    price: Decimal,
    lower_price: Decimal,
    upper_price: Decimal,
) -> Decimal:
    sp = _sqrt_price(price)
    sa = _sqrt_price(lower_price)
    sb = _sqrt_price(upper_price)
    if sp <= sa:
        return liquidity_from_amount0(amount0, sa, sb)
    if sp >= sb:
        return liquidity_from_amount1(amount1, sa, sb)
    l0 = liquidity_from_amount0(amount0, sp, sb)
    l1 = liquidity_from_amount1(amount1, sa, sp)
    return min(l0, l1)


def amounts_from_liquidity(
    liquidity: Decimal,
    price: Decimal,
    lower_price: Decimal,
    upper_price: Decimal,
) -> tuple[Decimal, Decimal]:
    sp = _sqrt_price(price)
    sa = _sqrt_price(lower_price)
    sb = _sqrt_price(upper_price)
    if sp <= sa:
        amount0 = liquidity * (sb - sa) / (sa * sb)
        return amount0, Decimal("0")
    if sp >= sb:
        amount1 = liquidity * (sb - sa)
        return Decimal("0"), amount1
    amount0 = liquidity * (sb - sp) / (sp * sb)
    amount1 = liquidity * (sp - sa)
    return amount0, amount1


def position_value(
    liquidity: Decimal,
    price: Decimal,
    lower_price: Decimal,
    upper_price: Decimal,
    quote_price_token1: Decimal = Decimal("1"),
) -> Decimal:
    a0, a1 = amounts_from_liquidity(liquidity, price, lower_price, upper_price)
    return a0 * price * quote_price_token1 + a1 * quote_price_token1


def position_exposure(liquidity: Decimal, price: Decimal, lower_price: Decimal, upper_price: Decimal) -> dict[str, Decimal]:
    a0, a1 = amounts_from_liquidity(liquidity, price, lower_price, upper_price)
    delta_token0 = a0
    delta_token1 = a1 - a0 * price
    gamma_proxy = liquidity / max(_to_decimal(price), Decimal("1e-12"))
    return {"token0": a0, "token1": a1, "delta_token0": delta_token0, "delta_token1": delta_token1, "gamma_proxy": gamma_proxy}


def active_range_fraction(price: Decimal, lower_price: Decimal, upper_price: Decimal) -> Decimal:
    p = _to_decimal(price)
    if p <= lower_price:
        return Decimal("0")
    if p >= upper_price:
        return Decimal("1")
    return (p - lower_price) / (upper_price - lower_price)


def rebalance_distance(price: Decimal, lower_price: Decimal, upper_price: Decimal) -> Decimal:
    p = _to_decimal(price)
    if lower_price <= p <= upper_price:
        return min((p - lower_price) / p, (upper_price - p) / p)
    if p < lower_price:
        return (lower_price - p) / p
    return (p - upper_price) / p


def width_bps(lower_price: Decimal, upper_price: Decimal) -> Decimal:
    mid = range_midpoint(lower_price, upper_price)
    return (upper_price - lower_price) / mid * Decimal("10000")


def range_midpoint(lower_price: Decimal, upper_price: Decimal) -> Decimal:
    return (lower_price + upper_price) / Decimal("2")


def position_inventory_mix(liquidity: Decimal, price: Decimal, lower_price: Decimal, upper_price: Decimal) -> dict[str, Decimal]:
    a0, a1 = amounts_from_liquidity(liquidity, price, lower_price, upper_price)
    v0 = a0 * price
    total = v0 + a1
    if total == 0:
        return {"token0_weight": Decimal("0"), "token1_weight": Decimal("0")}
    return {"token0_weight": v0 / total, "token1_weight": a1 / total}
