from decimal import Decimal

from onchain.dex.clmm.math import (
    amounts_from_liquidity,
    liquidity_from_amounts,
    price_to_tick,
    tick_to_price,
    width_bps,
)


def test_tick_price_roundtrip():
    p = Decimal("2350")
    tick = price_to_tick(p, 10, 18, 18)
    p2 = tick_to_price(tick, 18, 18)
    assert abs((p2 - p) / p) < Decimal("0.01")


def test_liquidity_amount_roundtrip():
    liquidity = liquidity_from_amounts(Decimal("1"), Decimal("2000"), Decimal("2000"), Decimal("1800"), Decimal("2200"))
    a0, a1 = amounts_from_liquidity(liquidity, Decimal("2000"), Decimal("1800"), Decimal("2200"))
    assert a0 >= 0 and a1 >= 0


def test_width_bps():
    assert width_bps(Decimal("1900"), Decimal("2100")) > 900
