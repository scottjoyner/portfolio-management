import unittest
from decimal import Decimal

from onchain.dex.clmm.math import (
    tick_to_price,
    price_to_tick,
    _sqrt_price,
    liquidity_from_amount0,
    liquidity_from_amount1,
    liquidity_from_amounts,
    amounts_from_liquidity,
    position_value,
    position_exposure,
    active_range_fraction,
    rebalance_distance,
    width_bps,
    range_midpoint,
    position_inventory_mix,
)


class TestCLMMMath(unittest.TestCase):
    def test_tick_to_price(self):
        p = tick_to_price(0, 18, 18)
        self.assertEqual(p, Decimal("1"))

    def test_price_to_tick(self):
        t = price_to_tick(Decimal("1"), 1, 18, 18)
        self.assertEqual(t, 0)

    def test_sqrt_price(self):
        self.assertEqual(_sqrt_price(Decimal("4")), Decimal("2"))

    def test_liquidity0_raises(self):
        with self.assertRaises(ValueError):
            liquidity_from_amount0(Decimal("1"), Decimal("2"), Decimal("1"))

    def test_liquidity0_ok(self):
        r = liquidity_from_amount0(Decimal("1"), Decimal("1"), Decimal("2"))
        self.assertEqual(r, Decimal("1") * Decimal("1") * Decimal("2") / (Decimal("2") - Decimal("1")))

    def test_liquidity1_raises(self):
        with self.assertRaises(ValueError):
            liquidity_from_amount1(Decimal("1"), Decimal("2"), Decimal("1"))

    def test_liquidity1_ok(self):
        r = liquidity_from_amount1(Decimal("1"), Decimal("1"), Decimal("2"))
        self.assertEqual(r, Decimal("1") / (Decimal("2") - Decimal("1")))

    def test_liquidity_amounts_low(self):
        # price below lower -> uses amount0
        l = liquidity_from_amounts(Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("4"))
        self.assertEqual(l, liquidity_from_amount0(Decimal("1"), Decimal("1"), Decimal("2")))

    def test_liquidity_amounts_high(self):
        l = liquidity_from_amounts(Decimal("1"), Decimal("1"), Decimal("4"), Decimal("1"), Decimal("4"))
        self.assertEqual(l, liquidity_from_amount1(Decimal("1"), Decimal("2"), Decimal("2")))

    def test_liquidity_amounts_mid(self):
        l = liquidity_from_amounts(Decimal("1"), Decimal("1"), Decimal("2"), Decimal("1"), Decimal("4"))
        self.assertGreater(l, Decimal("0"))

    def test_amounts_low(self):
        a0, a1 = amounts_from_liquidity(Decimal("10"), Decimal("0.5"), Decimal("1"), Decimal("4"))
        self.assertEqual(a1, Decimal("0"))
        self.assertGreater(a0, Decimal("0"))

    def test_amounts_high(self):
        a0, a1 = amounts_from_liquidity(Decimal("10"), Decimal("5"), Decimal("1"), Decimal("4"))
        self.assertEqual(a0, Decimal("0"))
        self.assertGreater(a1, Decimal("0"))

    def test_amounts_mid(self):
        a0, a1 = amounts_from_liquidity(Decimal("10"), Decimal("2"), Decimal("1"), Decimal("4"))
        self.assertGreater(a0, Decimal("0"))
        self.assertGreater(a1, Decimal("0"))

    def test_position_value(self):
        v = position_value(Decimal("10"), Decimal("2"), Decimal("1"), Decimal("4"))
        self.assertGreater(v, Decimal("0"))

    def test_position_exposure(self):
        e = position_exposure(Decimal("10"), Decimal("2"), Decimal("1"), Decimal("4"))
        self.assertIn("token0", e)
        self.assertIn("gamma_proxy", e)

    def test_active_below(self):
        self.assertEqual(active_range_fraction(Decimal("0.5"), Decimal("1"), Decimal("4")), Decimal("0"))

    def test_active_above(self):
        self.assertEqual(active_range_fraction(Decimal("5"), Decimal("1"), Decimal("4")), Decimal("1"))

    def test_active_mid(self):
        self.assertEqual(active_range_fraction(Decimal("2"), Decimal("1"), Decimal("4")), Decimal("1") / Decimal("3"))

    def test_rebalance_in_range(self):
        r = rebalance_distance(Decimal("2"), Decimal("1"), Decimal("4"))
        self.assertEqual(r, Decimal("0.5"))

    def test_rebalance_below(self):
        r = rebalance_distance(Decimal("0.5"), Decimal("1"), Decimal("4"))
        self.assertEqual(r, Decimal("1"))

    def test_rebalance_above(self):
        r = rebalance_distance(Decimal("5"), Decimal("1"), Decimal("4"))
        self.assertEqual(r, Decimal("0.2"))

    def test_width_bps(self):
        self.assertEqual(width_bps(Decimal("1"), Decimal("3")), Decimal("2") / Decimal("2") * Decimal("10000"))

    def test_range_midpoint(self):
        self.assertEqual(range_midpoint(Decimal("1"), Decimal("3")), Decimal("2"))

    def test_inventory_mix_zero(self):
        m = position_inventory_mix(Decimal("0"), Decimal("2"), Decimal("1"), Decimal("4"))
        self.assertEqual(m["token0_weight"], Decimal("0"))
        self.assertEqual(m["token1_weight"], Decimal("0"))

    def test_inventory_mix(self):
        m = position_inventory_mix(Decimal("10"), Decimal("2"), Decimal("1"), Decimal("4"))
        self.assertGreater(m["token0_weight"], Decimal("0"))


if __name__ == "__main__":
    unittest.main()
