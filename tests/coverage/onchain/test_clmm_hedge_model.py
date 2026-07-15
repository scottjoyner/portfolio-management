import unittest
from decimal import Decimal

from onchain.dex.clmm.hedge_model import (
    compute_target_hedge,
    compute_hedge_band,
    should_hedge,
    hedge_urgency_score,
    hedge_notional_after_costs,
    hedge_break_even_move,
    hedge_slippage_budget,
    post_hedge_exposure,
)
from onchain.dex.clmm.schemas import HedgeMode


class TestHedgeModel(unittest.TestCase):
    def test_none(self):
        self.assertEqual(compute_target_hedge(Decimal("100"), HedgeMode.NONE), Decimal("0"))

    def test_delta_neutral(self):
        self.assertEqual(compute_target_hedge(Decimal("100"), HedgeMode.DELTA_NEUTRAL), Decimal("-100"))

    def test_partial(self):
        self.assertEqual(compute_target_hedge(Decimal("100"), HedgeMode.PARTIAL_HEDGE, Decimal("0.5")), Decimal("-50"))

    def test_directional(self):
        self.assertEqual(compute_target_hedge(Decimal("100"), HedgeMode.DIRECTIONAL_BIAS, directional_bias_usd=Decimal("20")), Decimal("-80"))

    def test_emergency(self):
        self.assertEqual(compute_target_hedge(Decimal("100"), HedgeMode.EMERGENCY_FLATTEN), Decimal("-100"))

    def test_band_hedge_unhandled(self):
        self.assertEqual(compute_target_hedge(Decimal("100"), HedgeMode.BAND_HEDGE), Decimal("0"))

    def test_compute_band(self):
        self.assertEqual(compute_hedge_band(Decimal("0.2"), Decimal("100"), Decimal("0.1")),
                         Decimal("100") * Decimal("1.2") * Decimal("1.1"))

    def test_should_hedge(self):
        self.assertTrue(should_hedge(Decimal("100"), Decimal("50"), True))
        self.assertFalse(should_hedge(Decimal("100"), Decimal("50"), False))
        self.assertFalse(should_hedge(Decimal("10"), Decimal("50"), True))

    def test_urgency_zero_band(self):
        self.assertEqual(hedge_urgency_score(Decimal("100"), Decimal("0"), Decimal("0.1")), Decimal("1"))

    def test_urgency(self):
        r = hedge_urgency_score(Decimal("100"), Decimal("50"), Decimal("0.1"))
        self.assertLessEqual(r, Decimal("1"))

    def test_notional_after_costs_yes(self):
        self.assertEqual(hedge_notional_after_costs(Decimal("100"), Decimal("5"), Decimal("10")), Decimal("100"))

    def test_notional_after_costs_no(self):
        self.assertEqual(hedge_notional_after_costs(Decimal("5"), Decimal("5"), Decimal("10")), Decimal("0"))

    def test_break_even_zero(self):
        self.assertEqual(hedge_break_even_move(Decimal("5"), Decimal("0")), Decimal("0"))

    def test_break_even(self):
        self.assertEqual(hedge_break_even_move(Decimal("5"), Decimal("100")), Decimal("0.05"))

    def test_slippage_budget(self):
        self.assertEqual(hedge_slippage_budget(Decimal("0"), Decimal("100")), Decimal("50"))
        self.assertEqual(hedge_slippage_budget(Decimal("2"), Decimal("100")), Decimal("100"))

    def test_post_exposure(self):
        self.assertEqual(post_hedge_exposure(Decimal("100"), Decimal("-50")), Decimal("50"))


if __name__ == "__main__":
    unittest.main()
