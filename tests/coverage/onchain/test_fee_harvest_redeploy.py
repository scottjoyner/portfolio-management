import unittest
from decimal import Decimal

from onchain.strategies.amm_lp.fee_harvest_redeploy import (
    should_compound_fees,
    sweep_priority_score,
    allocate_realized_gains,
    protect_high_water_mark,
    evaluate_profit_sweep,
)
from onchain.dex.clmm.schemas import ProfitSweepDecision


class TestFeeHarvestRedeploy(unittest.TestCase):
    def test_should_compound(self):
        self.assertTrue(should_compound_fees(Decimal("100"), Decimal("20"), Decimal("5"), Decimal("10")))

    def test_should_not_compound_gas(self):
        self.assertFalse(should_compound_fees(Decimal("5"), Decimal("20"), Decimal("50"), Decimal("10")))

    def test_should_not_compound_edge(self):
        self.assertFalse(should_compound_fees(Decimal("100"), Decimal("5"), Decimal("5"), Decimal("10")))

    def test_sweep_priority(self):
        r = sweep_priority_score(Decimal("1000"), Decimal("0.5"), Decimal("0.2"))
        self.assertEqual(r, Decimal("1") + Decimal("0.4") + Decimal("0.2"))

    def test_allocate(self):
        sweep, comp = allocate_realized_gains(Decimal("100"), Decimal("0.7"))
        self.assertEqual(sweep, Decimal("70"))
        self.assertEqual(comp, Decimal("30"))

    def test_allocate_default(self):
        sweep, comp = allocate_realized_gains(Decimal("100"))
        self.assertEqual(sweep, Decimal("70"))

    def test_hwm(self):
        self.assertEqual(protect_high_water_mark(Decimal("100"), Decimal("80")), Decimal("10"))
        self.assertEqual(protect_high_water_mark(Decimal("50"), Decimal("80")), Decimal("0"))

    def test_evaluate(self):
        d = evaluate_profit_sweep(Decimal("200"), Decimal("0.7"), Decimal("20"))
        self.assertIsInstance(d, ProfitSweepDecision)
        self.assertTrue(d.should_sweep)
        self.assertTrue(d.should_compound)

    def test_evaluate_low(self):
        d = evaluate_profit_sweep(Decimal("50"), Decimal("0.9"), Decimal("1"))
        self.assertFalse(d.should_sweep)
        self.assertFalse(d.should_compound)


if __name__ == "__main__":
    unittest.main()
