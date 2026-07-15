import unittest
from decimal import Decimal

from trading_system.research.lp.stress_paths import run_stress_case
from onchain.dex.clmm.schemas import StressScenarioInput


def _inp(vol_multiplier, spot_shock_pct=Decimal("1"), gas_multiplier=Decimal("1"),
         liquidity_haircut_pct=Decimal("0")):
    return StressScenarioInput(
        name="case",
        spot_shock_pct=spot_shock_pct,
        vol_multiplier=vol_multiplier,
        gas_multiplier=gas_multiplier,
        liquidity_haircut_pct=liquidity_haircut_pct,
    )


class TestStressPaths(unittest.TestCase):
    def test_low_vol_multiplier(self):
        res = run_stress_case(_inp(Decimal("1")))
        self.assertEqual(res.hedge_failures, 0)
        self.assertAlmostEqual(float(res.fragility_score), 1 / 3, places=4)

    def test_high_vol_multiplier(self):
        res = run_stress_case(_inp(Decimal("3")))
        self.assertEqual(res.hedge_failures, 1)
        self.assertAlmostEqual(float(res.fragility_score), 1.0, places=4)

    def test_negative_pnl_drawdown(self):
        # Large spot shock makes pnl negative -> max_drawdown positive
        res = run_stress_case(_inp(Decimal("1"), spot_shock_pct=Decimal("100")))
        self.assertLess(res.pnl_usd, Decimal("0"))
        self.assertEqual(res.max_drawdown_usd, abs(res.pnl_usd))

    def test_time_out_of_range_clamped(self):
        res = run_stress_case(_inp(Decimal("1"), spot_shock_pct=Decimal("1000")))
        self.assertEqual(res.time_out_of_range_pct, Decimal("100"))


if __name__ == "__main__":
    unittest.main()
