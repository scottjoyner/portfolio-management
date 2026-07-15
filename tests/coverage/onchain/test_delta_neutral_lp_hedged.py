import unittest
from decimal import Decimal

from onchain.strategies.amm_lp.delta_neutral_lp_hedged import DeltaNeutralLPHedged, DeltaNeutralLPHedgedConfig
from onchain.dex.clmm.schemas import HedgeMode, HedgePlan


class TestDeltaNeutralLP(unittest.TestCase):
    def test_hedge_plan(self):
        s = DeltaNeutralLPHedged(DeltaNeutralLPHedgedConfig())
        plan = s.hedge_plan(Decimal("500"))
        self.assertIsInstance(plan, HedgePlan)
        self.assertEqual(plan.hedge_mode, HedgeMode.DELTA_NEUTRAL)


if __name__ == "__main__":
    unittest.main()
