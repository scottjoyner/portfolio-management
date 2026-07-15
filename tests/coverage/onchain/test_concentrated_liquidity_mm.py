import unittest
from decimal import Decimal

from onchain.strategies.amm_lp.concentrated_liquidity_mm import ConcentratedLiquidityMM, ConcentratedLiquidityMMConfig
from onchain.dex.clmm.schemas import RangeSelectionInput


class TestConcentratedLiquidityMM(unittest.TestCase):
    def test_generate(self):
        s = ConcentratedLiquidityMM(ConcentratedLiquidityMMConfig())
        inp = RangeSelectionInput(mark_price=Decimal("2000"), tick_spacing=10,
                                  realized_vol=Decimal("0.1"), short_vol=Decimal("0.1"),
                                  market_regime="NORMAL", inventory_skew=Decimal("0"),
                                  gas_regime_score=Decimal("0.1"))
        action = s.generate_action(inp)
        self.assertEqual(action["action"], "rebalance_position")
        self.assertIn("range", action)


if __name__ == "__main__":
    unittest.main()
