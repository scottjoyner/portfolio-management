import unittest
from decimal import Decimal

from onchain.dex.clmm.il_model import (
    il_vs_hold_benchmark,
    lp_pnl_decomposition,
    hedge_adjusted_lp_pnl,
    gas_adjusted_net_pnl,
)


class TestILModel(unittest.TestCase):
    def test_il(self):
        r = il_vs_hold_benchmark(Decimal("100"), Decimal("121"), Decimal("1"), Decimal("1"))
        hold = Decimal("1") * Decimal("121") + Decimal("1")
        lp = Decimal("2") * (Decimal("1") * Decimal("121")).sqrt()
        self.assertEqual(r, lp - hold)

    def test_decomp_nonzero_edge(self):
        d = lp_pnl_decomposition(Decimal("100"), Decimal("200"), Decimal("10"),
                                 Decimal("5"), Decimal("2"), Decimal("1"),
                                 Decimal("1"), Decimal("1"), Decimal("50"))
        self.assertIn("net_pnl_usd", d)
        self.assertIn("fee_to_il_ratio", d)

    def test_decomp_zero_edge(self):
        d = lp_pnl_decomposition(Decimal("100"), Decimal("100"), Decimal("0"),
                                 Decimal("0"), Decimal("0"), Decimal("0"),
                                 Decimal("0"), Decimal("0"), Decimal("0"))
        self.assertEqual(d["fee_to_il_ratio"], Decimal("0"))

    def test_hedge_adjusted(self):
        self.assertEqual(hedge_adjusted_lp_pnl(Decimal("10"), Decimal("1"), Decimal("2")), Decimal("13"))

    def test_gas_adjusted(self):
        self.assertEqual(gas_adjusted_net_pnl(Decimal("10"), Decimal("3")), Decimal("7"))


if __name__ == "__main__":
    unittest.main()
