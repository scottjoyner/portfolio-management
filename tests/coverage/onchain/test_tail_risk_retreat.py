import unittest
from decimal import Decimal

from onchain.strategies.amm_lp.tail_risk_retreat import retreat_intensity


class TestTailRiskRetreat(unittest.TestCase):
    def test_intensity(self):
        r = retreat_intensity(Decimal("1"), Decimal("50"))
        self.assertEqual(r, min(Decimal("1"), Decimal("0.4") + Decimal("0.5")))

    def test_cap(self):
        r = retreat_intensity(Decimal("3"), Decimal("200"))
        self.assertEqual(r, Decimal("1"))


if __name__ == "__main__":
    unittest.main()
