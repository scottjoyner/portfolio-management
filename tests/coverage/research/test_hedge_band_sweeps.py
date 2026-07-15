import unittest
from decimal import Decimal

from trading_system.research.lp.hedge_band_sweeps import sweep_bands


class TestHedgeBandSweeps(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(sweep_bands([], Decimal("100")), {})

    def test_sweep(self):
        result = sweep_bands([Decimal("0.1"), Decimal("0.2")], Decimal("100"))
        self.assertIn("0.1", result)
        self.assertIn("0.2", result)
        self.assertIsInstance(result["0.1"], Decimal)


if __name__ == "__main__":
    unittest.main()
