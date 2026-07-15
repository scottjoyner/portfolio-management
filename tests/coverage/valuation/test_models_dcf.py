import asyncio
import unittest

from trading_system.valuation.models.dcf import DCFCalculation


class TestDCFCalculation(unittest.TestCase):
    def setUp(self):
        self.dcf = DCFCalculation()

    def test_get_beta(self):
        self.assertEqual(asyncio.run(self.dcf._get_beta("AAPL")), 1.0)

    def test_project_cash_flows_small_growth(self):
        cf = self.dcf._project_cash_flows(1000.0, 5, 1.0)
        self.assertEqual(len(cf), 5)
        # growth rate floored at 0.02
        self.assertAlmostEqual(cf[0], 1000.0 * 1.02 * (1.02) ** 1, places=4)

    def test_project_cash_flows_normal_growth(self):
        cf = self.dcf._project_cash_flows(1000.0, 5, 2.5)
        self.assertEqual(len(cf), 5)
        self.assertGreater(cf[-1], cf[0])

    def test_discount_cash_flows(self):
        pv = self.dcf._discount_cash_flows([100.0, 100.0], 10.0)
        self.assertEqual(len(pv), 2)
        self.assertLess(pv[1], pv[0])

    def test_calculate_terminal_value_capped(self):
        tv = self.dcf._calculate_terminal_value(100.0, 2.5, 2.0)
        self.assertEqual(tv, 100.0 * 100)

    def test_calculate_terminal_value_normal(self):
        tv = self.dcf._calculate_terminal_value(100.0, 2.5, 10.5)
        self.assertGreater(tv, 0)
        self.assertNotEqual(tv, 100.0 * 100)

    def test_sensitivity_growth(self):
        self.assertGreater(self.dcf._sensitivity_growth(1000.0, 0.1), 0)

    def test_sensitivity_wacc(self):
        self.assertLess(self.dcf._sensitivity_wacc(1000.0, 500.0, 5, 100.0, 2.5), 0)

    def test_calculate_intrinsic_value_with_wacc(self):
        out = asyncio.run(self.dcf.calculate_intrinsic_value("AAPL", wacc=0.1))
        self.assertIn("intrinsic_value", out)
        self.assertEqual(out["wacc_used"], 0.1)
        self.assertIsNone(out["current_price"])

    def test_calculate_intrinsic_value_default_wacc(self):
        out = asyncio.run(self.dcf.calculate_intrinsic_value("AAPL"))
        self.assertGreater(out["wacc_used"], 0)

    def test_calculate_intrinsic_value_capped_terminal(self):
        out = asyncio.run(self.dcf.calculate_intrinsic_value("AAPL", wacc=0.02, terminal_growth_rate=2.5))
        self.assertIn("intrinsic_value", out)
        self.assertIn("sensitivity_analysis", out)


if __name__ == "__main__":
    unittest.main()
