import unittest

from trading_system.valuation.fundamental import FundamentalMetrics


class TestFundamentalMetrics(unittest.TestCase):
    def setUp(self):
        self.m = FundamentalMetrics()

    def test_pe_ratio_raises_on_nonpositive_eps(self):
        with self.assertRaises(ValueError):
            self.m.calculate_pe_ratio("X", 100.0, 0.0)
        with self.assertRaises(ValueError):
            self.m.calculate_pe_ratio("X", 100.0, -1.0)

    def test_pe_ratio_fair(self):
        pe, sig = self.m.calculate_pe_ratio("X", 250.0, 10.0)
        self.assertAlmostEqual(pe, 25.0)
        self.assertEqual(sig, "fair_valuation")

    def test_pe_ratio_undervalued(self):
        pe, sig = self.m.calculate_pe_ratio("X", 100.0, 10.0)
        self.assertEqual(sig, "undervalued")

    def test_pe_ratio_overvalued(self):
        pe, sig = self.m.calculate_pe_ratio("X", 1000.0, 10.0)
        self.assertEqual(sig, "overvalued")

    def test_pb_ratio_raises(self):
        with self.assertRaises(ValueError):
            self.m.calculate_pb_ratio("X", 100.0, 0.0)

    def test_pb_ratio_significantly_undervalued(self):
        pb, sig = self.m.calculate_pb_ratio("X", 10.0, 10.0)
        self.assertEqual(sig, "significantly_undervalued")

    def test_pb_ratio_undervalued(self):
        pb, sig = self.m.calculate_pb_ratio("X", 12.0, 10.0)
        self.assertEqual(sig, "undervalued")

    def test_pb_ratio_overvalued(self):
        pb, sig = self.m.calculate_pb_ratio("X", 40.0, 10.0)
        self.assertEqual(sig, "overvalued")

    def test_pb_ratio_fair(self):
        pb, sig = self.m.calculate_pb_ratio("X", 25.0, 10.0)
        self.assertEqual(sig, "fair_valuation")

    def test_ev_ebitda_raises(self):
        with self.assertRaises(ValueError):
            self.m.calculate_ev_ebitda("X", 100.0, 0.0)

    def test_ev_ebitda_undervalued(self):
        ev, sig = self.m.calculate_ev_ebitda("X", 100.0, 10.0)
        self.assertEqual(sig, "undervalued")

    def test_ev_ebitda_overvalued(self):
        ev, sig = self.m.calculate_ev_ebitda("X", 1000.0, 10.0)
        self.assertEqual(sig, "overvalued")

    def test_ev_ebitda_fair(self):
        ev, sig = self.m.calculate_ev_ebitda("X", 220.0, 10.0)
        self.assertEqual(sig, "fair_valuation")

    def test_dividend_yield_raises(self):
        with self.assertRaises(ValueError):
            self.m.calculate_dividend_yield("X", 0.0, 1.0)
        with self.assertRaises(ValueError):
            self.m.calculate_dividend_yield("X", 10.0, -1.0)

    def test_dividend_yield_low(self):
        y, sig = self.m.calculate_dividend_yield("X", 100.0, 1.0)
        self.assertEqual(sig, "low_yield")

    def test_dividend_yield_high(self):
        y, sig = self.m.calculate_dividend_yield("X", 100.0, 10.0)
        self.assertEqual(sig, "high_yield_watch")

    def test_dividend_yield_normal(self):
        y, sig = self.m.calculate_dividend_yield("X", 100.0, 3.0)
        self.assertEqual(sig, "normal_range")

    def test_fcf_yield_raises(self):
        with self.assertRaises(ValueError):
            self.m.calculate_free_cash_flow_yield("X", 0.0, 10.0)

    def test_fcf_yield_below_average(self):
        y, sig = self.m.calculate_free_cash_flow_yield("X", 1000.0, 10.0)
        self.assertEqual(sig, "below_average")

    def test_fcf_yield_above_average(self):
        y, sig = self.m.calculate_free_cash_flow_yield("X", 100.0, 20.0)
        self.assertEqual(sig, "above_average")

    def test_fcf_yield_normal(self):
        y, sig = self.m.calculate_free_cash_flow_yield("X", 100.0, 4.0)
        self.assertEqual(sig, "normal_range")


if __name__ == "__main__":
    unittest.main()
