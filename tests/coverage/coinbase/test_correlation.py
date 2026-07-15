import unittest
from coinbase.src.correlation import CorrelationMatrix, CorrelationAwareSizer


class TestCorrelationMatrix(unittest.TestCase):
    def test_get_self(self):
        cm = CorrelationMatrix()
        self.assertEqual(cm.get("BTC-USD", "BTC-USD"), 1.0)

    def test_set_and_get(self):
        cm = CorrelationMatrix()
        cm.set("A", "B", 0.4)
        self.assertEqual(cm.get("A", "B"), 0.4)
        self.assertEqual(cm.get("B", "A"), 0.4)
        self.assertEqual(cm.get("A", "C"), 0.0)

    def test_estimate_default_known(self):
        cm = CorrelationMatrix()
        self.assertAlmostEqual(cm.estimate_default("BTC-USD", "ETH-USD"), 0.65)
        self.assertAlmostEqual(cm.estimate_default("ETH-USD", "SOL-USD"), 0.55)

    def test_estimate_default_same_base(self):
        cm = CorrelationMatrix()
        self.assertEqual(cm.estimate_default("BTC-USD", "BTC-USD"), 1.0)

    def test_estimate_default_unknown(self):
        cm = CorrelationMatrix()
        self.assertEqual(cm.estimate_default("XRP-USD", "ADA-USD"), 0.30)

    def test_compute_from_returns_short(self):
        cm = CorrelationMatrix()
        self.assertEqual(cm.compute_from_returns([0.1], [0.2]), 0.5)

    def test_compute_from_returns_zero_std(self):
        cm = CorrelationMatrix()
        self.assertEqual(cm.compute_from_returns([1, 1, 1], [2, 2, 2]), 0.0)

    def test_compute_from_returns_normal(self):
        cm = CorrelationMatrix()
        r = cm.compute_from_returns([0.1, 0.2, 0.3, 0.4], [0.1, 0.15, 0.1, 0.05])
        self.assertGreaterEqual(r, -1.0)
        self.assertLessEqual(r, 1.0)


class TestCorrelationAwareSizer(unittest.TestCase):
    def test_diversify_empty(self):
        s = CorrelationAwareSizer()
        self.assertEqual(s.diversify_multiplier("BTC-USD", {}), 1.0)

    def test_diversify_with_existing(self):
        s = CorrelationAwareSizer()
        mult = s.diversify_multiplier("BTC-USD", {"ETH-USD": 100.0})
        self.assertLess(mult, 1.0)
        self.assertGreaterEqual(mult, 0.4)

    def test_diversify_total_zero(self):
        s = CorrelationAwareSizer()
        mult = s.diversify_multiplier("BTC-USD", {"ETH-USD": 0.0})
        self.assertEqual(mult, 1.0)

    def test_size_with_correlation(self):
        s = CorrelationAwareSizer()
        self.assertEqual(s.size_with_correlation(100.0, "BTC-USD", {"ETH-USD": 100.0}),
                         100.0 * s.diversify_multiplier("BTC-USD", {"ETH-USD": 100.0}))

    def test_portfolio_heat_few(self):
        s = CorrelationAwareSizer()
        self.assertEqual(s.portfolio_heat([{"product_id": "BTC-USD", "weight": 1.0}]),
                         {"heat": 0.0, "diversification_score": 1.0})

    def test_portfolio_heat_normal(self):
        s = CorrelationAwareSizer()
        res = s.portfolio_heat([
            {"product_id": "BTC-USD", "weight": 0.5},
            {"product_id": "ETH-USD", "weight": 0.5},
        ])
        self.assertIn("heat", res)
        self.assertIn("diversification_score", res)
        self.assertIn("avg_correlation", res)


if __name__ == "__main__":
    unittest.main()
