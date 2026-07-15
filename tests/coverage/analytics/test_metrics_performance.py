import unittest

from analytics.metrics.performance import basic_metrics


class TestPerformanceMetrics(unittest.TestCase):
    def test_empty(self):
        out = basic_metrics([])
        self.assertEqual(out, {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0})

    def test_basic(self):
        out = basic_metrics([0.01, -0.02, 0.03])
        self.assertIn("return", out)
        self.assertIn("sharpe", out)
        self.assertIn("max_drawdown", out)
        self.assertLessEqual(out["max_drawdown"], 0.0)

    def test_flat(self):
        out = basic_metrics([0.0, 0.0, 0.0])
        self.assertEqual(out["return"], 0.0)


if __name__ == "__main__":
    unittest.main()
