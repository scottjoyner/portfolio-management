import unittest

from analytics.tearsheets.service import Tearsheet


class TestTearsheet(unittest.TestCase):
    def test_add_metric_and_chart(self):
        t = Tearsheet(title="My Tearsheet")
        self.assertEqual(t.title, "My Tearsheet")
        t.add_metric("sharpe", 1.5)
        t.add_chart("equity", [1, 2, 3])
        self.assertEqual(t.metrics["sharpe"], 1.5)
        self.assertEqual(t.charts["equity"], [1, 2, 3])

    def test_defaults(self):
        t = Tearsheet(title="x")
        self.assertEqual(t.metrics, {})
        self.assertEqual(t.charts, {})


if __name__ == "__main__":
    unittest.main()
