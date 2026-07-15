import asyncio
import unittest

from trading_system.valuation.consensus import ConsensusEstimates


class TestConsensusEstimates(unittest.TestCase):
    def setUp(self):
        self.c = ConsensusEstimates()

    def test_fetch_estimates_returns_and_caches(self):
        out = asyncio.run(self.c.fetch_estimates("AAPL"))
        self.assertIn("mean_eps_estimate", out)
        self.assertIn("AAPL", self.c.cache)
        # second call uses cache
        out2 = asyncio.run(self.c.fetch_estimates("AAPL"))
        self.assertIs(out, out2)

    def test_fetch_estimates_different_symbols(self):
        a = asyncio.run(self.c.fetch_estimates("AAPL"))
        b = asyncio.run(self.c.fetch_estimates("MSFT"))
        self.assertIn("mean_eps_estimate", a)
        self.assertIn("mean_eps_estimate", b)

    def test_get_recommendation_strength_zero_total(self):
        rec, score = self.c.get_recommendation_strength(0, 0, 0)
        self.assertEqual(rec, "unknown")
        self.assertEqual(score, 0.0)

    def test_get_recommendation_strength_strong_buy(self):
        rec, score = self.c.get_recommendation_strength(20, 0, 0)
        self.assertEqual(rec, "strong_buy")
        self.assertEqual(score, 5.0)

    def test_get_recommendation_strength_buy(self):
        rec, score = self.c.get_recommendation_strength(15, 5, 0)
        self.assertEqual(rec, "buy")
        self.assertAlmostEqual(score, 3.8)

    def test_get_recommendation_strength_hold(self):
        rec, score = self.c.get_recommendation_strength(10, 10, 0)
        self.assertEqual(rec, "hold")
        self.assertAlmostEqual(score, 2.5)

    def test_get_recommendation_strength_strong_sell(self):
        rec, score = self.c.get_recommendation_strength(0, 0, 20)
        self.assertEqual(rec, "strong_sell")

    def test_calculate_revision_impact_zero_old(self):
        direction, pct = self.c.calculate_revision_impact("AAPL", 7.0, 0)
        self.assertEqual(direction, "unchanged")
        self.assertEqual(pct, 0.0)

    def test_calculate_revision_impact_significant_upgrade(self):
        direction, pct = self.c.calculate_revision_impact("AAPL", 6.80, 6.0)
        self.assertEqual(direction, "significant_upgrade")
        self.assertGreater(pct, 0)

    def test_calculate_revision_impact_upgrade(self):
        direction, pct = self.c.calculate_revision_impact("AAPL", 6.10, 6.0)
        self.assertEqual(direction, "upgrade")

    def test_calculate_revision_impact_significant_downgrade(self):
        direction, pct = self.c.calculate_revision_impact("AAPL", 5.80, 6.0)
        self.assertEqual(direction, "significant_downgrade")

    def test_calculate_revision_impact_downgrade(self):
        direction, pct = self.c.calculate_revision_impact("AAPL", 5.95, 6.0)
        self.assertEqual(direction, "downgrade")

    def test_calculate_revision_impact_minimal(self):
        direction, pct = self.c.calculate_revision_impact("AAPL", 6.02, 6.0)
        self.assertEqual(direction, "minimal_revision")

    def test_calculate_revision_impact_negative_old(self):
        direction, pct = self.c.calculate_revision_impact("AAPL", -7.0, -6.0)
        self.assertEqual(direction, "significant_downgrade")
        self.assertLess(pct, 0)


if __name__ == "__main__":
    unittest.main()
