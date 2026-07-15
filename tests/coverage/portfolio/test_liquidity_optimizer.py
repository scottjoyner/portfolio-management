import unittest

from portfolio.liquidity_distribution.optimizer import (
    LiquidityInput,
    LiquidityOptimizer,
    LiquidityScore,
)


class TestLiquidityOptimizer(unittest.TestCase):
    def setUp(self):
        self.opt = LiquidityOptimizer()

    def _item(self, **kw):
        base = dict(
            asset="BTC",
            idle_balance=1000.0,
            working_balance=1000.0,
            depth_score=0.5,
            spread_opportunity=0.5,
            friction_score=0.2,
            hedgeability=0.5,
            risk_budget_available=0.5,
        )
        base.update(kw)
        return LiquidityInput(**base)

    def test_score(self):
        score = self.opt.score(self._item())
        self.assertIsInstance(score, LiquidityScore)
        self.assertGreaterEqual(score.usefulness, 0.0)
        self.assertLessEqual(score.usefulness, 1.0)
        self.assertGreaterEqual(score.productivity, 0.0)
        self.assertLessEqual(score.productivity, 1.0)
        self.assertGreaterEqual(score.transfer_necessity, 0.0)
        self.assertLessEqual(score.transfer_necessity, 1.0)

    def test_score_zero_balances(self):
        score = self.opt.score(self._item(idle_balance=0.0, working_balance=0.0))
        self.assertGreater(score.usefulness, 0.0)

    def test_score_high_friction(self):
        score = self.opt.score(self._item(
            idle_balance=1000.0, working_balance=0.0, depth_score=0.0,
            spread_opportunity=0.0, hedgeability=0.0, risk_budget_available=0.0,
            friction_score=1.0))
        self.assertAlmostEqual(score.productivity, 0.0, places=4)

    def test_recommend_move_amount(self):
        amt = self.opt.recommend_move_amount(self._item())
        self.assertGreaterEqual(amt, 0.0)

    def test_recommend_move_amount_zero_idle(self):
        amt = self.opt.recommend_move_amount(self._item(idle_balance=0.0))
        self.assertEqual(amt, 0.0)


if __name__ == "__main__":
    unittest.main()
