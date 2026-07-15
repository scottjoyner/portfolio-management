import os
import sys
import unittest
from unittest.mock import MagicMock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from _env import install_stubs  # noqa: E402

install_stubs()

from evaluation.service import EvaluationService, _action_to_rating  # noqa: E402
from evaluation.base import Action  # noqa: E402


class TestActionToRating(unittest.TestCase):
    def test_mapping(self):
        cases = {
            "strong_buy": "BUY", "buy": "BUY", "hold": "HOLD", "reduce": "SELL",
            "sell": "SELL", "exit": "SELL", "watch": "HOLD",
        }
        for action, rating in cases.items():
            self.assertEqual(_action_to_rating(Action(action)), rating)


class TestEvaluationService(unittest.TestCase):
    def setUp(self):
        self.repo = MagicMock(name="repo")
        self.svc = EvaluationService(self.repo)

    def _md(self, dcf=65000.0, technical=72.0, sentiment=0.25):
        return {
            "current_price": 60000.0, "entry_price": 55000.0,
            "volatility_1h": 0.025, "volume_24h": 1_500_000,
            "value_at_risk": 0.08, "current_drawdown": 0.05,
            "correlation_to_index": 0.75, "spread_bps": 8.0,
            "backtest_sharpe": 1.2, "backtest_max_drawdown": 0.15,
            "dcf_intrinsic_value": dcf, "technical_score": technical,
            "sentiment_score": sentiment,
        }

    def test_evaluate_instrument_with_estimate(self):
        out = self.svc.evaluate_instrument("BTC-USD", self._md())
        self.assertEqual(out["instrument"], "BTC-USD")
        self.assertIn("consensus", out)
        self.assertEqual(len(out["agents"]), 7)
        self.assertIn("evidence", out)
        self.repo.db.add.assert_called()
        self.repo.db.commit.assert_called()

    def test_evaluate_instrument_no_estimate(self):
        out = self.svc.evaluate_instrument("BTC-USD", self._md(dcf=0.0, technical=0.0))
        self.assertEqual(out["instrument"], "BTC-USD")

    def test_evaluate_instrument_bullish(self):
        out = self.svc.evaluate_instrument("BTC-USD", self._md(sentiment=0.5))
        self.assertIn(
            out["consensus"]["philosophy"],
            ("value", "momentum", "mean_reversion", "growth", "hedge", "market_making"),
        )

    def test_evaluate_instrument_bearish(self):
        self.svc.evaluate_instrument("BTC-USD", self._md(sentiment=-0.5))

    def test_evaluate_instrument_neutral(self):
        self.svc.evaluate_instrument("BTC-USD", self._md(sentiment=0.1))

    def test_evaluate_portfolio_found(self):
        self.repo.get_portfolio.return_value = MagicMock()
        out = self.svc.evaluate_portfolio("p1")
        self.assertEqual(out["portfolio_id"], "p1")
        self.assertEqual(out["total_instruments"], 3)
        self.assertIn("BTC-USD", out["results"])

    def test_evaluate_portfolio_not_found(self):
        self.repo.get_portfolio.return_value = None
        out = self.svc.evaluate_portfolio("p1")
        self.assertEqual(out, {"portfolio_id": "p1", "error": "not found"})


if __name__ == "__main__":
    unittest.main()
