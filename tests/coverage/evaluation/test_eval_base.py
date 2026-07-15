import unittest

from trading_system.evaluation.base import (
    Action,
    Philosophy,
    Evidence,
    AgentResult,
    BaseAgent,
)


class _Concrete(BaseAgent):
    agent_name = "test"

    def evaluate(self, instrument, market_data):
        return AgentResult(
            agent_name=self.agent_name,
            instrument=instrument,
            action=Action.BUY,
            confidence=0.5,
            rationale="r",
            risk_score=0.1,
            philosophy=Philosophy.VALUE,
            holding_period_hint="1d",
        )


class TestEvaluationBase(unittest.TestCase):
    def test_enums(self):
        self.assertEqual(Action.STRONG_BUY.value, "strong_buy")
        self.assertEqual(Philosophy.MEAN_REVERSION.value, "mean_reversion")

    def test_agent_result_defaults(self):
        ar = AgentResult(
            agent_name="a",
            instrument="BTC-USD",
            action=Action.HOLD,
            confidence=0.0,
            rationale="x",
            risk_score=0.0,
            philosophy=Philosophy.GROWTH,
            holding_period_hint="intraday",
        )
        self.assertEqual(ar.evidence, [])
        self.assertIsNone(ar.dissenting)
        self.assertEqual(ar.model_version, "1.0.0")
        self.assertIsNotNone(ar.created_at)
        self.assertEqual(ar.dissenting, None)

    def test_evidence(self):
        e = Evidence(source="s", metric="m", value=1.0, weight=2.0)
        self.assertEqual(e.weight, 2.0)

    def test_base_agent_abstract(self):
        self.assertTrue(issubclass(_Concrete, BaseAgent))
        # BaseAgent itself cannot be instantiated (abstract)
        with self.assertRaises(TypeError):
            BaseAgent()


if __name__ == "__main__":
    unittest.main()
