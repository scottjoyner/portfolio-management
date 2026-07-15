import unittest

from onchain.models import Opportunity, ActionType
from onchain.strategies.execution.opportunity_ranker import OpportunityRanker


def make_opp(gross_edge=10.0, capital=1000.0, confidence=0.9, age_ms=0):
    return Opportunity(
        opportunity_id="o1", strategy_name="s", chain="eth", protocol="uni",
        action_type=ActionType.SWAP, token_pair="ETH/USDC", gross_edge=gross_edge,
        capital_required=capital, confidence=confidence, age_ms=age_ms,
    )


class TestOpportunityRanker(unittest.TestCase):
    def test_rank_executable(self):
        r = OpportunityRanker().rank([make_opp(gross_edge=10.0, capital=1000.0, confidence=0.9, age_ms=100)])
        self.assertEqual(len(r), 1)
        self.assertTrue(r[0].executable)
        self.assertIsNone(r[0].reject_reason)

    def test_rank_not_executable_edge(self):
        r = OpportunityRanker(min_net_edge=50.0).rank([make_opp(gross_edge=10.0)])
        self.assertFalse(r[0].executable)
        self.assertEqual(r[0].reject_reason, "insufficient_net_or_trust")

    def test_rank_not_executable_trust(self):
        r = OpportunityRanker().rank([make_opp(gross_edge=100.0, capital=1000.0, confidence=0.1, age_ms=20000)])
        self.assertFalse(r[0].executable)

    def test_rank_sort(self):
        r = OpportunityRanker().rank([make_opp(gross_edge=5.0), make_opp(gross_edge=50.0)])
        self.assertGreater(r[0].estimated_net_edge, r[1].estimated_net_edge)


if __name__ == "__main__":
    unittest.main()
