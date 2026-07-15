from __future__ import annotations

from unittest import TestCase

from onchain.contracts.risk_scoring.service import ContractRiskScore, RiskScoringService


class TestRiskScoringService(TestCase):
    def test_score_all_false(self):
        svc = RiskScoringService()
        res = svc.score("0xA", "base", has_pause=True, age_days=90, tx_count=1000)
        self.assertEqual(res.risk_score, 0.0)
        self.assertFalse(res.is_upgradeable)
        self.assertFalse(res.has_admin_keys)
        self.assertTrue(res.has_pause)
        self.assertEqual(res.factors, {})

    def test_score_all_true(self):
        svc = RiskScoringService()
        res = svc.score(
            "0xA",
            "base",
            is_proxy=True,
            has_admin_keys=True,
            has_pause=False,
            age_days=10,
            tx_count=50,
        )
        self.assertEqual(res.risk_score, 0.85)
        self.assertTrue(res.is_upgradeable)
        self.assertTrue(res.has_admin_keys)
        self.assertFalse(res.has_pause)
        self.assertIn("is_proxy", res.factors)
        self.assertIn("has_admin_keys", res.factors)
        self.assertIn("no_pause", res.factors)
        self.assertIn("young_contract", res.factors)
        self.assertIn("low_usage", res.factors)

    def test_score_capped_at_one(self):
        svc = RiskScoringService()
        # Even maxed factors only sum to 0.85; verify min cap path executes.
        res = svc.score("0xA", "base", is_proxy=True, has_admin_keys=True, has_pause=False, age_days=1, tx_count=1)
        self.assertLessEqual(res.risk_score, 1.0)

    def test_score_partial(self):
        svc = RiskScoringService()
        res = svc.score("0xA", "base", has_admin_keys=True, has_pause=True, age_days=60, tx_count=500)
        self.assertEqual(res.risk_score, 0.3)
        self.assertIn("has_admin_keys", res.factors)
        self.assertNotIn("young_contract", res.factors)
        self.assertNotIn("low_usage", res.factors)
        self.assertTrue(res.has_pause)
