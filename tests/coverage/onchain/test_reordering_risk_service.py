from __future__ import annotations

from unittest import TestCase

from onchain.mev_protection.reordering_risk.service import (
    ReorderingRisk,
    assess_reordering_risk,
)


class TestReorderingRisk(TestCase):
    def test_low_risk(self):
        r = assess_reordering_risk(tx_priority_fee=0, sandwich_risk_score=0.0, pool_liquidity_usd=10**15)
        self.assertAlmostEqual(r.score, 0.0)
        self.assertAlmostEqual(r.top_of_block_risk, 0.0)
        self.assertAlmostEqual(r.sandwich_risk, 0.0)
        self.assertFalse(r.high_risk)

    def test_high_priority_fee(self):
        r = assess_reordering_risk(tx_priority_fee=10_000_000_000, sandwich_risk_score=0.0, pool_liquidity_usd=1_000_000)
        self.assertGreater(r.top_of_block_risk, 0.0)
        self.assertFalse(r.high_risk)

    def test_high_combined_risk(self):
        r = assess_reordering_risk(tx_priority_fee=200_000_000_000, sandwich_risk_score=1.0, pool_liquidity_usd=1000)
        self.assertGreater(r.score, 0.6)
        self.assertTrue(r.high_risk)

    def test_zero_liquidity(self):
        r = assess_reordering_risk(tx_priority_fee=0, sandwich_risk_score=0.5, pool_liquidity_usd=0)
        self.assertAlmostEqual(r.score, 0.45)
        self.assertFalse(r.high_risk)

    def test_sandwich_field(self):
        r = ReorderingRisk(score=0.1, sandwich_risk=0.2)
        self.assertEqual(r.sandwich_risk, 0.2)
