from __future__ import annotations

from decimal import Decimal
from unittest import TestCase

from onchain.mev_protection.sandwich_risk.service import (
    estimate_sandwich_loss,
    sandwich_risk_score,
)


class TestSandwichRisk(TestCase):
    def test_estimate_zero_liquidity(self):
        self.assertEqual(estimate_sandwich_loss(Decimal("100"), Decimal("0")), Decimal("100"))

    def test_estimate_normal(self):
        loss = estimate_sandwich_loss(Decimal("1000"), Decimal("100000"))
        # impact = 0.01 -> 100 bps + 10 bps = 110 bps -> 1000*110/10000 = 11.0
        self.assertAlmostEqual(loss, Decimal("11.0"))

    def test_score_zero_liquidity(self):
        self.assertEqual(sandwich_risk_score(Decimal("100"), Decimal("0")), 1.0)

    def test_score_normal(self):
        score = sandwich_risk_score(Decimal("1000"), Decimal("100000"))
        self.assertAlmostEqual(score, 1.0)  # ratio*100 = 1.0 -> min(1.0,1.0)

    def test_score_small_ratio(self):
        score = sandwich_risk_score(Decimal("10"), Decimal("100000"))
        self.assertAlmostEqual(score, 0.01)
