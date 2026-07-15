from __future__ import annotations

from decimal import Decimal
from unittest import TestCase

from onchain.bridges.risk.service import compute_bridge_risk


class TestComputeBridgeRisk(TestCase):
    def test_zero_amount(self):
        # amount factor is 0, but base risk from bridge count remains
        self.assertAlmostEqual(compute_bridge_risk(Decimal("0"), 3, ["base", "eth"]), 0.15)

    def test_positive_amount_and_bridges(self):
        risk = compute_bridge_risk(Decimal("1_000_000"), 5, ["base", "eth"])
        self.assertAlmostEqual(risk, min(0.05 * 5 + 1.0 * 0.02, 1.0))

    def test_capped_at_one(self):
        risk = compute_bridge_risk(Decimal("100_000_000"), 50, ["a", "b", "c"])
        self.assertLessEqual(risk, 1.0)

    def test_only_bridges(self):
        risk = compute_bridge_risk(Decimal("0"), 10, [])
        self.assertEqual(risk, 0.5)
