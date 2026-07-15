from __future__ import annotations

from unittest import TestCase
from decimal import Decimal

from onchain.security.route_guard import route_allowed


class TestRouteAllowed(TestCase):
    def test_allowed(self):
        self.assertTrue(route_allowed(Decimal("0.5"), Decimal("0.9")))

    def test_fragility_too_high(self):
        self.assertFalse(route_allowed(Decimal("0.8"), Decimal("0.9")))

    def test_trust_too_low(self):
        self.assertFalse(route_allowed(Decimal("0.5"), Decimal("0.6")))

    def test_both_fail(self):
        self.assertFalse(route_allowed(Decimal("0.9"), Decimal("0.5")))

    def test_custom_thresholds(self):
        self.assertTrue(route_allowed(Decimal("0.2"), Decimal("0.99"), Decimal("0.3"), Decimal("0.9")))
        self.assertFalse(route_allowed(Decimal("0.4"), Decimal("0.9"), Decimal("0.3"), Decimal("0.9")))
