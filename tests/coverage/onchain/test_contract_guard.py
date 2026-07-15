from __future__ import annotations

from unittest import TestCase
from decimal import Decimal

from onchain.security.contract_guard import contract_allowed


class TestContractAllowed(TestCase):
    def test_allowed_equal_to_min(self):
        self.assertTrue(contract_allowed(Decimal("0.8")))

    def test_allowed_above_min(self):
        self.assertTrue(contract_allowed(Decimal("0.9"), Decimal("0.8")))

    def test_not_allowed_below_min(self):
        self.assertFalse(contract_allowed(Decimal("0.79"), Decimal("0.8")))

    def test_custom_min(self):
        self.assertTrue(contract_allowed(Decimal("0.5"), Decimal("0.4")))
        self.assertFalse(contract_allowed(Decimal("0.3"), Decimal("0.4")))
