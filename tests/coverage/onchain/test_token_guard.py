from __future__ import annotations

from unittest import TestCase
from decimal import Decimal

from onchain.security.token_guard import token_allowed


class TestTokenAllowed(TestCase):
    def test_allowed_equal(self):
        self.assertTrue(token_allowed(Decimal("0.75")))

    def test_allowed_above(self):
        self.assertTrue(token_allowed(Decimal("0.9"), Decimal("0.75")))

    def test_not_allowed_below(self):
        self.assertFalse(token_allowed(Decimal("0.74"), Decimal("0.75")))

    def test_custom_min(self):
        self.assertTrue(token_allowed(Decimal("0.5"), Decimal("0.4")))
        self.assertFalse(token_allowed(Decimal("0.3"), Decimal("0.4")))
