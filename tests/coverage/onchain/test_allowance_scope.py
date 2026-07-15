import unittest
from decimal import Decimal

from onchain.wallets.allowance_scope import minimized_allowance


class TestAllowanceScope(unittest.TestCase):
    def test_minimized(self):
        self.assertEqual(minimized_allowance(Decimal("100")), Decimal("102"))

    def test_custom_buffer(self):
        self.assertEqual(minimized_allowance(Decimal("100"), Decimal("0.1")), Decimal("110"))


if __name__ == "__main__":
    unittest.main()
