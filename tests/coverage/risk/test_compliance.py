import unittest
from datetime import datetime, timezone

from trading_system.risk.compliance.service import ComplianceCheck, ComplianceService


class TestComplianceService(unittest.TestCase):
    def setUp(self):
        self.svc = ComplianceService()

    def test_block_unblock_wallet(self):
        self.svc.block_wallet("WalletA")
        self.assertIn("walleta", self.svc.blocked_wallets)
        self.svc.unblock_wallet("WALLETA")
        self.assertNotIn("walleta", self.svc.blocked_wallets)

    def test_block_unblock_product(self):
        self.svc.block_product("BTC-USD")
        self.assertIn("BTC-USD", self.svc.blocked_products)
        self.svc.unblock_product("BTC-USD")
        self.assertNotIn("BTC-USD", self.svc.blocked_products)

    def test_check_wallet_blocked(self):
        self.svc.block_wallet("bad")
        c = self.svc.check("BAD", "trade", "BTC-USD")
        self.assertIsInstance(c, ComplianceCheck)
        self.assertFalse(c.passed)
        self.assertEqual(c.reason, "wallet is blocked")
        self.assertIsInstance(c.checked_at, datetime)

    def test_check_product_blocked(self):
        self.svc.block_product("BTC-USD")
        c = self.svc.check("goodwallet", "trade", "BTC-USD")
        self.assertFalse(c.passed)
        self.assertEqual(c.reason, "product BTC-USD is blocked")

    def test_check_passed(self):
        c = self.svc.check("goodwallet", "trade", "BTC-USD")
        self.assertTrue(c.passed)
        self.assertEqual(c.reason, "")

    def test_check_no_product(self):
        c = self.svc.check("goodwallet", "trade")
        self.assertTrue(c.passed)


if __name__ == "__main__":
    unittest.main()
