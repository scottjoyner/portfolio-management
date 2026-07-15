from __future__ import annotations

from unittest import TestCase
from decimal import Decimal

from onchain.security.wallet_safety.service import WalletSafetyCheck, WalletSafetyService


class TestWalletSafetyCheck(TestCase):
    def test_check_ok(self):
        c = WalletSafetyCheck(wallet="0xW", chain="base")
        self.assertTrue(c.check())
        self.assertTrue(c.is_allowed)

    def test_check_tx_limit(self):
        c = WalletSafetyCheck(wallet="0xW", chain="base", daily_tx_count=100)
        self.assertFalse(c.check())
        self.assertFalse(c.is_allowed)
        self.assertEqual(c.reason, "daily transaction limit exceeded")

    def test_check_volume_limit(self):
        c = WalletSafetyCheck(wallet="0xW", chain="base", daily_volume_usd=Decimal("1000000"))
        self.assertFalse(c.check())
        self.assertFalse(c.is_allowed)
        self.assertEqual(c.reason, "daily volume limit exceeded")

    def test_check_volume_just_below(self):
        c = WalletSafetyCheck(wallet="0xW", chain="base", daily_volume_usd=Decimal("999999"))
        self.assertTrue(c.check())


class TestWalletSafetyService(TestCase):
    def test_allow_and_block(self):
        svc = WalletSafetyService()
        svc.allow_wallet("0xA")
        svc.block_wallet("0xB")
        self.assertIn("0xa", svc.allowlist)
        self.assertIn("0xb", svc.blocklist)

    def test_is_allowed_blocked(self):
        svc = WalletSafetyService()
        svc.block_wallet("0xB")
        self.assertFalse(svc.is_wallet_allowed("0xB"))

    def test_is_allowed_allowlist_enforced(self):
        svc = WalletSafetyService()
        svc.allow_wallet("0xA")
        self.assertTrue(svc.is_wallet_allowed("0xA"))
        self.assertFalse(svc.is_wallet_allowed("0xC"))

    def test_is_allowed_no_allowlist(self):
        svc = WalletSafetyService()
        self.assertTrue(svc.is_wallet_allowed("0xANY"))

    def test_is_allowed_case_insensitive(self):
        svc = WalletSafetyService()
        svc.allow_wallet("0xAa")
        self.assertTrue(svc.is_wallet_allowed("0xaa"))
