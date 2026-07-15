from __future__ import annotations

from unittest import TestCase

from onchain.contracts.verification.service import VerificationResult, VerificationService


class TestVerificationService(TestCase):
    def test_mark_and_is_verified_true(self):
        svc = VerificationService()
        svc.mark_verified(VerificationResult(address="0xABC", chain="base", verified=True, source="etherscan"))
        self.assertTrue(svc.is_verified("base", "0xABC"))

    def test_is_verified_false_unverified(self):
        svc = VerificationService()
        svc.mark_verified(VerificationResult(address="0xABC", chain="base", verified=False))
        self.assertFalse(svc.is_verified("base", "0xABC"))

    def test_is_verified_unknown(self):
        svc = VerificationService()
        self.assertFalse(svc.is_verified("base", "0xMISSING"))

    def test_lowercase_address(self):
        svc = VerificationService()
        svc.mark_verified(VerificationResult(address="0xABCDEF", chain="base", verified=True))
        self.assertTrue(svc.is_verified("base", "0xabcdef"))
