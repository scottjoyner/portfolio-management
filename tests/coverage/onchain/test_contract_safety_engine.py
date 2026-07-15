from __future__ import annotations

from unittest import TestCase

from onchain.contracts.registry.service import ContractRegistry
from onchain.models import ContractProfile, SafetyState
from onchain.security.contract_safety.engine import ContractSafetyEngine


def _profile(risk_score=0.5, upgradeable=False, admin_keys=False, codehash="0xabc123456789") -> ContractProfile:
    return ContractProfile(
        chain="base",
        address="0xABC",
        protocol="p",
        codehash=codehash,
        verified_abi=True,
        upgradeable=upgradeable,
        admin_keys_present=admin_keys,
        risk_score=risk_score,
    )


class TestContractSafetyEngine(TestCase):
    def test_risk_score_no_profile(self):
        eng = ContractSafetyEngine(registry=ContractRegistry())
        self.assertEqual(eng.risk_score("base", "0xUNK"), 0.95)

    def test_risk_score_basic(self):
        reg = ContractRegistry()
        reg.register(_profile(risk_score=0.4))
        eng = ContractSafetyEngine(registry=reg)
        self.assertEqual(eng.risk_score("base", "0xABC"), 0.4)

    def test_risk_score_upgradeable(self):
        reg = ContractRegistry()
        reg.register(_profile(risk_score=0.4, upgradeable=True))
        eng = ContractSafetyEngine(registry=reg)
        self.assertEqual(eng.risk_score("base", "0xABC"), 0.5)

    def test_risk_score_admin_keys(self):
        reg = ContractRegistry()
        reg.register(_profile(risk_score=0.4, admin_keys=True))
        eng = ContractSafetyEngine(registry=reg)
        self.assertEqual(eng.risk_score("base", "0xABC"), 0.5)

    def test_risk_score_capped_at_one(self):
        reg = ContractRegistry()
        reg.register(_profile(risk_score=0.95, upgradeable=True, admin_keys=True))
        eng = ContractSafetyEngine(registry=reg)
        self.assertEqual(eng.risk_score("base", "0xABC"), 1.0)

    def test_ensure_verified_true(self):
        eng = ContractSafetyEngine(registry=ContractRegistry())
        self.assertTrue(eng.ensure_verified(_profile(codehash="0xabcdefghij1234567")))

    def test_ensure_verified_false_short_hash(self):
        eng = ContractSafetyEngine(registry=ContractRegistry())
        self.assertFalse(eng.ensure_verified(_profile(codehash="0xabc")))

    def test_ensure_verified_false_unverified_abi(self):
        eng = ContractSafetyEngine(registry=ContractRegistry())
        prof = _profile(codehash="0xabcdefghij1234567")
        prof.verified_abi = False
        self.assertFalse(eng.ensure_verified(prof))
