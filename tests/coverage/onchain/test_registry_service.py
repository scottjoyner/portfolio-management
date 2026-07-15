from __future__ import annotations

from unittest import TestCase

from onchain.contracts.registry.service import ContractRegistry
from onchain.models import ContractProfile, SafetyState


def _profile(address="0xABC", state=SafetyState.WATCHED, selectors=None) -> ContractProfile:
    return ContractProfile(
        chain="base",
        address=address,
        protocol="p",
        codehash="0xhash",
        selectors_allowlist=set(selectors or []),
        safety_state=state,
    )


class TestContractRegistry(TestCase):
    def test_register_and_lookup(self):
        reg = ContractRegistry()
        reg.register(_profile())
        self.assertIsNotNone(reg.lookup("base", "0xABC"))
        self.assertIsNone(reg.lookup("base", "0xMISSING"))

    def test_is_allowed_denylist(self):
        reg = ContractRegistry()
        reg.register(_profile())
        reg.deny("base", "0xABC")
        self.assertFalse(reg.is_allowed("base", "0xABC"))

    def test_is_allowed_unknown(self):
        reg = ContractRegistry()
        self.assertFalse(reg.is_allowed("base", "0xMISSING"))

    def test_is_allowed_quarantined(self):
        reg = ContractRegistry()
        reg.register(_profile(state=SafetyState.QUARANTINED))
        self.assertFalse(reg.is_allowed("base", "0xABC"))

    def test_is_allowed_denied(self):
        reg = ContractRegistry()
        reg.register(_profile(state=SafetyState.DENIED))
        self.assertFalse(reg.is_allowed("base", "0xABC"))

    def test_is_allowed_selector_blocked(self):
        reg = ContractRegistry()
        reg.register(_profile(selectors=["0xaaa"]))
        self.assertFalse(reg.is_allowed("base", "0xABC", selector="0xbbb"))

    def test_is_allowed_selector_ok(self):
        reg = ContractRegistry()
        reg.register(_profile(selectors=["0xaaa"]))
        self.assertTrue(reg.is_allowed("base", "0xABC", selector="0xaaa"))

    def test_is_allowed_no_allowlist(self):
        reg = ContractRegistry()
        reg.register(_profile())
        self.assertTrue(reg.is_allowed("base", "0xABC"))

    def test_set_state(self):
        reg = ContractRegistry()
        reg.register(_profile())
        reg.set_state("base", "0xABC", SafetyState.TRUSTED)
        self.assertEqual(reg.lookup("base", "0xABC").safety_state, SafetyState.TRUSTED)

    def test_set_state_unknown_noop(self):
        reg = ContractRegistry()
        reg.set_state("base", "0xMISSING", SafetyState.TRUSTED)  # no error
