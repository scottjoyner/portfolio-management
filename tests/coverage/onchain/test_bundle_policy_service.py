from __future__ import annotations

from decimal import Decimal
from unittest import TestCase

from onchain.mev_protection.bundle_policy.service import (
    BundlePolicy,
    BundlePolicyEngine,
)


class TestBundlePolicy(TestCase):
    def test_set_and_get_policy(self):
        eng = BundlePolicyEngine()
        policy = BundlePolicy(max_bundles_per_block=5, min_priority_fee_wei=100,
                              max_gas_price_wei=200, require_eth_balance=Decimal("0.5"),
                              allowed_contracts={"0xC"})
        eng.set_policy("fast", policy)
        self.assertIs(eng.get_policy("fast"), policy)

    def test_get_default_policy(self):
        eng = BundlePolicyEngine()
        default = eng.get_policy("default")
        self.assertIsInstance(default, BundlePolicy)
        self.assertEqual(default.max_bundles_per_block, 3)

    def test_default_policy_fields(self):
        p = BundlePolicy()
        self.assertEqual(p.min_priority_fee_wei, 0)
        self.assertEqual(p.max_gas_price_wei, 0)
        self.assertEqual(p.require_eth_balance, Decimal("0.01"))
        self.assertEqual(p.allowed_contracts, set())
