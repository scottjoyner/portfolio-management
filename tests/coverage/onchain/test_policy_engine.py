import unittest

from onchain.wallets.policy_engine.engine import WalletPolicy, WalletPolicyEngine


def make_policy(wallet="W1"):
    return WalletPolicy(
        wallet=wallet, daily_spend_limit=1000.0, per_contract_cap=500.0,
        per_token_cap=300.0, allowance_cap=200.0, bridge_limit=100.0,
    )


class TestPolicyEngine(unittest.TestCase):
    def test_missing_policy(self):
        e = WalletPolicyEngine()
        ok, reason = e.approve_spend("W1", 10, 5, 5, 5)
        self.assertFalse(ok)
        self.assertEqual(reason, "wallet policy missing")

    def test_approved(self):
        e = WalletPolicyEngine()
        e.register_policy(make_policy())
        ok, reason = e.approve_spend("W1", 10, 5, 5, 5)
        self.assertTrue(ok)
        self.assertEqual(reason, "approved")
        self.assertEqual(e.daily_spent["W1"], 10.0)

    def test_daily_limit(self):
        e = WalletPolicyEngine()
        e.register_policy(make_policy())
        ok, reason = e.approve_spend("W1", 1500, 5, 5, 5)
        self.assertFalse(ok)
        self.assertEqual(reason, "daily spend limit exceeded")

    def test_per_contract(self):
        e = WalletPolicyEngine()
        e.register_policy(make_policy())
        ok, reason = e.approve_spend("W1", 10, 600, 5, 5)
        self.assertFalse(ok)
        self.assertEqual(reason, "per-contract cap exceeded")

    def test_per_token(self):
        e = WalletPolicyEngine()
        e.register_policy(make_policy())
        ok, reason = e.approve_spend("W1", 10, 5, 600, 5)
        self.assertFalse(ok)
        self.assertEqual(reason, "per-token cap exceeded")

    def test_allowance(self):
        e = WalletPolicyEngine()
        e.register_policy(make_policy())
        ok, reason = e.approve_spend("W1", 10, 5, 5, 600)
        self.assertFalse(ok)
        self.assertEqual(reason, "allowance cap exceeded")

    def test_bridge(self):
        e = WalletPolicyEngine()
        e.register_policy(make_policy())
        ok, reason = e.approve_spend("W1", 10, 5, 5, 5, bridge_spend=600)
        self.assertFalse(ok)
        self.assertEqual(reason, "bridge cap exceeded")


if __name__ == "__main__":
    unittest.main()
