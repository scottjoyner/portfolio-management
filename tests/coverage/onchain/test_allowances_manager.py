import unittest

from onchain.wallets.allowances.manager import AllowanceManager


class TestAllowanceManager(unittest.TestCase):
    def setUp(self):
        self.m = AllowanceManager()

    def test_set_get(self):
        self.m.set_allowance("eth", "W1", "T1", "S1", 500.0)
        self.assertEqual(self.m.get_allowance("eth", "w1", "t1", "s1"), 500.0)

    def test_get_default(self):
        self.assertEqual(self.m.get_allowance("eth", "W1", "T1", "S1"), 0.0)

    def test_minimize(self):
        self.assertEqual(self.m.minimize_approval(100.0, 50.0), 50.0)
        self.assertEqual(self.m.minimize_approval(30.0, 50.0), 30.0)
        self.assertEqual(self.m.minimize_approval(-5.0, 50.0), 0.0)

    def test_revoke(self):
        self.m.set_allowance("eth", "W1", "T1", "S1", 500.0)
        self.m.revoke("eth", "W1", "T1", "S1")
        self.assertEqual(self.m.get_allowance("eth", "W1", "T1", "S1"), 0.0)


if __name__ == "__main__":
    unittest.main()
