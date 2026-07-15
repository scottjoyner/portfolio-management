import unittest

from onchain.wallets.smart_wallet.service import SmartWalletAdapter, UserOperation


class TestSmartWallet(unittest.TestCase):
    def test_user_operation(self):
        op = UserOperation(sender="0x1", nonce=0)
        self.assertEqual(op.nonce, 0)

    def test_build(self):
        a = SmartWalletAdapter()
        op = a.build_user_operation("0xsender", b"calldata", 5, 10, 2)
        self.assertEqual(op.sender, "0xsender")
        self.assertEqual(len(a._operations), 1)

    def test_supports_entry_point(self):
        a = SmartWalletAdapter()
        self.assertTrue(a.supports_entry_point("0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789"))
        self.assertFalse(a.supports_entry_point("0xOTHER"))


if __name__ == "__main__":
    unittest.main()
