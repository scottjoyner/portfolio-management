import unittest
from eth_account import Account

from onchain.wallets.signing.base import SignerType, SignerCapability, SignedTransaction, Signer


class DummySigner(Signer):
    def signer_type(self):
        return SignerType.LOCAL_KEY

    def address(self):
        return "0xabc"

    def capability(self):
        return SignerCapability(signer_type=SignerType.LOCAL_KEY, address="0xabc")

    def sign_transaction(self, tx):
        return SignedTransaction(raw_tx=b"", tx_hash="0xh", signer_address="0xabc", signer_type=SignerType.LOCAL_KEY)

    def sign_message(self, message):
        return "0xsig"


class TestSigningBase(unittest.TestCase):
    def test_enums(self):
        self.assertEqual(SignerType.LOCAL_KEY.value, "local_key")
        self.assertEqual(SignerType.KMS.value, "kms")

    def test_dataclasses(self):
        cap = SignerCapability(signer_type=SignerType.HSM, address="0x1")
        self.assertTrue(cap.supports_typed_data)
        st = SignedTransaction(raw_tx=b"x", tx_hash="0xh", signer_address="0x1", signer_type=SignerType.HSM)
        self.assertEqual(st.raw_tx, b"x")

    def test_typed_data_not_implemented(self):
        s = DummySigner()
        with self.assertRaises(NotImplementedError):
            s.sign_typed_data({}, {}, {})

    def test_recover_address(self):
        acct = Account.create()
        s = DummySigner()
        from eth_account.messages import encode_defunct
        sig = Account.from_key(acct.key).sign_message(encode_defunct(b"hello")).signature.hex()
        recovered = s.recover_address(b"hello", sig)
        self.assertEqual(recovered.lower(), acct.address.lower())
        recovered2 = s.recover_address("hello", sig)
        self.assertEqual(recovered2.lower(), acct.address.lower())


if __name__ == "__main__":
    unittest.main()
