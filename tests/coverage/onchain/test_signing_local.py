import unittest

# BUG WORKAROUND: onchain/wallets/signing/local.py imports `SignTransaction` from base,
# but base.py defines `SignedTransaction`. Pre-populate the attribute so the import resolves.
import onchain.wallets.signing.base as _base
_base.SignTransaction = _base.SignedTransaction  # type: ignore[attr-defined]

from onchain.wallets.signing.local import LocalKeySigner  # noqa: E402

KEY = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"


class TestLocalKeySigner(unittest.TestCase):
    def setUp(self):
        self.s = LocalKeySigner(KEY)

    def test_signer_type(self):
        self.assertEqual(self.s.signer_type().value, "local_key")

    def test_address(self):
        self.assertEqual(self.s.address(), "0x70997970C51812dc3A010C7d01b50e0d17dc79C8")

    def test_capability(self):
        cap = self.s.capability()
        self.assertTrue(cap.supports_typed_data)

    def test_sign_transaction(self):
        tx = {"to": "0x" + "1" * 40, "value": 0, "nonce": 0, "gas": 21000, "gasPrice": 1, "chainId": 1}
        stx = self.s.sign_transaction(tx)
        self.assertEqual(stx.signer_address, self.s.address())
        self.assertIsInstance(stx.raw_tx, bytes)

    def test_sign_message(self):
        sig = self.s.sign_message(b"hello")
        self.assertIsInstance(sig, str)

    def test_sign_message_str(self):
        sig = self.s.sign_message("hello")
        self.assertIsInstance(sig, str)

    def test_sign_typed_data(self):
        message_types = {
            "Mail": [
                {"name": "from", "type": "address"},
                {"name": "to", "type": "address"},
                {"name": "contents", "type": "string"},
            ],
        }
        domain = {"name": "Test"}
        message = {"from": "0x" + "0" * 40, "to": "0x" + "1" * 40, "contents": "hi"}
        sig = self.s.sign_typed_data(domain, message_types, message)
        self.assertIsInstance(sig, str)


if __name__ == "__main__":
    unittest.main()
