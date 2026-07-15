import unittest

from onchain.wallets.signing.service import SigningService

KEY = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
ADDR = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"


class TestSigningService(unittest.TestCase):
    def test_not_initialized_address(self):
        s = SigningService()
        self.assertFalse(s.is_initialized)
        with self.assertRaises(RuntimeError):
            _ = s.address

    def test_not_initialized_tx(self):
        s = SigningService()
        with self.assertRaises(RuntimeError):
            s.sign_transaction({"to": "0x1"})

    def test_not_initialized_message(self):
        s = SigningService()
        with self.assertRaises(RuntimeError):
            s.sign_message(b"x")

    def test_not_initialized_typed(self):
        s = SigningService()
        with self.assertRaises(RuntimeError):
            s.sign_typed_data({}, {}, {})

    def test_initialized(self):
        s = SigningService.from_key(KEY)
        self.assertTrue(s.is_initialized)
        self.assertEqual(s.address, ADDR)
        self.assertIsInstance(s.sign_transaction({"to": "0x" + "1" * 40, "value": 0, "nonce": 0, "gas": 21000, "gasPrice": 1, "chainId": 1}), bytes)
        self.assertIsInstance(s.sign_message(b"hi"), str)
        message_types = {
            "Mail": [
                {"name": "from", "type": "address"},
                {"name": "to", "type": "address"},
                {"name": "contents", "type": "string"},
            ],
        }
        domain = {"name": "Test"}
        message = {"from": "0x" + "0" * 40, "to": "0x" + "1" * 40, "contents": "hi"}
        self.assertIsInstance(s.sign_typed_data(domain, message_types, message), str)

    def test_recover(self):
        s = SigningService.from_key(KEY)
        sig = s.sign_message(b"hello")
        self.assertEqual(s.recover_address(b"hello", sig), ADDR)
        self.assertEqual(s.recover_address("hello", sig), ADDR)


if __name__ == "__main__":
    unittest.main()
