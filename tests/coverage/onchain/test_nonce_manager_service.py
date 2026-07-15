import unittest
from types import SimpleNamespace

from onchain.wallets.nonce_manager.service import NonceManager


def fake_adapter(nonce):
    return SimpleNamespace(w3=SimpleNamespace(eth=SimpleNamespace(get_transaction_count=lambda addr: nonce)))


class TestNonceManager(unittest.TestCase):
    def test_no_adapter(self):
        nm = NonceManager()
        # onchain nonce returns 0 (no adapter)
        self.assertEqual(nm.get_nonce("eth", "0x" + "0" * 40), 0)

    def test_with_adapter(self):
        nm = NonceManager()
        nm.register_adapter("eth", fake_adapter(5))
        self.assertEqual(nm.get_nonce("eth", "0x" + "0" * 40), 5)

    def test_consume(self):
        nm = NonceManager()
        n = nm.consume_nonce("eth", "0x" + "0" * 40)
        self.assertEqual(n, 0)
        self.assertEqual(nm.get_nonce("eth", "0x" + "0" * 40), 1)

    def test_consume_with_adapter(self):
        nm = NonceManager()
        nm.register_adapter("eth", fake_adapter(5))
        n = nm.consume_nonce("eth", "0x" + "0" * 40)
        self.assertEqual(n, 5)
        self.assertEqual(nm.get_nonce("eth", "0x" + "0" * 40), 6)

    def test_adapter_exception(self):
        class Bad:
            w3 = SimpleNamespace(eth=SimpleNamespace(get_transaction_count=lambda addr: (_ for _ in ()).throw(Exception("boom"))))
        nm = NonceManager()
        nm.register_adapter("eth", Bad())
        self.assertEqual(nm.get_nonce("eth", "0x" + "0" * 40), 0)

    def test_reset(self):
        nm = NonceManager()
        nm.consume_nonce("eth", "0x" + "0" * 40)
        nm.reset_local("eth")
        self.assertEqual(nm.get_nonce("eth", "0x" + "0" * 40), 0)


if __name__ == "__main__":
    unittest.main()
