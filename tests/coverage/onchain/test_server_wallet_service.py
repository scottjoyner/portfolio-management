import unittest
from types import SimpleNamespace

from onchain.wallets.server_wallet.service import ServerWallet, SigningService, NonceManager

KEY = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
ADDR = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"


def fake_signing(init=True):
    return SimpleNamespace(is_initialized=init, address=ADDR, sign_transaction=lambda tx: b"signed")


def fake_adapter():
    return SimpleNamespace(
        build_transaction=lambda to, value, data, gas, gas_price: {"to": to, "value": value},
        send_transaction=lambda signed: "0xhash",
        wait_for_receipt=lambda h, timeout=120: {"status": 1},
        get_balance=lambda addr: 123,
        estimate_gas=lambda to, value=0, data="": 21000,
    )


class TestServerWallet(unittest.TestCase):
    def test_address_none(self):
        w = ServerWallet(name="w")
        self.assertIsNone(w.address)

    def test_address(self):
        w = ServerWallet(name="w")
        w.set_signing_service(fake_signing())
        self.assertEqual(w.address, ADDR)

    def test_no_signing(self):
        w = ServerWallet(name="w")
        with self.assertRaises(RuntimeError):
            w.send("eth", "0x1")

    def test_not_init(self):
        w = ServerWallet(name="w")
        w.set_signing_service(fake_signing(init=False))
        w.register_adapter("eth", fake_adapter())
        with self.assertRaises(RuntimeError):
            w.send("eth", "0x1")

    def test_no_adapter(self):
        w = ServerWallet(name="w")
        w.set_signing_service(fake_signing())
        with self.assertRaises(ValueError):
            w.send("eth", "0x1")

    def test_send(self):
        w = ServerWallet(name="w")
        w.set_signing_service(fake_signing())
        w.set_nonce_manager(SimpleNamespace(consume_nonce=lambda c, a: 3))
        w.register_adapter("eth", fake_adapter())
        h = w.send("eth", "0x1", 0, "0xdata")
        self.assertEqual(h, "0xhash")

    def test_send_no_nonce_manager(self):
        w = ServerWallet(name="w")
        w.set_signing_service(fake_signing())
        w.register_adapter("eth", fake_adapter())
        h = w.send("eth", "0x1", 0, "0xdata")
        self.assertEqual(h, "0xhash")

    def test_send_with_receipt(self):
        w = ServerWallet(name="w")
        w.set_signing_service(fake_signing())
        w.register_adapter("eth", fake_adapter())
        r = w.send_with_receipt("eth", "0x1")
        self.assertEqual(r, {"status": 1})

    def test_send_with_receipt_none_hash(self):
        w = ServerWallet(name="w")
        w.set_signing_service(fake_signing())
        w.register_adapter("eth", SimpleNamespace(
            build_transaction=lambda *a, **k: {}, send_transaction=lambda s: None,
            wait_for_receipt=lambda h, timeout=120: {"status": 1}))
        self.assertIsNone(w.send_with_receipt("eth", "0x1"))

    def test_get_balance_none(self):
        w = ServerWallet(name="w")
        self.assertIsNone(w.get_balance("eth"))

    def test_get_balance(self):
        w = ServerWallet(name="w")
        w.set_signing_service(fake_signing())
        w.register_adapter("eth", fake_adapter())
        self.assertEqual(w.get_balance("eth"), 123)

    def test_estimate_gas_none(self):
        w = ServerWallet(name="w")
        self.assertIsNone(w.estimate_gas("eth", "0x1"))

    def test_estimate_gas(self):
        w = ServerWallet(name="w")
        w.register_adapter("eth", fake_adapter())
        self.assertEqual(w.estimate_gas("eth", "0x1"), 21000)


if __name__ == "__main__":
    unittest.main()
