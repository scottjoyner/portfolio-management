from onchain.wallets.signing.service import SigningService
from onchain.wallets.nonce_manager.service import NonceManager


def test_signing_service_from_key():
    key = "0x" + "ab" * 32
    svc = SigningService.from_key(key)
    assert svc.is_initialized
    assert svc.address.startswith("0x")


def test_signing_message():
    key = "0x" + "cd" * 32
    svc = SigningService.from_key(key)
    sig = svc.sign_message(b"hello")
    assert isinstance(sig, str)
    assert len(sig) > 0


def test_sign_transaction():
    key = "0x" + "ef" * 32
    svc = SigningService.from_key(key)
    tx = {"to": "0x" + "01" * 20, "value": 1000, "gas": 21000, "gasPrice": 1000000000, "nonce": 0, "chainId": 1}
    signed = svc.sign_transaction(tx)
    assert isinstance(signed, bytes)


def test_nonce_manager():
    nm = NonceManager()
    nonce = nm.consume_nonce("ethereum", "0x" + "aa" * 20)
    assert nonce >= 0
    next_nonce = nm.consume_nonce("ethereum", "0x" + "aa" * 20)
    assert next_nonce == nonce + 1


def test_nonce_reset():
    nm = NonceManager()
    nm.consume_nonce("base", "0x" + "bb" * 20)
    nm.consume_nonce("base", "0x" + "bb" * 20)
    nm.reset_local("base")
    assert nm._local_nonces.get("base") is None
