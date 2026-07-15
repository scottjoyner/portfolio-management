"""Tests for trading_system.exchange.coinbase.auth.jwt."""
import base64
import unittest

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization

from trading_system.exchange.coinbase.auth.jwt import (
    _b64url,
    _load_private_key,
    build_jwt_token,
)


def make_ec_pem() -> str:
    key = ec.generate_private_key(ec.SECP256R1())
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


class TestJwt(unittest.IsolatedAsyncioTestCase):
    def test_b64url(self):
        self.assertNotIn("=", _b64url(b"hello"))

    def test_load_private_key_valid(self):
        pem = make_ec_pem()
        key = _load_private_key(pem.encode())
        self.assertTrue(hasattr(key, "private_numbers"))

    def test_load_private_key_invalid(self):
        from cryptography.hazmat.primitives.asymmetric import rsa
        rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        rsa_pem = rsa_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        with self.assertRaises(TypeError):
            _load_private_key(rsa_pem)

    def test_build_jwt_raw_pem(self):
        pem = make_ec_pem()
        token = build_jwt_token("mykey", pem, "GET", "/ws")
        parts = token.split(".")
        self.assertEqual(len(parts), 3)

    def test_build_jwt_base64_pem(self):
        pem = make_ec_pem()
        b64 = base64.b64encode(pem.encode()).decode()
        token = build_jwt_token("mykey", b64, "GET", "/ws")
        self.assertEqual(len(token.split(".")), 3)

    def test_build_jwt_with_body(self):
        pem = make_ec_pem()
        token = build_jwt_token("mykey", pem, "POST", "/orders", body='{"a":1}')
        parts = token.split(".")
        self.assertEqual(len(parts), 3)
        # body claim present in payload
        import json
        payload = json.loads(_b64url_decode(parts[1]))
        self.assertIn("body", payload)


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


if __name__ == "__main__":
    unittest.main()
