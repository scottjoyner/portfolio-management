import base64
import hashlib
import hmac
import unittest
from unittest import mock

from cryptography.fernet import Fernet

from trading_system.plaid import services as svc


class TestPlaidServices(unittest.IsolatedAsyncioTestCase):
    def test_plaid_service_default_key(self):
        s = svc.PlaidService("cid", environment="sandbox")
        self.assertIsNotNone(s.fernet)

    def test_plaid_service_explicit_key(self):
        key = Fernet.generate_key()
        s = svc.PlaidService("cid", environment="production", encryption_key=key)
        self.assertEqual(s.environment, "production")

    def test_plaid_service_no_crypto_raises(self):
        orig = svc.CRYPTOGRAPHY_AVAILABLE
        svc.CRYPTOGRAPHY_AVAILABLE = False
        try:
            self.assertRaises(ImportError, svc.PlaidService, "cid")
        finally:
            svc.CRYPTOGRAPHY_AVAILABLE = orig

    async def test_create_link_token(self):
        s = svc.PlaidService("cid")
        res = await s.create_link_token(product_scope=["auth"])
        self.assertEqual(res["status"], "success")
        self.assertIn("link_token", res)

    async def test_link_item(self):
        s = svc.PlaidService("cid")
        res = await s.link_item("i1", public_token="pt")
        self.assertEqual(res["item_id"], "i1")

    async def test_refresh_item(self):
        s = svc.PlaidService("cid")
        res = await s.refresh_item("i1")
        self.assertEqual(res["status"], "success")

    async def test_get_accounts(self):
        s = svc.PlaidService("cid")
        res = await s.get_accounts("i1")
        self.assertEqual(res, [])

    async def test_revoke_item(self):
        s = svc.PlaidService("cid")
        res = await s.revoke_item("i1")
        self.assertEqual(res["status"], "success")

    async def test_verify_webhook_signature(self):
        """A forged signature must fail; a real one must pass.

        This asserted True for the literal signature "sig". That was the stub
        behaviour before the HMAC verification landed -- a verify_* function
        that always authenticated, which is worse than none because callers
        trust it.
        """
        s = svc.PlaidService("cid")
        payload, secret = b"x", "secret"
        good = base64.b64encode(
            hmac.new(secret.encode(), payload, hashlib.sha256).digest()
        ).decode()
        self.assertTrue(await s.verify_webhook_signature(payload, good, secret))
        # Forged, wrong secret, wrong body, and missing inputs all refuse.
        self.assertFalse(await s.verify_webhook_signature(payload, "sig", secret))
        self.assertFalse(await s.verify_webhook_signature(payload, good, "other"))
        self.assertFalse(await s.verify_webhook_signature(b"tampered", good, secret))
        self.assertFalse(await s.verify_webhook_signature(payload, "", secret))
        self.assertFalse(await s.verify_webhook_signature(payload, good, ""))

    # ---- CredentialVault ----
    def test_credential_vault_roundtrip(self):
        key = Fernet.generate_key()
        vault = svc.CredentialVault(key)
        enc = vault.encrypt_token("secret-token")
        self.assertEqual(vault.decrypt_token(enc), "secret-token")

    def test_credential_vault_no_crypto_raises(self):
        orig = svc.CRYPTOGRAPHY_AVAILABLE
        svc.CRYPTOGRAPHY_AVAILABLE = False
        try:
            vault = svc.CredentialVault(b"0" * 44)
            self.assertRaises(ImportError, vault.encrypt_token, "t")
            self.assertRaises(ImportError, vault.decrypt_token, b"x")
        finally:
            svc.CRYPTOGRAPHY_AVAILABLE = orig

    # ---- VaultManager ----
    async def test_vault_manager_initialize(self):
        vm = svc.VaultManager()
        res = await vm.initialize("cid", "sandbox", Fernet.generate_key())
        self.assertTrue(res["vault_initialized"])

    async def test_vault_manager_store_credentials(self):
        vm = svc.VaultManager()
        await vm.store_credentials({"client_id": "cid"})
        self.assertIsNone(vm.credentials_encrypted)

    # ---- utility functions ----
    def test_generate_encryption_key(self):
        key = svc.generate_encryption_key()
        self.assertIsInstance(key, bytes)

    def test_validate_token_expiration_needed(self):
        from datetime import datetime, timezone, timedelta
        expiry = datetime.now(timezone.utc) - timedelta(hours=1)
        self.assertTrue(svc.validate_token_expiration(expiry))

    def test_validate_token_expiration_not_needed(self):
        from datetime import datetime, timezone, timedelta
        expiry = datetime.now(timezone.utc) + timedelta(days=10)
        self.assertFalse(svc.validate_token_expiration(expiry))

    # ---- initialize_plaid_service ----
    async def test_initialize_no_credentials(self):
        s = await svc.initialize_plaid_service("cid")
        self.assertIsInstance(s, svc.PlaidService)

    async def test_initialize_with_credentials(self):
        key = Fernet.generate_key()
        s = await svc.initialize_plaid_service("cid", credentials_encrypted=key)
        self.assertIsInstance(s, svc.PlaidService)


if __name__ == "__main__":
    unittest.main()
