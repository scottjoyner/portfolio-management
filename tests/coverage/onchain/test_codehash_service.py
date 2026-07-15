from __future__ import annotations

from unittest import TestCase

from onchain.contracts.codehash.service import CodeHashEntry, CodeHashService


def _hash_of(code: bytes) -> str:
    import hashlib
    return "0x" + hashlib.sha256(code).hexdigest()


class TestCodeHashService(TestCase):
    def test_register_and_get(self):
        svc = CodeHashService()
        entry = CodeHashEntry(address="0xABC", chain="base", codehash="0xdead", verified=True)
        svc.register(entry)
        self.assertIs(svc.get("base", "0xabc"), entry)
        self.assertIsNone(svc.get("base", "0xmissing"))

    def test_verify_match(self):
        code = b"\x60\x00\x60\x00"
        svc = CodeHashService()
        svc.register(CodeHashEntry(address="0xABC", chain="base", codehash=_hash_of(code)))
        self.assertTrue(svc.verify("base", "0xABC", code))

    def test_verify_mismatch(self):
        code = b"\x60\x00"
        svc = CodeHashService()
        svc.register(CodeHashEntry(address="0xABC", chain="base", codehash="0xother"))
        self.assertFalse(svc.verify("base", "0xABC", code))

    def test_verify_no_entry(self):
        svc = CodeHashService()
        self.assertFalse(svc.verify("base", "0xABC", b"code"))

    def test_verify_no_runtime_code(self):
        svc = CodeHashService()
        svc.register(CodeHashEntry(address="0xABC", chain="base", codehash="0xother"))
        self.assertFalse(svc.verify("base", "0xABC", None))
