from __future__ import annotations

from unittest import TestCase

from onchain.mev_protection.private_tx.service import PrivateTxService


class TestPrivateTxService(TestCase):
    def test_submit_default(self):
        svc = PrivateTxService()
        self.assertIsNone(svc.submit(b"\x01\x02"))

    def test_submit_with_relay(self):
        svc = PrivateTxService(relay_endpoints=["https://relay"])
        self.assertIsNone(svc.submit(b"\x01", relay_url="https://relay"))

    def test_relay_endpoints(self):
        svc = PrivateTxService(relay_endpoints=["a", "b"])
        self.assertEqual(svc.relay_endpoints, ["a", "b"])
