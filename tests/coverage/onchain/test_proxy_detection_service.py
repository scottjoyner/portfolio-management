from __future__ import annotations

from unittest import TestCase

from onchain.contracts.proxy_detection.service import (
    ProxyDetectionService,
    ProxyInfo,
)


class TestProxyDetectionService(TestCase):
    def test_register_and_detect_known(self):
        svc = ProxyDetectionService()
        info = ProxyInfo(is_proxy=True, implementation="0xIMPL", proxy_type="uups", confidence=0.9)
        svc.register("0xABC", "base", info)
        self.assertIs(svc.detect("0xABC", "base"), info)

    def test_detect_unknown_returns_default(self):
        svc = ProxyDetectionService()
        detected = svc.detect("0xMISSING", "base")
        self.assertIsInstance(detected, ProxyInfo)
        self.assertFalse(detected.is_proxy)

    def test_register_lowercases_address(self):
        svc = ProxyDetectionService()
        info = ProxyInfo(is_proxy=True)
        svc.register("0xABCDEF", "base", info)
        self.assertIs(svc.detect("0xabcdef", "base"), info)
