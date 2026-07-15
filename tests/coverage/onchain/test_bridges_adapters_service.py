from __future__ import annotations

from decimal import Decimal
from unittest import TestCase

from onchain.bridges.adapters.service import (
    BridgeProtocol,
    BridgeQuote,
    BridgeRisk,
    BridgeService,
    BridgeSettlement,
)


class TestBridgeAdapters(TestCase):
    def test_protocol_enum(self):
        self.assertEqual(BridgeProtocol.CCTP.value, "cctp")

    def test_quote(self):
        svc = BridgeService()
        q = svc.quote("base", "ethereum", "USDC", Decimal("1000"))
        self.assertIsInstance(q, BridgeQuote)
        self.assertEqual(q.source_chain, "base")
        self.assertEqual(q.estimated_gas, Decimal("0.005"))
        self.assertEqual(q.bridge_fee, Decimal("1.0"))
        self.assertEqual(q.protocol, BridgeProtocol.CCTP)

    def test_assess_risk(self):
        svc = BridgeService()
        risk = svc.assess_risk(BridgeQuote(source_chain="base", destination_chain="eth", token="USDC",
                                           amount=Decimal("1"), estimated_gas=Decimal("0"), bridge_fee=Decimal("0"),
                                           estimated_time_minutes=1, protocol=BridgeProtocol.CCTP))
        self.assertIsInstance(risk, BridgeRisk)
        self.assertTrue(risk.approved)

    def test_execute(self):
        svc = BridgeService()
        settle = svc.execute(BridgeQuote(source_chain="base", destination_chain="eth", token="USDC",
                                         amount=Decimal("1"), estimated_gas=Decimal("0"), bridge_fee=Decimal("0"),
                                         estimated_time_minutes=1, protocol=BridgeProtocol.CCTP))
        self.assertIsInstance(settle, BridgeSettlement)
        self.assertEqual(settle.status, "pending")
