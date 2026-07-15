from __future__ import annotations

from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock

from onchain.bridges.adapters.service import BridgeQuote, BridgeService
from onchain.bridges.quoting.service import quote_bridge


class TestQuoteBridge(TestCase):
    def test_quote_bridge(self):
        bridge = MagicMock(spec=BridgeService)
        expected = BridgeQuote(source_chain="base", destination_chain="eth", token="USDC",
                               amount=Decimal("1"), estimated_gas=Decimal("0"), bridge_fee=Decimal("0"),
                               estimated_time_minutes=1, protocol=MagicMock())
        bridge.quote.return_value = expected
        result = quote_bridge(bridge, "base", "eth", "USDC", Decimal("1000"))
        self.assertIs(result, expected)
        bridge.quote.assert_called_once_with("base", "eth", "USDC", Decimal("1000"))
