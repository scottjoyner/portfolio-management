from __future__ import annotations

from decimal import Decimal

from onchain.bridges.adapters.service import BridgeQuote, BridgeService


def quote_bridge(bridge: BridgeService, source_chain: str, destination_chain: str, token: str, amount: Decimal) -> BridgeQuote | None:
    return bridge.quote(source_chain, destination_chain, token, amount)
