from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from onchain.chains.evm_generic.base import EVMChainAdapter

log = logging.getLogger(__name__)

ERC20_DECIMALS_ABI: list[dict[str, Any]] = [
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
]

CHAINLINK_AGGREGATOR_ABI: list[dict[str, Any]] = [
    {"inputs": [], "name": "latestRoundData", "outputs": [{"name": "roundId", "type": "uint80"}, {"name": "answer", "type": "int256"}, {"name": "startedAt", "type": "uint256"}, {"name": "updatedAt", "type": "uint256"}, {"name": "answeredInRound", "type": "uint80"}], "type": "function"},
    {"inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
]


@dataclass
class PriceSnapshot:
    token_address: str
    price_usd: Decimal
    decimals: int
    source: str
    block_number: int


@dataclass
class PriceService:
    adapters: dict[str, EVMChainAdapter] = field(default_factory=dict)
    chainlink_feeds: dict[str, dict[str, str]] = field(default_factory=dict)

    def register_adapter(self, chain: str, adapter: EVMChainAdapter) -> None:
        self.adapters[chain] = adapter

    def set_chainlink_feed(self, chain: str, token_address: str, feed_address: str) -> None:
        self.chainlink_feeds.setdefault(chain, {})[token_address.lower()] = feed_address

    def fetch_usd_price(self, chain: str, token_address: str) -> PriceSnapshot | None:
        adapter = self.adapters.get(chain)
        if adapter is None:
            return None

        feed_address = self.chainfeeds_for(chain).get(token_address.lower())
        if feed_address:
            return self._from_chainlink(adapter, chain, token_address, feed_address)

        return None

    def chainfeeds_for(self, chain: str) -> dict[str, str]:
        return self.chainlink_feeds.get(chain, {})

    def _from_chainlink(self, adapter: EVMChainAdapter, chain: str, token_address: str, feed_address: str) -> PriceSnapshot | None:
        try:
            _, answer, _, updated_at, _ = adapter.call_contract(feed_address, CHAINLINK_AGGREGATOR_ABI, "latestRoundData")
            feed_decimals = adapter.call_contract(feed_address, CHAINLINK_AGGREGATOR_ABI, "decimals")
            block = adapter.get_block_number()
            divisor = 10**feed_decimals
            price = Decimal(str(answer)) / Decimal(str(divisor))
            return PriceSnapshot(
                token_address=token_address.lower(),
                price_usd=price,
                decimals=feed_decimals,
                source=f"chainlink:{chain}",
                block_number=block,
            )
        except Exception:
            log.exception("failed to fetch chainlink price %s/%s", chain, token_address)
            return None
