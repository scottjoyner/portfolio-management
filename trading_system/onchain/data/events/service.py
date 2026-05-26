from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from onchain.chains.evm_generic.base import EVMChainAdapter

log = logging.getLogger(__name__)

TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
SWAP_EVENT_TOPIC_V2 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SWAP_EVENT_TOPIC_V3 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


@dataclass
class EventLog:
    chain: str
    tx_hash: str
    block_number: int
    log_index: int
    address: str
    topics: list[str]
    data: str
    decoded: dict[str, Any] | None = None


@dataclass
class SwapEvent:
    chain: str
    tx_hash: str
    block_number: int
    pool_address: str
    sender: str
    amount0: int
    amount1: int
    sqrt_price_x96: int
    liquidity: int
    tick: int


@dataclass
class EventService:
    adapters: dict[str, EVMChainAdapter] = field(default_factory=dict)

    def register_adapter(self, chain: str, adapter: EVMChainAdapter) -> None:
        self.adapters[chain] = adapter

    def fetch_transfer_events(self, chain: str, token_address: str, from_block: int, to_block: int) -> list[EventLog]:
        adapter = self.adapters.get(chain)
        if adapter is None:
            return []
        try:
            logs = adapter.get_logs(from_block, to_block, address=token_address, topics=[TRANSFER_EVENT_TOPIC])
            return [self._raw_to_event(chain, log) for log in logs]
        except Exception:
            log.exception("failed to fetch transfer events %s/%s [%d,%d]", chain, token_address, from_block, to_block)
            return []

    def fetch_swap_events_v2(self, chain: str, pool_address: str, from_block: int, to_block: int) -> list[SwapEvent]:
        adapter = self.adapters.get(chain)
        if adapter is None:
            return []

        try:
            logs = adapter.get_logs(from_block, to_block, address=pool_address, topics=[SWAP_EVENT_TOPIC_V2])
            return [self._v2_swap_to_event(chain, pool_address, log) for log in logs]
        except Exception:
            log.exception("failed to fetch v2 swap events %s/%s", chain, pool_address)
            return []

    def fetch_swap_events_v3(self, chain: str, pool_address: str, from_block: int, to_block: int) -> list[SwapEvent]:
        adapter = self.adapters.get(chain)
        if adapter is None:
            return []

        try:
            logs = adapter.get_logs(from_block, to_block, address=pool_address, topics=[SWAP_EVENT_TOPIC_V3])
            return [self._v3_swap_to_event(chain, pool_address, log) for log in logs]
        except Exception:
            log.exception("failed to fetch v3 swap events %s/%s", chain, pool_address)
            return []

    def _raw_to_event(self, chain: str, raw: dict[str, Any]) -> EventLog:
        return EventLog(
            chain=chain,
            tx_hash=raw["transactionHash"].hex(),
            block_number=raw["blockNumber"],
            log_index=raw["logIndex"],
            address=raw["address"],
            topics=[t.hex() for t in raw["topics"]],
            data=raw["data"].hex(),
        )

    def _v2_swap_to_event(self, chain: str, pool_address: str, raw: dict[str, Any]) -> SwapEvent:
        topics = raw["topics"]
        sender = "0x" + topics[1].hex()[-40:] if len(topics) > 1 else ""
        return SwapEvent(
            chain=chain,
            tx_hash=raw["transactionHash"].hex(),
            block_number=raw["blockNumber"],
            pool_address=pool_address.lower(),
            sender=sender,
            amount0=0,
            amount1=0,
            sqrt_price_x96=0,
            liquidity=0,
            tick=0,
        )

    def _v3_swap_to_event(self, chain: str, pool_address: str, raw: dict[str, Any]) -> SwapEvent:
        topics = raw["topics"]
        sender = "0x" + topics[1].hex()[-40:] if len(topics) > 1 else ""
        return SwapEvent(
            chain=chain,
            tx_hash=raw["transactionHash"].hex(),
            block_number=raw["blockNumber"],
            pool_address=pool_address.lower(),
            sender=sender,
            amount0=0,
            amount1=0,
            sqrt_price_x96=0,
            liquidity=0,
            tick=0,
        )
