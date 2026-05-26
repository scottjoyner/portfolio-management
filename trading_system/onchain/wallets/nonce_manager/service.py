from __future__ import annotations

import logging
from dataclasses import dataclass, field

from web3 import Web3
from onchain.chains.evm_generic.base import EVMChainAdapter

log = logging.getLogger(__name__)


@dataclass
class NonceManager:
    adapters: dict[str, EVMChainAdapter] = field(default_factory=dict)
    _local_nonces: dict[str, int] = field(default_factory=dict)

    def register_adapter(self, chain: str, adapter: EVMChainAdapter) -> None:
        self.adapters[chain] = adapter

    def get_nonce(self, chain: str, address: str) -> int:
        local = self._local_nonces.get(chain, 0)
        onchain = self._onchain_nonce(chain, address)
        return max(local, onchain)

    def _onchain_nonce(self, chain: str, address: str) -> int:
        adapter = self.adapters.get(chain)
        if adapter is None:
            return 0
        try:
            return adapter.w3.eth.get_transaction_count(Web3.to_checksum_address(address))
        except Exception:
            log.exception("failed to fetch onchain nonce for %s/%s", chain, address)
            return 0

    def consume_nonce(self, chain: str, address: str) -> int:
        nonce = self.get_nonce(chain, address)
        self._local_nonces[chain] = nonce + 1
        return nonce

    def reset_local(self, chain: str) -> None:
        self._local_nonces.pop(chain, None)
