from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from onchain.chains.evm_generic.base import EVMChainAdapter

log = logging.getLogger(__name__)

ERC20_METADATA_ABI: list[dict[str, Any]] = [
    {"constant": True, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "totalSupply", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
]


@dataclass
class TokenMetadata:
    chain: str
    address: str
    name: str
    symbol: str
    decimals: int
    total_supply: int
    block_number: int


@dataclass
class TokenService:
    adapters: dict[str, EVMChainAdapter] = field(default_factory=dict)
    _cache: dict[str, TokenMetadata] = field(default_factory=dict)

    def register_adapter(self, chain: str, adapter: EVMChainAdapter) -> None:
        self.adapters[chain] = adapter

    def _cache_key(self, chain: str, address: str) -> str:
        return f"{chain}:{address.lower()}"

    def fetch_metadata(self, chain: str, token_address: str) -> TokenMetadata | None:
        adapter = self.adapters.get(chain)
        if adapter is None:
            return None

        key = self._cache_key(chain, token_address)
        cached = self._cache.get(key)
        if cached:
            return cached

        try:
            abi = ERC20_METADATA_ABI
            name = adapter.call_contract(token_address, abi, "name")
            symbol = adapter.call_contract(token_address, abi, "symbol")
            decimals = adapter.call_contract(token_address, abi, "decimals")
            total_supply = adapter.call_contract(token_address, abi, "totalSupply")
            block = adapter.get_block_number()
            meta = TokenMetadata(
                chain=chain,
                address=token_address.lower(),
                name=name,
                symbol=symbol,
                decimals=decimals,
                total_supply=total_supply,
                block_number=block,
            )
            self._cache[key] = meta
            return meta
        except Exception:
            log.exception("failed to fetch token metadata %s/%s", chain, token_address)
            return None

    def invalidate(self, chain: str, address: str) -> None:
        self._cache.pop(self._cache_key(chain, address), None)
