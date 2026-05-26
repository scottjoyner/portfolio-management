from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from onchain.chains.evm_generic.base import EVMChainAdapter

log = logging.getLogger(__name__)

UNISWAP_V2_PAIR_ABI: list[dict[str, Any]] = [
    {
        "constant": True,
        "inputs": [],
        "name": "getReserves",
        "outputs": [{"name": "_reserve0", "type": "uint112"}, {"name": "_reserve1", "type": "uint112"}, {"name": "_blockTimestampLast", "type": "uint32"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "token0",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "token1",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
]

UNISWAP_V3_POOL_ABI: list[dict[str, Any]] = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "observationIndex", "type": "uint16"},
            {"name": "observationCardinality", "type": "uint16"},
            {"name": "observationCardinalityNext", "type": "uint16"},
            {"name": "feeProtocol", "type": "uint8"},
            {"name": "unlocked", "type": "bool"},
        ],
        "type": "function",
    },
    {
        "inputs": [],
        "name": "liquidity",
        "outputs": [{"name": "", "type": "uint128"}],
        "type": "function",
    },
    {
        "inputs": [],
        "name": "fee",
        "outputs": [{"name": "", "type": "uint24"}],
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function",
    },
]


ChainId = int


@dataclass
class PoolSnapshot:
    chain: str
    pool_address: str
    protocol: str
    token0: str
    token1: str
    reserve0: int
    reserve1: int
    sqrt_price_x96: int | None
    tick: int | None
    liquidity: int | None
    fee_bps: int
    block_number: int


@dataclass
class PoolDataService:
    adapters: dict[str, EVMChainAdapter] = field(default_factory=dict)
    _cache: dict[str, PoolSnapshot] = field(default_factory=dict)

    def register_adapter(self, chain: str, adapter: EVMChainAdapter) -> None:
        self.adapters[chain] = adapter

    def _cache_key(self, chain: str, address: str) -> str:
        return f"{chain}:{address.lower()}"

    async def fetch_pool_snapshot(self, chain: str, pool_address: str, protocol: str) -> PoolSnapshot | None:
        adapter = self.adapters.get(chain)
        if adapter is None:
            return None

        key = self._cache_key(chain, pool_address)
        cached = self._cache.get(key)
        if cached:
            return cached

        try:
            if protocol in ("uniswap_v2", "aerodrome_v1"):
                abi = UNISWAP_V2_PAIR_ABI
                reserves = adapter.call_contract(pool_address, abi, "getReserves")
                token0 = adapter.call_contract(pool_address, abi, "token0")
                token1 = adapter.call_contract(pool_address, abi, "token1")
                block = adapter.get_block_number()
                snapshot = PoolSnapshot(
                    chain=chain,
                    pool_address=pool_address.lower(),
                    protocol=protocol,
                    token0=token0.lower(),
                    token1=token1.lower(),
                    reserve0=reserves[0],
                    reserve1=reserves[1],
                    sqrt_price_x96=None,
                    tick=None,
                    liquidity=None,
                    fee_bps=30,
                    block_number=block,
                )
            elif protocol in ("uniswap_v3",):
                abi = UNISWAP_V3_POOL_ABI
                slot0 = adapter.call_contract(pool_address, abi, "slot0")
                liq = adapter.call_contract(pool_address, abi, "liquidity")
                fee = adapter.call_contract(pool_address, abi, "fee")
                token0 = adapter.call_contract(pool_address, abi, "token0")
                token1 = adapter.call_contract(pool_address, abi, "token1")
                block = adapter.get_block_number()
                snapshot = PoolSnapshot(
                    chain=chain,
                    pool_address=pool_address.lower(),
                    protocol=protocol,
                    token0=token0.lower(),
                    token1=token1.lower(),
                    reserve0=0,
                    reserve1=0,
                    sqrt_price_x96=slot0[0],
                    tick=slot0[1],
                    liquidity=liq,
                    fee_bps=fee // 100,
                    block_number=block,
                )
            else:
                return None

            self._cache[key] = snapshot
            return snapshot
        except Exception:
            log.exception("failed to fetch pool snapshot %s/%s", chain, pool_address)
            return None

    def invalidate(self, chain: str, address: str) -> None:
        self._cache.pop(self._cache_key(chain, address), None)
