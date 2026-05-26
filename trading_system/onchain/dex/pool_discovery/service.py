from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class PoolInfo:
    address: str
    protocol: str
    token0: str
    token1: str
    fee_bps: int
    liquidity_usd: Decimal = Decimal("0")


@dataclass
class PoolDiscovery:
    pools: dict[str, PoolInfo] = field(default_factory=dict)

    def register_pool(self, pool: PoolInfo) -> None:
        self.pools[pool.address.lower()] = pool

    def find_pools(self, token0: str, token1: str, protocol: str | None = None) -> list[PoolInfo]:
        results = []
        for pool in self.pools.values():
            tokens_match = (pool.token0.lower() == token0.lower() and pool.token1.lower() == token1.lower()) or \
                           (pool.token0.lower() == token1.lower() and pool.token1.lower() == token0.lower())
            if tokens_match and (protocol is None or pool.protocol == protocol):
                results.append(pool)
        return results
