from __future__ import annotations

from onchain.chains.evm_generic.base import EVMChainAdapter


class BaseAdapter(EVMChainAdapter):
    BASE_CHAIN_ID = 8453

    def __init__(self, rpc_url: str) -> None:
        super().__init__(rpc_url=rpc_url, chain_id=self.BASE_CHAIN_ID)
