from __future__ import annotations

from onchain.chains.evm_generic.base import EVMChainAdapter


class EthereumAdapter(EVMChainAdapter):
    MAINNET_CHAIN_ID = 1

    def __init__(self, rpc_url: str) -> None:
        super().__init__(rpc_url=rpc_url, chain_id=self.MAINNET_CHAIN_ID)
