from onchain.chains.evm_generic.base import EVMChainAdapter
from onchain.chains.ethereum.adapter import EthereumAdapter
from onchain.chains.base.adapter import BaseAdapter


def test_ethereum_adapter_defaults():
    adapter = EthereumAdapter(rpc_url="http://localhost:8545")
    assert adapter.chain_id == 1
    assert adapter.rpc_url == "http://localhost:8545"


def test_base_adapter_defaults():
    adapter = BaseAdapter(rpc_url="http://localhost:8545")
    assert adapter.chain_id == 8453


def test_evm_adapter_not_connected():
    adapter = EVMChainAdapter(rpc_url="http://localhost:9999", chain_id=1)
    assert not adapter.is_connected()


def test_evm_adapter_close():
    adapter = EVMChainAdapter(rpc_url="http://localhost:9999", chain_id=1)
    adapter.close()
    assert not adapter.is_connected()
