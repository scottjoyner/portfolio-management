from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, cast

from web3 import Web3
from web3.types import BlockIdentifier, FilterParams, HexStr, TxParams, Wei

log = logging.getLogger(__name__)


@dataclass
class EVMChainAdapter:
    rpc_url: str
    chain_id: int
    _w3: Web3 | None = field(default=None, repr=False)

    @property
    def w3(self) -> Web3:
        if self._w3 is None:
            self._w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        return self._w3

    def is_connected(self) -> bool:
        return self.w3.is_connected()

    def get_block(self, block_identifier: BlockIdentifier = "latest", full_transactions: bool = False) -> dict[str, Any]:
        return dict(self.w3.eth.get_block(block_identifier, full_transactions))

    def get_block_number(self) -> int:
        return self.w3.eth.block_number

    def get_balance(self, address: str, block: BlockIdentifier = "latest") -> int:
        return self.w3.eth.get_balance(Web3.to_checksum_address(address), block)

    def call_contract(self, contract_address: str, abi: list[dict[str, Any]], fn_name: str, *args: Any, block: BlockIdentifier = "latest") -> Any:
        checksum = Web3.to_checksum_address(contract_address)
        contract = self.w3.eth.contract(address=checksum, abi=abi)
        fn = getattr(contract.functions, fn_name)
        return fn(*args).call(block_identifier=block)

    def get_logs(self, from_block: int, to_block: int, address: str | None = None, topics: list[str] | None = None) -> list[dict[str, Any]]:
        filter_params: FilterParams = {"fromBlock": from_block, "toBlock": to_block}
        if address:
            filter_params["address"] = Web3.to_checksum_address(address)
        if topics:
            filter_params["topics"] = topics
        return [dict(log) for log in self.w3.eth.get_logs(filter_params)]

    def build_transaction(self, to: str, value: int = 0, data: str | bytes | HexStr = "", gas: int | None = None, gas_price: int | None = None) -> TxParams:
        tx_data = cast(bytes | HexStr, data)
        tx: TxParams = {
            "to": Web3.to_checksum_address(to),
            "value": Wei(value),
            "data": tx_data,
            "chainId": self.chain_id,
        }
        addr = Web3.to_checksum_address(to)
        if gas is None:
            gas = self.w3.eth.estimate_gas({"to": addr, "value": Wei(value), "data": cast(bytes | HexStr, data)})
        tx["gas"] = Wei(gas)
        if gas_price is None:
            gas_price = self.w3.eth.gas_price
        tx["gasPrice"] = Wei(gas_price)
        return tx

    def send_transaction(self, signed_tx: bytes) -> str:
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx)
        return tx_hash.hex()

    def wait_for_receipt(self, tx_hash: str | HexStr, timeout: int = 120, poll_latency: float = 0.5) -> dict[str, Any]:
        receipt = self.w3.eth.wait_for_transaction_receipt(cast(HexStr, tx_hash), timeout=timeout, poll_latency=poll_latency)
        return dict(receipt)

    def estimate_gas(self, to: str, value: int = 0, data: str | bytes | HexStr = "") -> int:
        return self.w3.eth.estimate_gas({"to": Web3.to_checksum_address(to), "value": Wei(value), "data": cast(bytes | HexStr, data)})

    def gas_price(self) -> int:
        return self.w3.eth.gas_price

    def close(self) -> None:
        if self._w3 is not None:
            self._w3.provider = None  # type: ignore[assignment]
            self._w3 = None
