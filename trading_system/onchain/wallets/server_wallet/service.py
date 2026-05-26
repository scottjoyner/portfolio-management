from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, cast

from web3.types import HexStr
from onchain.chains.evm_generic.base import EVMChainAdapter
from onchain.wallets.nonce_manager.service import NonceManager
from onchain.wallets.signing.service import SigningService

log = logging.getLogger(__name__)


@dataclass
class ServerWallet:
    name: str
    chain_adapters: dict[str, EVMChainAdapter] = field(default_factory=dict)
    signing_service: SigningService | None = field(default=None, repr=False)
    nonce_manager: NonceManager | None = field(default=None)

    @property
    def address(self) -> str | None:
        if self.signing_service is None:
            return None
        return self.signing_service.address

    def set_signing_service(self, signing: SigningService) -> None:
        self.signing_service = signing

    def set_nonce_manager(self, nm: NonceManager) -> None:
        self.nonce_manager = nm

    def register_adapter(self, chain: str, adapter: EVMChainAdapter) -> None:
        self.chain_adapters[chain] = adapter

    def send(self, chain: str, to: str, value: int = 0, data: str | bytes | HexStr = "", gas: int | None = None, gas_price: int | None = None) -> str | None:
        if self.signing_service is None or not self.signing_service.is_initialized:
            raise RuntimeError(f"wallet {self.name} has no signing service")

        adapter = self.chain_adapters.get(chain)
        if adapter is None:
            raise ValueError(f"no adapter for chain {chain}")

        tx = cast(dict[str, Any], adapter.build_transaction(to, value, data, gas, gas_price))

        if self.nonce_manager is not None:
            tx["nonce"] = self.nonce_manager.consume_nonce(chain, self.signing_service.address)

        signed = self.signing_service.sign_transaction(tx)
        tx_hash = adapter.send_transaction(signed)
        log.info("tx_sent wallet=%s chain=%s hash=%s", self.name, chain, tx_hash)
        return tx_hash

    def send_with_receipt(self, chain: str, to: str, value: int = 0, data: str = "", gas: int | None = None, gas_price: int | None = None, timeout: int = 120) -> dict[str, Any] | None:
        tx_hash = self.send(chain, to, value, data, gas, gas_price)
        if tx_hash is None:
            return None

        adapter = self.chain_adapters.get(chain)
        if adapter is None:
            return None

        return adapter.wait_for_receipt(cast(HexStr, tx_hash), timeout=timeout)

    def get_balance(self, chain: str) -> int | None:
        adapter = self.chain_adapters.get(chain)
        if adapter is None or self.address is None:
            return None
        return adapter.get_balance(self.address)

    def estimate_gas(self, chain: str, to: str, value: int = 0, data: str = "") -> int | None:
        adapter = self.chain_adapters.get(chain)
        if adapter is None:
            return None
        return adapter.estimate_gas(to, value, data)
