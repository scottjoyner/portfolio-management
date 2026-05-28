from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any


class SignerType(Enum):
    LOCAL_KEY = "local_key"
    KMS = "kms"
    HSM = "hsm"
    HARDWARE_WALLET = "hardware_wallet"
    MANUAL_APPROVAL = "manual_approval"


@dataclass
class SignerCapability:
    signer_type: SignerType
    address: str
    supports_typed_data: bool = True
    supports_personal_sign: bool = True
    requires_manual_approval: bool = False
    max_gas_per_tx: int = 0
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class SignedTransaction:
    raw_tx: bytes
    tx_hash: str
    signer_address: str
    signer_type: SignerType


class Signer(ABC):
    @abstractmethod
    def signer_type(self) -> SignerType: ...

    @abstractmethod
    def address(self) -> str: ...

    @abstractmethod
    def capability(self) -> SignerCapability: ...

    @abstractmethod
    def sign_transaction(self, tx: dict[str, Any]) -> SignedTransaction: ...

    @abstractmethod
    def sign_message(self, message: bytes | str) -> str: ...

    def sign_typed_data(self, domain: dict, message_types: dict, message: dict) -> str:
        raise NotImplementedError("typed data signing not supported")

    def recover_address(self, message: bytes | str, signature: str) -> str:
        from eth_account import Account
        from eth_account.messages import encode_defunct

        if isinstance(message, str):
            message = message.encode()
        encoded = encode_defunct(message)
        return Account.recover_message(encoded, signature=signature)
