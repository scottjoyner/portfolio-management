from __future__ import annotations

import logging
from dataclasses import dataclass, field

from eth_account import Account
from eth_account.signers.local import LocalAccount

log = logging.getLogger(__name__)


@dataclass
class SigningService:
    _account: LocalAccount | None = field(default=None, repr=False)
    _private_key: str = field(default="", repr=False)

    @classmethod
    def from_key(cls, private_key_hex: str) -> SigningService:
        account = Account.from_key(private_key_hex)
        return cls(_account=account, _private_key=private_key_hex)

    @property
    def address(self) -> str:
        if self._account is None:
            raise RuntimeError("SigningService not initialized")
        return self._account.address

    @property
    def is_initialized(self) -> bool:
        return self._account is not None

    def sign_transaction(self, tx: dict) -> bytes:
        if self._account is None:
            raise RuntimeError("SigningService not initialized")
        signed = self._account.sign_transaction(tx)
        return signed.raw_transaction  # type: ignore[return-value]

    def sign_message(self, message: bytes | str) -> str:
        if self._account is None:
            raise RuntimeError("SigningService not initialized")
        from eth_account.messages import encode_defunct

        if isinstance(message, str):
            message = message.encode()
        signable = encode_defunct(message)
        signed = self._account.sign_message(signable)
        return signed.signature.hex()

    def sign_typed_data(self, domain: dict, message_types: dict, message: dict) -> str:
        from eth_account.messages import encode_typed_data

        if self._account is None:
            raise RuntimeError("SigningService not initialized")
        encoded = encode_typed_data(domain, message_types, message)
        signed = self._account.sign_message(encoded)
        return signed.signature.hex()

    def recover_address(self, message: bytes | str, signature: str) -> str:
        if isinstance(message, str):
            message = message.encode()
        from eth_account.messages import encode_defunct

        encoded = encode_defunct(message)
        return Account.recover_message(encoded, signature=signature)
