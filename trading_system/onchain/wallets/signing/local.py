from __future__ import annotations

from decimal import Decimal
from typing import Any

from eth_account import Account
from eth_account.signers.local import LocalAccount

from onchain.wallets.signing.base import (
    SignTransaction, Signer, SignerCapability, SignerType,
)


class LocalKeySigner(Signer):
    def __init__(self, private_key_hex: str) -> None:
        self._account: LocalAccount = Account.from_key(private_key_hex)

    def signer_type(self) -> SignerType:
        return SignerType.LOCAL_KEY

    def address(self) -> str:
        return self._account.address

    def capability(self) -> SignerCapability:
        return SignerCapability(
            signer_type=SignerType.LOCAL_KEY,
            address=self._account.address,
            supports_typed_data=True,
            supports_personal_sign=True,
            requires_manual_approval=False,
        )

    def sign_transaction(self, tx: dict[str, Any]) -> SignTransaction:
        signed = self._account.sign_transaction(tx)
        return SignTransaction(
            raw_tx=signed.raw_transaction,  # type: ignore[arg-type]
            tx_hash=signed.hash.hex(),  # type: ignore[arg-type]
            signer_address=self._account.address,
            signer_type=SignerType.LOCAL_KEY,
        )

    def sign_message(self, message: bytes | str) -> str:
        from eth_account.messages import encode_defunct

        if isinstance(message, str):
            message = message.encode()
        signable = encode_defunct(message)
        signed = self._account.sign_message(signable)
        return signed.signature.hex()

    def sign_typed_data(self, domain: dict, message_types: dict, message: dict) -> str:
        from eth_account.messages import encode_typed_data

        encoded = encode_typed_data(domain, message_types, message)
        signed = self._account.sign_message(encoded)
        return signed.signature.hex()
