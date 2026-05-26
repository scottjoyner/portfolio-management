from __future__ import annotations

from dataclasses import dataclass, field



@dataclass
class UserOperation:
    sender: str
    nonce: int
    init_code: bytes = b""
    call_data: bytes = b""
    call_gas_limit: int = 0
    verification_gas_limit: int = 0
    pre_verification_gas: int = 0
    max_fee_per_gas: int = 0
    max_priority_fee_per_gas: int = 0
    paymaster_and_data: bytes = b""
    signature: bytes = b""


@dataclass
class SmartWalletAdapter:
    entry_point: str = "0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789"
    supported_entry_points: set[str] = field(default_factory=lambda: {"0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789"})
    _operations: list[UserOperation] = field(default_factory=list)

    def build_user_operation(self, sender: str, call_data: bytes, nonce: int, max_fee_per_gas: int = 0, max_priority_fee_per_gas: int = 0) -> UserOperation:
        op = UserOperation(
            sender=sender,
            nonce=nonce,
            call_data=call_data,
            max_fee_per_gas=max_fee_per_gas,
            max_priority_fee_per_gas=max_priority_fee_per_gas,
        )
        self._operations.append(op)
        return op

    def supports_entry_point(self, ep: str) -> bool:
        return ep.lower() in {a.lower() for a in self.supported_entry_points}
