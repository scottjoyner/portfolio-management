from __future__ import annotations

import hashlib
from dataclasses import dataclass, field


@dataclass
class CodeHashEntry:
    address: str
    chain: str
    codehash: str
    verified: bool = False


@dataclass
class CodeHashService:
    known_hashes: dict[str, CodeHashEntry] = field(default_factory=dict)

    def register(self, entry: CodeHashEntry) -> None:
        key = f"{entry.chain}:{entry.address.lower()}"
        self.known_hashes[key] = entry

    def verify(self, chain: str, address: str, runtime_code: bytes | None = None) -> bool:
        key = f"{chain}:{address.lower()}"
        entry = self.known_hashes.get(key)
        if not entry or not runtime_code:
            return False
        actual_hash = "0x" + hashlib.sha256(runtime_code).hexdigest()
        return actual_hash.lower() == entry.codehash.lower()

    def get(self, chain: str, address: str) -> CodeHashEntry | None:
        return self.known_hashes.get(f"{chain}:{address.lower()}")
