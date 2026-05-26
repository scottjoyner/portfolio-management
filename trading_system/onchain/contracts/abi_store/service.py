from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ABIEntry:
    address: str
    chain: str
    abi: list[dict[str, Any]]
    source: str = "etherscan"


@dataclass
class ABIStore:
    entries: dict[str, ABIEntry] = field(default_factory=dict)

    def store(self, entry: ABIEntry) -> None:
        key = f"{entry.chain}:{entry.address.lower()}"
        self.entries[key] = entry

    def get(self, chain: str, address: str) -> ABIEntry | None:
        return self.entries.get(f"{chain}:{address.lower()}")

    def has_abi(self, chain: str, address: str) -> bool:
        return f"{chain}:{address.lower()}" in self.entries
