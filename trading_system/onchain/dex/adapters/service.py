from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DEXAdapter:
    name: str
    protocol: str
    supports_v2: bool = False
    supports_v3: bool = False
    supported_chains: set[str] = field(default_factory=set)


@dataclass
class DEXRegistry:
    adapters: dict[str, DEXAdapter] = field(default_factory=dict)

    def register(self, adapter: DEXAdapter) -> None:
        self.adapters[adapter.name] = adapter

    def get(self, name: str) -> DEXAdapter | None:
        return self.adapters.get(name)

    def for_chain(self, chain: str) -> list[DEXAdapter]:
        return [a for a in self.adapters.values() if chain in a.supported_chains]
