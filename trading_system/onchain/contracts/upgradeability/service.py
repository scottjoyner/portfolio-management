from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class UpgradeInfo:
    is_upgradeable: bool = False
    implementation: str = ""
    admin_address: str = ""
    last_upgrade: datetime | None = None


@dataclass
class UpgradeabilityService:
    known: dict[str, UpgradeInfo] = field(default_factory=dict)

    def register(self, address: str, chain: str, info: UpgradeInfo) -> None:
        self.known[f"{chain}:{address.lower()}"] = info

    def check(self, address: str, chain: str) -> UpgradeInfo:
        return self.known.get(f"{chain}:{address.lower()}", UpgradeInfo())
