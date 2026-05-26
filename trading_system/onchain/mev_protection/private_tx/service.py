from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PrivateTxService:
    relay_endpoints: list[str] = field(default_factory=list)

    def submit(self, signed_tx: bytes, relay_url: str | None = None) -> str | None:
        return None
