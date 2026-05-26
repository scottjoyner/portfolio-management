from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class VerificationResult:
    address: str
    chain: str
    verified: bool = False
    source: str = ""
    compiler_version: str = ""
    matches_runtime: bool = False


@dataclass
class VerificationService:
    verified: dict[str, VerificationResult] = field(default_factory=dict)

    def mark_verified(self, result: VerificationResult) -> None:
        self.verified[f"{result.chain}:{result.address.lower()}"] = result

    def is_verified(self, chain: str, address: str) -> bool:
        entry = self.verified.get(f"{chain}:{address.lower()}")
        return entry is not None and entry.verified
