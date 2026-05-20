from __future__ import annotations

from dataclasses import dataclass

from onchain.contracts.registry.service import ContractRegistry
from onchain.models import ContractProfile


@dataclass
class ContractSafetyEngine:
    registry: ContractRegistry

    def risk_score(self, chain: str, address: str) -> float:
        profile = self.registry.lookup(chain, address)
        if not profile:
            return 0.95
        score = profile.risk_score
        if profile.upgradeable:
            score = min(1.0, score + 0.1)
        if profile.admin_keys_present:
            score = min(1.0, score + 0.1)
        return score

    def ensure_verified(self, profile: ContractProfile) -> bool:
        return profile.verified_abi and len(profile.codehash) > 8
