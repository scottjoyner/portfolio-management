from __future__ import annotations

from dataclasses import dataclass, field

from onchain.models import ContractProfile, SafetyState


@dataclass
class ContractRegistry:
    contracts: dict[tuple[str, str], ContractProfile] = field(default_factory=dict)
    emergency_denylist: set[tuple[str, str]] = field(default_factory=set)

    def register(self, profile: ContractProfile) -> None:
        key = (profile.chain, profile.address.lower())
        self.contracts[key] = profile

    def lookup(self, chain: str, address: str) -> ContractProfile | None:
        return self.contracts.get((chain, address.lower()))

    def is_allowed(self, chain: str, address: str, selector: str | None = None) -> bool:
        key = (chain, address.lower())
        if key in self.emergency_denylist:
            return False
        profile = self.contracts.get(key)
        if not profile:
            return False
        if profile.safety_state in {SafetyState.QUARANTINED, SafetyState.DENIED}:
            return False
        if selector and profile.selectors_allowlist and selector not in profile.selectors_allowlist:
            return False
        return True

    def deny(self, chain: str, address: str) -> None:
        self.emergency_denylist.add((chain, address.lower()))

    def set_state(self, chain: str, address: str, state: SafetyState) -> None:
        profile = self.lookup(chain, address)
        if profile:
            profile.safety_state = state
