from __future__ import annotations

from dataclasses import dataclass, field

from onchain.models import SafetyState, TokenProfile


@dataclass
class TokenSafetyEngine:
    tokens: dict[tuple[str, str], TokenProfile] = field(default_factory=dict)
    denylist: set[tuple[str, str]] = field(default_factory=set)

    def register_token(self, token: TokenProfile) -> None:
        self.tokens[(token.chain, token.address.lower())] = token

    def classify(self, chain: str, token: str) -> tuple[SafetyState, float]:
        key = (chain, token.lower())
        if key in self.denylist:
            return SafetyState.DENIED, 1.0
        profile = self.tokens.get(key)
        if not profile:
            return SafetyState.QUARANTINED, 0.95
        return profile.safety_state, profile.risk_score

    def validate_decimals(self, chain: str, token: str, expected: int) -> bool:
        profile = self.tokens.get((chain, token.lower()))
        return bool(profile and profile.decimals == expected)
