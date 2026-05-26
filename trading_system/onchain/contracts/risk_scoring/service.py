from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ContractRiskScore:
    address: str
    chain: str
    risk_score: float = 0.0
    factors: dict[str, float] = field(default_factory=dict)
    is_upgradeable: bool = False
    has_admin_keys: bool = False
    has_pause: bool = False


@dataclass
class RiskScoringService:
    def score(self, address: str, chain: str, is_proxy: bool = False, has_admin_keys: bool = False, has_pause: bool = False, age_days: int = 0, tx_count: int = 0) -> ContractRiskScore:
        score = 0.0
        factors: dict[str, float] = {}

        if is_proxy:
            score += 0.2
            factors["is_proxy"] = 0.2
        if has_admin_keys:
            score += 0.3
            factors["has_admin_keys"] = 0.3
        if not has_pause:
            score += 0.1
            factors["no_pause"] = 0.1
        if age_days < 30:
            score += 0.15
            factors["young_contract"] = 0.15
        if tx_count < 100:
            score += 0.1
            factors["low_usage"] = 0.1

        return ContractRiskScore(
            address=address,
            chain=chain,
            risk_score=min(score, 1.0),
            factors=factors,
            is_upgradeable=is_proxy,
            has_admin_keys=has_admin_keys,
            has_pause=has_pause,
        )
