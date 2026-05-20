from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class WalletPolicy:
    wallet: str
    daily_spend_limit: float
    per_contract_cap: float
    per_token_cap: float
    allowance_cap: float
    bridge_limit: float


@dataclass
class WalletPolicyEngine:
    policies: dict[str, WalletPolicy] = field(default_factory=dict)
    daily_spent: dict[str, float] = field(default_factory=dict)

    def register_policy(self, policy: WalletPolicy) -> None:
        self.policies[policy.wallet] = policy
        self.daily_spent.setdefault(policy.wallet, 0.0)

    def approve_spend(
        self,
        wallet: str,
        notional: float,
        contract_spend: float,
        token_spend: float,
        allowance_requested: float,
        bridge_spend: float = 0.0,
    ) -> tuple[bool, str]:
        policy = self.policies.get(wallet)
        if not policy:
            return False, "wallet policy missing"
        if self.daily_spent[wallet] + notional > policy.daily_spend_limit:
            return False, "daily spend limit exceeded"
        if contract_spend > policy.per_contract_cap:
            return False, "per-contract cap exceeded"
        if token_spend > policy.per_token_cap:
            return False, "per-token cap exceeded"
        if allowance_requested > policy.allowance_cap:
            return False, "allowance cap exceeded"
        if bridge_spend > policy.bridge_limit:
            return False, "bridge cap exceeded"
        self.daily_spent[wallet] += notional
        return True, "approved"
