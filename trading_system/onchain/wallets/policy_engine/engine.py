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

        # Every cap below is an upper bound, so a negative amount passes all of
        # them. Notional was then added to the running total, so repeated
        # negatives walked daily_spent negative and the daily limit stopped
        # binding. Refuse non-positive spend before doing any arithmetic.
        if not isinstance(notional, (int, float)) or isinstance(notional, bool):
            return False, "notional must be numeric"
        if notional <= 0:
            return False, f"notional must be positive, got {notional}"
        for name, value in (
            ("contract_spend", contract_spend),
            ("token_spend", token_spend),
            ("allowance_requested", allowance_requested),
            ("bridge_spend", bridge_spend),
        ):
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return False, f"{name} must be numeric"
            if value < 0:
                return False, f"{name} must not be negative, got {value}"

        if self.daily_spent.get(wallet, 0.0) + notional > policy.daily_spend_limit:
            return False, "daily spend limit exceeded"
        if contract_spend > policy.per_contract_cap:
            return False, "per-contract cap exceeded"
        if token_spend > policy.per_token_cap:
            return False, "per-token cap exceeded"
        if allowance_requested > policy.allowance_cap:
            return False, "allowance cap exceeded"
        if bridge_spend > policy.bridge_limit:
            return False, "bridge cap exceeded"
        self.daily_spent[wallet] = self.daily_spent.get(wallet, 0.0) + notional
        return True, "approved"
