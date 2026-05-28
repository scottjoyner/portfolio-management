from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from onchain.wallets.signing.base import Signer, SignerType
from onchain.wallets.signing.local import LocalKeySigner

AllowlistEntry = dict[str, Any]
SpendPolicy = dict[str, Any]

DEFAULT_SPEND_POLICY: SpendPolicy = {
    "max_per_action_usd": Decimal("10000"),
    "max_daily_usd": Decimal("50000"),
    "max_gas_gwei": Decimal("200"),
    "allowed_chains": ["eth_mainnet", "polygon_mainnet", "arbitrum_mainnet"],
}


@dataclass
class OnchainApprovalPacket:
    route_id: str
    chain_id: str
    contract_address: str
    function_signature: str
    params_json: str
    gas_estimate: int
    max_gas_price_gwei: Decimal
    estimated_slippage_bps: float
    capital_at_risk_usd: Decimal
    token_allowlist_check: str
    wallet_spend_policy_check: str
    signer_type_required: SignerType
    approved: bool = False
    rejection_reason: str = ""
    reviewed_at: datetime | None = None
    signed_tx_hash: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


class OnchainApprovalRouter:
    def __init__(
        self,
        signer: Signer,
        allowlisted_contracts: list[AllowlistEntry] | None = None,
        spend_policy: SpendPolicy | None = None,
    ) -> None:
        self._signer = signer
        self._allowlisted_contracts = allowlisted_contracts or []
        self._spend_policy = spend_policy or DEFAULT_SPEND_POLICY

    @property
    def signer(self) -> Signer:
        return self._signer

    def register_allowlisted_contract(self, entry: AllowlistEntry) -> None:
        self._allowlisted_contracts.append(entry)

    def check_contract_allowlisted(self, chain_id: str, contract_address: str) -> tuple[bool, str]:
        for entry in self._allowlisted_contracts:
            if entry.get("chain_id") == chain_id and entry.get("address", "").lower() == contract_address.lower():
                return True, entry.get("name", "allowlisted")
        return False, f"contract {contract_address} not allowlisted on {chain_id}"

    def check_spend_policy(self, capital_at_risk_usd: Decimal, daily_used_usd: Decimal = Decimal("0")) -> tuple[bool, str]:
        max_per = self._spend_policy.get("max_per_action_usd", Decimal("10000"))
        max_daily = self._spend_policy.get("max_daily_usd", Decimal("50000"))
        if capital_at_risk_usd > max_per:
            return False, f"capital at risk ${capital_at_risk_usd} exceeds max per action ${max_per}"
        if daily_used_usd + capital_at_risk_usd > max_daily:
            return False, f"daily total ${daily_used_usd + capital_at_risk_usd} exceeds max daily ${max_daily}"
        return True, "spend policy passed"

    def build_packet(
        self,
        route_id: str,
        chain_id: str,
        contract_address: str,
        function_signature: str,
        params: dict[str, Any],
        gas_estimate: int,
        max_gas_price_gwei: Decimal,
        capital_at_risk_usd: Decimal,
        estimated_slippage_bps: float = 0.0,
        daily_used_usd: Decimal = Decimal("0"),
    ) -> OnchainApprovalPacket:
        contract_ok, contract_msg = self.check_contract_allowlisted(chain_id, contract_address)
        spend_ok, spend_msg = self.check_spend_policy(capital_at_risk_usd, daily_used_usd)

        import json
        packet = OnchainApprovalPacket(
            route_id=route_id,
            chain_id=chain_id,
            contract_address=contract_address,
            function_signature=function_signature,
            params_json=json.dumps(params),
            gas_estimate=gas_estimate,
            max_gas_price_gwei=max_gas_price_gwei,
            estimated_slippage_bps=estimated_slippage_bps,
            capital_at_risk_usd=capital_at_risk_usd,
            token_allowlist_check=contract_msg,
            wallet_spend_policy_check=spend_msg,
            signer_type_required=self._signer.signer_type(),
        )

        if not contract_ok:
            packet.rejection_reason = contract_msg
            return packet
        if not spend_ok:
            packet.rejection_reason = spend_msg
            return packet

        packet.approved = True
        return packet

    def sign_packet(self, packet: OnchainApprovalPacket, tx: dict[str, Any]) -> OnchainApprovalPacket:
        if not packet.approved:
            packet.rejection_reason = "cannot sign unapproved packet"
            return packet

        signed = self._signer.sign_transaction(tx)
        packet.signed_tx_hash = signed.tx_hash
        packet.reviewed_at = datetime.utcnow()
        return packet
