from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum


class BridgeProtocol(Enum):
    ACROSS = "across"
    STARGATE = "stargate"
    WORMHOLE = "wormhole"
    SYNAPSE = "synapse"
    CCTP = "cctp"


@dataclass
class BridgeQuote:
    source_chain: str
    destination_chain: str
    token: str
    amount: Decimal
    estimated_gas: Decimal
    bridge_fee: Decimal
    estimated_time_minutes: int
    protocol: BridgeProtocol
    max_slippage_bps: int = 50


@dataclass
class BridgeRisk:
    total_risk_score: float = 0.0
    bridge_downtime_risk: float = 0.0
    liquidity_risk: float = 0.0
    counterparty_risk: float = 0.0
    finality_risk: float = 0.0
    approved: bool = True
    reason: str = ""


@dataclass
class BridgeSettlement:
    source_tx_hash: str = ""
    destination_tx_hash: str = ""
    status: str = "pending"
    confirmations: int = 0


@dataclass
class BridgeService:
    adapters: dict[str, object] = field(default_factory=dict)

    def quote(self, source_chain: str, destination_chain: str, token: str, amount: Decimal) -> BridgeQuote | None:
        return BridgeQuote(
            source_chain=source_chain,
            destination_chain=destination_chain,
            token=token,
            amount=amount,
            estimated_gas=Decimal("0.005"),
            bridge_fee=amount * Decimal("0.001"),
            estimated_time_minutes=5,
            protocol=BridgeProtocol.CCTP,
        )

    def assess_risk(self, quote: BridgeQuote) -> BridgeRisk:
        return BridgeRisk()

    def execute(self, quote: BridgeQuote) -> BridgeSettlement:
        return BridgeSettlement()
