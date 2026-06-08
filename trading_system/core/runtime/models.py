from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class SerializableModel:
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RuntimeStatus(SerializableModel):
    mode: str = "paper"
    live_trading_enabled: bool = False
    coinbase_connected: bool = False
    worker_status: str = "unknown"
    event_log_status: str = "unknown"
    timestamp: str = field(default_factory=utc_now_iso)


@dataclass
class StrategyStatus(SerializableModel):
    strategy_id: str
    name: str
    category: str
    enabled: bool = False
    mode: str = "paper"
    product_ids: List[str] = field(default_factory=list)
    last_tick_at: Optional[str] = None
    last_signal: Optional[Dict[str, Any]] = None
    last_error: Optional[str] = None
    pnl: Optional[float] = None
    open_orders: int = 0
    position_summary: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderIntent(SerializableModel):
    strategy_id: str
    product_id: str
    side: str
    order_type: str = "market"
    quote_size: Optional[float] = None
    base_size: Optional[float] = None
    limit_price: Optional[float] = None
    client_order_id: str = field(default_factory=lambda: str(uuid4()))
    reason: Optional[str] = None
    confidence: Optional[float] = None
    timestamp: str = field(default_factory=utc_now_iso)

    def __post_init__(self) -> None:
        self.side = self.side.upper()
        self.order_type = self.order_type.lower()
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        if self.quote_size is None and self.base_size is None:
            raise ValueError("OrderIntent requires quote_size or base_size")


@dataclass
class OrderPreviewRecord(SerializableModel):
    client_order_id: str
    strategy_id: str
    product_id: str
    side: str
    preview: Dict[str, Any]
    accepted: bool = False
    reason: Optional[str] = None
    timestamp: str = field(default_factory=utc_now_iso)


@dataclass
class ExecutionStatus(SerializableModel):
    order_id: str
    client_order_id: str
    strategy_id: str
    product_id: str
    side: str
    order_type: str
    status: str
    requested_size: Optional[float] = None
    preview_result: Optional[Dict[str, Any]] = None
    risk_decision: Optional[Dict[str, Any]] = None
    coinbase_status: Optional[str] = None
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass
class TradingEvent(SerializableModel):
    source: str
    event_type: str
    payload: Dict[str, Any] = field(default_factory=dict)
    level: str = "INFO"
    strategy_id: Optional[str] = None
    event_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: str = field(default_factory=utc_now_iso)


@dataclass
class AccountSnapshot(SerializableModel):
    accounts: List[Dict[str, Any]] = field(default_factory=list)
    prices: Dict[str, Any] = field(default_factory=dict)
    total_estimated_value: Optional[float] = None
    open_orders: List[Dict[str, Any]] = field(default_factory=list)
    fills: List[Dict[str, Any]] = field(default_factory=list)
    timestamp: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["account_count"] = len(self.accounts)
        return data


@dataclass
class RiskDecision(SerializableModel):
    allowed: bool
    reason: str
    strategy_id: Optional[str] = None
    product_id: Optional[str] = None
    timestamp: str = field(default_factory=utc_now_iso)
