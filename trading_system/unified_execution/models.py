from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Any, Dict, List, Set
from abc import ABC, abstractmethod

class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    TWAP = "TWAP"
    ICEBERG = "ICEBERG"

class OrderStatus(Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"

class HealthStatus(Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNHEALTHY = "UNHEALTHY"

@dataclass(frozen=True)
class UniversalAsset:
    asset_id: str
    symbol: str
    base_currency: str
    quote_currency: str
    chain_id: Optional[str] = None
    contract_address: Optional[str] = None
    decimals: int = 18

@dataclass(frozen=True)
class UniversalOrder:
    order_id: str
    asset: UniversalAsset
    side: OrderSide
    order_type: OrderType
    size: Decimal
    price: Optional[Decimal] = None
    slippage_pct: Decimal = Decimal("0.0")
    gas_limit: Optional[int] = None
    status: OrderStatus = OrderStatus.PENDING
    venue_order_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

@dataclass(frozen=True)
class UniversalFill:
    fill_id: str
    order_id: str
    venue_fill_id: Optional[str]
    asset: UniversalAsset
    size: Decimal
    price: Decimal
    fee: Decimal
    timestamp: datetime

@dataclass(frozen=True)
class UniversalPosition:
    asset: UniversalAsset
    size: Decimal
    avg_entry_price: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal

@dataclass(frozen=True)
class UniversalBalance:
    asset: UniversalAsset
    amount: Decimal
    available: Decimal
    locked: Decimal

@dataclass(frozen=True)
class UniversalAccount:
    account_id: str
    venue_name: str
    name: str
    currency: str
    balances: List[UniversalBalance]

@dataclass(frozen=True)
class TickerInfo:
    asset: UniversalAsset
    bid_price: Decimal
    ask_price: Decimal
    last_price: Decimal
    volume_24h: Decimal
    timestamp: datetime

@dataclass(frozen=True)
class OrderbookLevel:
    price: Decimal
    amount: Decimal

@dataclass(frozen=True)
class Orderbook:
    asset: UniversalAsset
    bids: List[OrderbookLevel]
    asks: List[OrderbookLevel]
    timestamp: datetime
