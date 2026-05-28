from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any


class OrderStatus(Enum):
    PENDING = "pending"
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class TimeInForce(Enum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    GTD = "GTD"


@dataclass
class BrokerOrder:
    broker_order_id: str
    client_order_id: str
    account_id: str
    product_id: str
    side: str
    order_type: str
    size: Decimal
    price: Decimal | None = None
    stop_price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.GTC
    status: OrderStatus = OrderStatus.PENDING
    filled_size: Decimal = Decimal("0")
    filled_value: Decimal = Decimal("0")
    remaining_size: Decimal | None = None
    avg_fill_price: Decimal | None = None
    fee: Decimal = Decimal("0")
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class BrokerFill:
    fill_id: str
    broker_order_id: str
    product_id: str
    side: str
    size: Decimal
    price: Decimal
    notional: Decimal
    fee: Decimal = Decimal("0")
    liquidity: str = "TAKER"
    filled_at: datetime = field(default_factory=datetime.utcnow)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class BrokerPosition:
    product_id: str
    side: str
    size: Decimal
    entry_price: Decimal
    current_price: Decimal | None = None
    unrealized_pnl: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class BrokerAccount:
    account_id: str
    name: str
    currency: str
    available_balance: Decimal
    hold_balance: Decimal = Decimal("0")
    total_balance: Decimal | None = None
    buying_power: Decimal | None = None
    extra: dict[str, Any] = field(default_factory=dict)


class BrokerAdapter(ABC):
    @abstractmethod
    def broker_name(self) -> str: ...

    # Account
    @abstractmethod
    async def get_accounts(self) -> list[BrokerAccount]: ...

    @abstractmethod
    async def get_account(self, account_id: str) -> BrokerAccount: ...

    # Orders
    @abstractmethod
    async def preview_order(self, order: BrokerOrder) -> tuple[bool, str]: ...

    @abstractmethod
    async def submit_order(self, order: BrokerOrder) -> BrokerOrder: ...

    @abstractmethod
    async def cancel_order(self, broker_order_id: str) -> bool: ...

    @abstractmethod
    async def get_order(self, broker_order_id: str) -> BrokerOrder | None: ...

    @abstractmethod
    async def list_orders(
        self, product_id: str | None = None, status: OrderStatus | None = None,
    ) -> list[BrokerOrder]: ...

    # Fills
    @abstractmethod
    async def get_fills(self, broker_order_id: str) -> list[BrokerFill]: ...

    # Positions
    @abstractmethod
    async def get_positions(self, product_id: str | None = None) -> list[BrokerPosition]: ...

    # Products / Assets
    @abstractmethod
    async def list_products(self) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def get_product(self, product_id: str) -> dict[str, Any] | None: ...

    # Market data
    async def get_market_price(self, product_id: str) -> Decimal | None:
        return None

    # Health
    async def health_check(self) -> dict[str, Any]:
        return {"status": "unknown"}
