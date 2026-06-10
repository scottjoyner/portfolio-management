from typing import List, Set
from datetime import datetime
from decimal import Decimal
import asyncio

from trading_system.unified_execution.interfaces import (
    ExchangeAdapter,
    OrderSide,
    OrderType,
    OrderStatus,
    HealthStatus,
    UniversalAsset,
    UniversalAccount,
    UniversalBalance,
    UniversalOrder,
    UniversalFill,
    UniversalPosition,
    TickerInfo,
    Orderbook,
    OrderbookLevel
)

class MockCoinbaseAdapter(ExchangeAdapter):
    def __init__(self):
        self._venue_name = "Mock Coinbase"
        self._supported_chains = {"base", "ethereum"}
        self._asset = UniversalAsset(
            asset_id="BTC-USD",
            symbol="BTC",
            base_currency="BTC",
            quote_currency="USD",
            decimals=8
        )

    @property
    def venue_name(self) -> str:
        return self._venue_name

    @property
    def supported_chains(self) -> Set[str]:
        return self._supported_chains

    async def get_accounts(self) -> List[UniversalAccount]:
        return [
            UniversalAccount(
                account_id="mock_acc_1",
                venue_name=self._venue_name,
                name="Mock Savings",
                currency="USD",
                balances=[
                    UniversalBalance(asset=self._asset, amount=Decimal("1.0"), available=Decimal("1.0"), locked=Decimal("0")),
                    UniversalBalance(asset=UniversalAsset(asset_id="USD-USD", symbol="USD", base_currency="USD", quote_currency="USD"), amount=Decimal("10000"), available=Decimal("10000"), locked=Decimal("0"))
                ]
            )
        ]

    async def get_balances(self, account_id: str) -> List[UniversalBalance]:
        return []

    async def get_ticker(self, asset: UniversalAsset) -> TickerInfo:
        return TickerInfo(
            asset=asset,
            bid_price=Decimal("60000.0"),
            ask_price=Decimal("60005.0"),
            last_price=Decimal("60002.5"),
            volume_24h=Decimal("1000000.0"),
            timestamp=datetime.now()
        )

    async def get_orderbook(self, asset: UniversalAsset, depth: int = 10) -> Orderbook:
        return Orderbook(
            asset=asset,
            bids=[OrderbookLevel(price=Decimal("60000"), amount=Decimal("0.5"))],
            asks=[OrderbookLevel(price=Decimal("60001"), amount=Decimal("0.5"))],
            timestamp=datetime.now()
        )

    async def execute_order(self, order: UniversalOrder) -> UniversalOrder:
        return UniversalOrder(
            order_id=order.order_id,
            asset=order.asset,
            side=order.side,
            order_type=order.order_type,
            size=order.size,
            status=OrderStatus.FILLED,
            venue_order_id="mock_order_123"
        )

    async def cancel_order(self, venue_order_id: str) -> bool:
        return True

    async def get_order_status(self, venue_order_id: str) -> OrderStatus:
        return OrderStatus.FILLED

    async def health_check(self) -> HealthStatus:
        return HealthStatus.HEALTHY
