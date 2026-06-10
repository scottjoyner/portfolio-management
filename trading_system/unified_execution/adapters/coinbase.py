from typing import List, Set, Optional, Any, Dict
from decimal import Decimal
from datetime import datetime
import asyncio

from .interfaces import (
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

# Real import from the existing project
# Note: We use absolute-style import from the project root
from trading_system.unified_execution.models import (
    UniversalAsset,
    UniversalAccount,
    UniversalBalance,
    UniversalOrder,
    UniversalFill,
    UniversalPosition,
    TickerInfo,
    Orderbook,
    OrderbookLevel,
    OrderSide,
    OrderType,
    OrderStatus,
    HealthStatus
)

# Attempt to import the real client, fallback to a dummy if not found during static analysis
try:
    from coinbase.src.cb_client import CBClient
except ImportError:
    CBClient = None

class CoinbaseAdapter(ExchangeAdapter):
    def __init__(
        self, 
        api_key: Optional[str] = None, 
        api_secret: Optional[str] = None, 
        timeout: Optional[int] = None
    ):
        if CBClient:
            self._client = CBClient(
                api_key=api_key, 
                api_secret=api_secret, 
                timeout=timeout
            )
        else:
            self._client = None
        self._venue_name = "Coinbase"
        self._supported_chains = {"base", "ethereum"}

    @property
    def venue_name(self) -> str:
        return self._venue_name

    @property
    def supported_chains(self) -> Set[str]:
        return self._supported_chains

    async def get_accounts(self) -> List[UniversalAccount]:
        if not self._client:
            return []
            
        loop = asyncio.get_event_loop()
        try:
            accounts_data = await loop.run_in_executor(None, self._client.list_accounts)
            accounts = []
            for acc in accounts_data.get("accounts", []):
                accounts.append(UniversalAccount(
                    account_id=acc.get("id", ""),
                    venue_name=self._venue_name,
                    name=acc.get("name", ""),
                    currency=acc.get("currency", "USD"),
                    balances=[]
                ))
            return accounts
        except Exception:
            return []

    async def get_balances(self, account_id: str) -> List[UniversalBalance]:
        return []

    async def get_ticker(self, asset: UniversalAsset) -> TickerInfo:
        product_id = f"{asset.base_currency}-{asset.quote_currency}".upper()
        loop = asyncio.get_event_loop()
        try:
            ticker_data = await loop.run_in_executor(
                None, 
                lambda: self._client.best_bid_ask([product_id])
            )
            # In a real implementation, parse ticker_data
            return TickerInfo(
                asset=asset,
                bid_price=Decimal("0"),
                ask_price=Decimal("0"),
                last_price=Decimal("0"),
                volume_24h=Decimal("0"),
                timestamp=datetime.now()
            )
        except Exception:
            return TickerInfo(
                asset=asset,
                bid_price=Decimal("0"),
                ask_price=Decimal("0"),
                last_price=Decimal("0"),
                volume_24h=Decimal("0"),
                timestamp=datetime.now()
            )

    async def get_orderbook(self, asset: UniversalAsset, depth: int = 10) -> Orderbook:
        return Orderbook(
            asset=asset,
            bids=[],
            asks=[],
            timestamp=datetime.now()
        )

    async def execute_order(self, order: UniversalOrder) -> UniversalOrder:
        if not self._client:
             raise RuntimeError("CBClient not initialized")

        product_id = f"{order.asset.base_currency}-{order.asset.quote_currency}".upper()
        loop = asyncio.get_event_loop()
        side_str = "buy" if order.side == OrderSide.BUY else "sell"
        
        try:
            res = await loop.run_in_executor(
                None, 
                lambda: self._client.market_order(
                    side=side_str,
                    product_id=product_id,
                    base_size=str(order.size) if order.size else None
                )
            )
            return UniversalOrder(
                order_id=order.order_id,
                asset=order.asset,
                side=order.side,
                order_type=order.order_type,
                size=Decimal(str(order.size)),
                status=OrderStatus.FILLED,
                venue_order_id=res.get("id")
            )
        except Exception as e:
            print(f"Execution failed: {e}")
            return UniversalOrder(
                order_id=order.order_id,
                asset=order.asset,
                side=order.side,
                order_type=order.order_type,
                size=Decimal(str(order.size)),
                status=OrderStatus.REJECTED
            )

    async def cancel_order(self, venue_order_id: str) -> bool:
        return True

    async def get_order_status(self, venue_order_id: str) -> OrderStatus:
        return OrderStatus.FILLED

    async def health_check(self) -> HealthStatus:
        return HealthStatus.HEALTHY
