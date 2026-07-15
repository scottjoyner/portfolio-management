from abc import ABC, abstractmethod
from typing import List, Set
from .models import (
    UniversalAsset,
    UniversalAccount,
    UniversalBalance,
    TickerInfo,
    Orderbook,
    OrderbookLevel,
    UniversalOrder,
    UniversalFill,
    UniversalPosition,
    OrderSide,
    OrderType,
    OrderStatus,
    HealthStatus
)

class ExchangeAdapter(ABC):
    """
    The abstract interface for all exchange adapters (CEX/DEX).
    All adapters must implement these methods to ensure a unified execution layer.
    """

    @property
    @abstractmethod
    def venue_name(self) -> str:
        """The human-readable name of the exchange (e.g., 'Coinbase', 'Uniswap')."""
        ...

    @property
    @abstractmethod
    def supported_chains(self) -> Set[str]:
        """A set of chain IDs supported by this venue (e.g., {'1', '137'}). 
        Return an empty set if it's a CEX.
        """
        ...

    # --- Account Management ---

    @abstractmethod
    async def get_accounts(self) -> List[UniversalAccount]:
        """Retrieve all accounts associated with this venue."""
        ...

    @abstractmethod
    async def get_balances(self, account_id: str) -> List[UniversalBalance]:
        """Retrieve balances for a specific account."""
        ...

    # --- Market Data ---

    @abstractmethod
    async def get_ticker(self, asset: UniversalAsset) -> TickerInfo:
        """Retrieve the latest ticker information for the given asset pair."""
        ...

    @abstractmethod
    async def get_orderbook(self, asset: UniversalAsset, depth: int = 10) -> Orderbook:
        """Retrieve the L2 orderbook for the given asset pair."""
        ...

    # --- Execution ---

    @abstractmethod
    async def execute_order(self, order: UniversalOrder) -> UniversalOrder:
        """Send an order to the venue and return the resulting order state."""
        ...

    @abstractmethod
    async def cancel_order(self, venue_order_id: str) -> bool:
        """Attempt to cancel an active order. Returns True if successful."""
        ...

    @abstractmethod
    async def get_order_status(self, venue_order_id: str) -> OrderStatus:
        """Retrieve the current status of an order on the venue."""
        ...

    # --- Lifecycle ---

    @abstractmethod
    async def health_check(self) -> HealthStatus:
        """Perform a connectivity and latency check for the venue."""
        ...
