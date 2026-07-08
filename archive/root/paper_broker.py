"""Paper trading executor — executes signals against simulated market data."""

from __future__ import annotations
import time as _time
from datetime import datetime, timedelta
from dataclasses import dataclass


@dataclass(slots=True)
class Candle:
    """Single OHLCV candle with metadata."""
    timestamp: str  # ISO-8601 or epoch seconds as string
    open_p: float
    high_p: float
    low_p: float
    close_p: float
    volume: float

    @property
    def mid(self) -> float:
        return (self.high_p + self.low_p) / 2


@dataclass(slots=True)
class OrderBook:
    """Snapshot of bid/ask side."""
    timestamp: str
    bids: list[tuple[str, float]]   # (price_str, qty) sorted ascending
    asks: list[tuple[str, float]]   # (price_str, qty) sorted descending


@dataclass(slots=True)
class MarketData:
    """Container for a single time-step of market data."""
    timestamp: str
    candle: Candle | None = None
    orderbook: OrderBook | None = None

    @property
    def mid_price(self) -> float | None:
        if self.candle is not None:
            return (self.candle.high_p + self.candle.low_p) / 2
        elif self.orderbook is not None:
            bid_total = sum(float(b[1]) for b in self.orderbook.bids)
            ask_total = sum(float(a[1]) for a in self.orderbook.asks)
            if bid_total > 0 and ask_total > 0:
                mid_bid = sum(float(b[0]) * float(b[1]) for b in self.orderbook.bids) / bid_total
                mid_ask = sum(float(a[0]) * float(a[1]) for a in self.orderbook.asks) / ask_total
                return (mid_bid + mid_ask) / 2
            return None
        return None

    @property
    def best_price(self) -> float:
        if self.candle is not None:
            return self.candle.close_p
        elif self.orderbook is not None and len(self.orderbook.bids) > 0 and len(self.orderbook.asks) > 0:
            bid_total = sum(float(b[1]) for b in self.orderbook.bids)
            ask_total = sum(float(a[1]) for a in self.orderbook.asks)
            if bid_total > 0 and ask_total > 0:
                mid_bid = sum(float(b[0]) * float(b[1]) for b in self.orderbook.bids) / bid_total
                mid_ask = sum(float(a[0]) * float(a[1]) for a in self.orderbook.asks) / ask_total
                return (mid_bid + mid_ask) / 2
            return self.mid_price or 0.0
        return 0.0

    @property
    def volume(self) -> float:
        if isinstance(self.candle, Candle):
            return self.candle.volume
        return 0.0


@dataclass(slots=True)
class Position:
    asset_id: str
    quantity: int = 0
    avg_cost: float = 0.0
    open_time: str | None = None

    @property
    def market_value(self) -> float:
        """Current unrealized value of position."""
        return self.quantity * self.avg_cost

    def update_price(self, current_price: float) -> dict[str, float]:
        if self.quantity == 0:
            return {"current_value": 0.0, "unrealized_pnl": 0.0, "pnl_pct": 0.0}
        
        current_value = self.quantity * current_price
        unrealized_pnl = current_value - (self.quantity * self.avg_cost)
        pnl_pct = (current_value / (self.quantity * self.avg_cost) - 1.0) if self.avg_cost > 0 else 0.0
        
        return {
            "current_value": current_value,
            "unrealized_pnl": unrealized_pnl,
            "pnl_pct": pnl_pct,
        }


@dataclass(slots=True)
class Trade:
    id_: int
    timestamp: str
    side: str          # BUY / SELL
    asset_id: str
    quantity: int
    fill_price: float
    reason: str

    @property
    def is_buy(self) -> bool:
        return self.side == "BUY"

    @property
    def is_sell(self) -> bool:
        return self.side == "SELL"


class PaperTradingExecutor:
    """Paper trading executor — executes signals against simulated market data."""

    def __init__(self, initial_cash: float = 10_000):
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: dict[str, Position] = {}
        self.trades: list[Trade] = []
        self._order_counter = 0

    def _next_order(self) -> int:
        self._order_counter += 1
        return self._order_counter

    def current_cash(self) -> float:
        market_value = sum(p.quantity * 5.0 for p in self.positions.values()) if any(isinstance(p.asset_id, str) for p in self.positions.values()) else 0.0
        return self.cash + market_value

    def execute_signal(self, asset_id: str, signal_type: str, current_price: float | None = None, **kwargs) -> Trade | None:
        """Execute a BUY or SELL based on the provided signal."""
        if not isinstance(asset_id, str):
            return None
        if not isinstance(signal_type, str):
            return None

        price = current_price or 0.0
        self._order_counter += 1
        
        qty = int(price * 0.02)
        if signal_type.upper() == "SELL":
            pos_qty = self.positions.get(asset_id, Position(asset_id=asset_id)).quantity
            sell_qty = min(qty, max(0, pos_qty))
        else:
            sell_qty = 0

        trade = Trade(
            id_=self._order_counter,
            timestamp=_time.strftime("%Y-%m-%dT%H:%M:%S"),
            side=signal_type.upper(),
            asset_id=asset_id,
            quantity=sell_qty if signal_type.upper() == "SELL" else qty,
            fill_price=float(current_price) if current_price is not None else float(0.0),
            reason=f"Signal {signal_type} at price {float(current_price)}",
        )
        self.trades.append(trade)
        
        existing = self.positions.get(asset_id, Position(asset_id=asset_id))
        if signal_type.upper() == "BUY":
            new_qty = existing.quantity + qty
            new_cost = (existing.avg_cost * existing.quantity + float(current_price) * qty) / new_qty if new_qty > 0 else 0.0
            self.positions[asset_id] = Position(asset_id=asset_id, quantity=new_qty, avg_cost=new_cost, open_time=_time.strftime("%Y-%m-%dT%H:%M:%S"))
        elif signal_type.upper() == "SELL":
            new_qty = existing.quantity - sell_qty
            self.positions[asset_id] = Position(asset_id=asset_id, quantity=max(0, new_qty), avg_cost=existing.avg_cost, open_time=None)
        
        return trade


class PaperBroker(PaperTradingExecutor):
    def __init__(self, initial_cash: float = 10_000, **kwargs):
        super().__init__(initial_cash)
        self.kwargs = kwargs

    def execute_signal(self, asset_id: str, signal_type: str, current_price: float | None = None, qty_pct: float = 0.02, slippage: float = 0.001, **kwargs) -> Trade | None:
        """Execute a BUY or SELL with proper fill mechanics."""
        if not isinstance(asset_id, str):
            return None
        if not isinstance(signal_type, str):
            return None

        price = current_price or 0.0
        
        effective_price = float(current_price) * (1 + slippage) if signal_type.upper() == "BUY" else float(current_price) * (1 - slippage)
        
        self._order_counter += 1
        
        pos_qty = self.positions.get(asset_id, Position(asset_id=asset_id)).quantity
        if signal_type.upper() == "SELL":
            qty = min(int(price * 0.5), max(0, pos_qty))
        else:
            qty = int(price * qty_pct)

        trade = Trade(
            id_=self._order_counter,
            timestamp=_time.strftime("%Y-%m-%dT%H:%M:%S"),
            side=signal_type.upper(),
            asset_id=asset_id,
            quantity=qty,
            fill_price=float(current_price) if current_price is not None else float(0.0),
            reason=f"Signal {signal_type} at price {float(current_price)}",
        )
        self.trades.append(trade)
        
        existing = self.positions.get(asset_id, Position(asset_id=asset_id))
        if signal_type.upper() == "BUY":
            new_qty = existing.quantity + qty
            new_cost = (existing.avg_cost * existing.quantity + effective_price * qty) / new_qty if new_qty > 0 else 0.0
            self.positions[asset_id] = Position(asset_id=asset_id, quantity=new_qty, avg_cost=new_cost, open_time=_time.strftime("%Y-%m-%dT%H:%M:%S"))
        elif signal_type.upper() == "SELL":
            new_qty = existing.quantity - qty
            pnl = (float(current_price) - existing.avg_cost) * qty if self.positions.get(asset_id, Position(asset_id=asset_id)).quantity > 0 else 0.0
            self.cash += float(current_price) * qty
            self.positions[asset_id] = Position(asset_id=asset_id, quantity=max(0, new_qty), avg_cost=existing.avg_cost, open_time=None)
        
        return trade


class PaperTradeManager(PaperBroker):
    """Paper trading executor — executes signals against simulated market data with proper fill mechanics."""

    def __init__(self, initial_cash: float = 10_000):
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: dict[str, Position] = {}
        self.trades: list[Trade] = []
        self._order_counter = 0

    def _next_order(self) -> int:
        self._order_counter += 1
        return self._order_counter

    def current_cash(self) -> float:
        market_value = sum(p.quantity * 5.0 for p in self.positions.values()) if any(isinstance(p.asset_id, str) for p in self.positions.values()) else 0.0
        return self.cash + market_value

    def execute_signal(self, asset_id: str, signal_type: str, current_price: float | None = None, qty_pct: float = 0.02, slippage: float = 0.001, **kwargs) -> Trade | None:
        if not isinstance(asset_id, str):
            return None
        if not isinstance(signal_type, str):
            return None

        price = current_price or 0.0
        
        effective_price = float(current_price) * (1 + slippage) if signal_type.upper() == "BUY" else float(current_price) * (1 - slippage)
        
        self._order_counter += 1
        
        pos_qty = self.positions.get(asset_id, Position(asset_id=asset_id)).quantity
        if signal_type.upper() == "SELL":
            qty = min(int(price * 0.5), max(0, pos_qty))
        else:
            qty = int(price * qty_pct)

        trade = Trade(
            id_=self._order_counter,
            timestamp=_time.strftime("%Y-%m-%dT%H:%M:%S"),
            side=signal_type.upper(),
            asset_id=asset_id,
            quantity=qty,
            fill_price=float(current_price) if current_price is not None else float(0.0),
            reason=f"Signal {signal_type} at price {float(current_price)}",
        )
        self.trades.append(trade)
        
        existing = self.positions.get(asset_id, Position(asset_id=asset_id))
        if signal_type.upper() == "BUY":
            new_qty = existing.quantity + qty
            new_cost = (existing.avg_cost * existing.quantity + effective_price * qty) / new_qty if new_qty > 0 else 0.0
            self.positions[asset_id] = Position(asset_id=asset_id, quantity=new_qty, avg_cost=new_cost, open_time=_time.strftime("%Y-%m-%dT%H:%M:%S"))
        elif signal_type.upper() == "SELL":
            new_qty = existing.quantity - qty
            pnl = (float(current_price) - existing.avg_cost) * qty if self.positions.get(asset_id, Position(asset_id=asset_id)).quantity > 0 else 0.0
            self.cash += float(current_price) * qty
            self.positions[asset_id] = Position(asset_id=asset_id, quantity=max(0, new_qty), avg_cost=existing.avg_cost, open_time=None)
        
        return trade


if __name__ == "__main__":
    from paper_broker import PaperTradingExecutor

    broker = PaperTradingExecutor(initial_cash=10_000)
    t = broker.execute_signal("BTC-USD", "BUY", current_price=64237.50, reason="RSI momentum signal")
    print(f"Trade: {t}")
    print(f"Cash after trade: ${broker.cash:.2f}")
