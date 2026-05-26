from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

log = logging.getLogger(__name__)


class TradeState(Enum):
    SIGNAL_RECEIVED = "SIGNAL_RECEIVED"
    RISK_CHECKED = "RISK_CHECKED"
    ROUTED = "ROUTED"
    ORDER_PLACED = "ORDER_PLACED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    SETTLED = "SETTLED"
    FAILED = "FAILED"


@dataclass
class TradeEvent:
    timestamp: datetime
    state: TradeState
    detail: str = ""


@dataclass
class TradeRecord:
    trade_id: str
    signal_id: str
    strategy_id: str
    product_id: str
    side: str
    size: float
    price: float
    state: TradeState = TradeState.SIGNAL_RECEIVED
    events: list[TradeEvent] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    fill_price: float | None = None
    filled_size: float = 0.0
    pnl: float | None = None

    def transition(self, new_state: TradeState, detail: str = "") -> None:
        self.state = new_state
        self.updated_at = datetime.now(timezone.utc)
        self.events.append(TradeEvent(timestamp=self.updated_at, state=new_state, detail=detail))
        log.info("trade_transition id=%s state=%s detail=%s", self.trade_id, new_state.value, detail)


@dataclass
class TradeLifecycleManager:
    trades: dict[str, TradeRecord] = field(default_factory=dict)

    def start_trade(self, record: TradeRecord) -> TradeRecord:
        self.trades[record.trade_id] = record
        record.transition(TradeState.SIGNAL_RECEIVED, "signal received from strategy")
        return record

    def get_trade(self, trade_id: str) -> TradeRecord | None:
        return self.trades.get(trade_id)

    def mark_risk_checked(self, trade_id: str, passed: bool) -> None:
        trade = self.trades.get(trade_id)
        if trade:
            trade.transition(TradeState.RISK_CHECKED, "risk check " + ("passed" if passed else "failed"))
            if not passed:
                trade.transition(TradeState.FAILED, "risk check failed")

    def mark_placed(self, trade_id: str, exchange_order_id: str) -> None:
        trade = self.trades.get(trade_id)
        if trade:
            trade.transition(TradeState.ORDER_PLACED, f"placed as {exchange_order_id}")

    def mark_filled(self, trade_id: str, fill_price: float, filled_size: float) -> None:
        trade = self.trades.get(trade_id)
        if trade:
            trade.fill_price = fill_price
            trade.filled_size = filled_size
            trade.transition(TradeState.FILLED, f"filled at {fill_price} size={filled_size}")

    def mark_settled(self, trade_id: str, pnl: float) -> None:
        trade = self.trades.get(trade_id)
        if trade:
            trade.pnl = pnl
            trade.transition(TradeState.SETTLED, f"settled pnl={pnl}")

    def mark_failed(self, trade_id: str, reason: str) -> None:
        trade = self.trades.get(trade_id)
        if trade:
            trade.transition(TradeState.FAILED, reason)

    def active_trades(self) -> list[TradeRecord]:
        return [t for t in self.trades.values() if t.state not in (TradeState.SETTLED, TradeState.FAILED, TradeState.CANCELLED)]
