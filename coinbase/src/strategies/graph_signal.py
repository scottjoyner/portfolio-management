from __future__ import annotations

from typing import List, Optional

from ..graph.neo4j_graph import CryptoGraphStore
from ..protocols import Bar, BaseStrategy, BracketSetup, Direction, InstrumentType


class GraphSignalStrategy(BaseStrategy):
    """Graph-aware strategy adapter.

    This emits a research-informed BracketSetup when a Coinbase product has enough
    graph evidence in Neo4j. It is intentionally signal-only: sizing, ranking,
    risk checks, and mode controls remain in the Coinbase orchestrator.
    """

    def __init__(
        self,
        min_graph_score: float = 0.45,
        take_profit_pct: float = 0.035,
        stop_loss_pct: float = 0.02,
        confidence_floor: float = 0.30,
        store: CryptoGraphStore | None = None,
    ):
        self.min_graph_score = min_graph_score
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.confidence_floor = confidence_floor
        self.store = store
        self.product_id = ""
        self._name = "crypto_graph_signal"

    def name(self) -> str:
        return self._name

    def set_product_id(self, product_id: str):
        self.product_id = product_id

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        if not self.product_id:
            return None
        store = self.store
        close_store = False
        if store is None:
            try:
                store = CryptoGraphStore()
                close_store = True
            except Exception:
                return None
        try:
            signal = store.asset_signal(self.product_id)
        finally:
            if close_store and store is not None:
                store.close()
        if not signal.available_on_coinbase or signal.graph_score < self.min_graph_score:
            return None
        entry = bar.close
        stop = entry * (1.0 - self.stop_loss_pct)
        target = entry * (1.0 + self.take_profit_pct)
        risk = abs(entry - stop)
        if risk <= 0:
            return None
        confidence = min(0.90, max(self.confidence_floor, signal.graph_score))
        return BracketSetup(
            direction=Direction.LONG,
            entry_price=entry,
            stop_price=stop,
            target_price=target,
            risk_reward=abs(target - entry) / risk,
            confidence=confidence,
            reason=";".join(signal.reasons),
            strategy_name=self._name,
            atr=_estimate_atr(history + [bar]),
            instrument_type=InstrumentType.SPOT,
            leverage=1.0,
            metadata=signal.to_dict(),
        )


def _estimate_atr(bars: List[Bar], period: int = 14) -> float:
    if len(bars) < 2:
        return 0.0
    recent = bars[-period:]
    tr_values: list[float] = []
    for idx, current in enumerate(recent):
        prev_close = recent[idx - 1].close if idx > 0 else current.close
        tr_values.append(max(current.high - current.low, abs(current.high - prev_close), abs(current.low - prev_close)))
    return sum(tr_values) / max(len(tr_values), 1)
