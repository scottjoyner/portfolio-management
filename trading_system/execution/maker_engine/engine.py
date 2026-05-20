from __future__ import annotations

from dataclasses import dataclass

from execution.queue_model.models import QueueEstimate, SimpleQueueModel
from market_data.microstructure.features import MicrostructureFeatureBuilder, TopOfBook


@dataclass(slots=True)
class MakerConfig:
    levels: int = 4
    base_order_size: float = 0.01
    min_spread_bps: float = 2.0
    max_spread_bps: float = 30.0
    inventory_target: float = 0.0
    inventory_skew_per_unit_bps: float = 3.0
    toxic_flow_threshold: float = 0.65
    fade_after_ms: float = 400.0
    max_cancel_replace_per_sec: int = 25


@dataclass(slots=True)
class QuoteLevel:
    side: str
    price: float
    size: float
    level: int


@dataclass(slots=True)
class MakerState:
    inventory: float = 0.0
    cancel_replace_count: int = 0
    quote_age_ms: float = 0.0


class MakerQuoteEngine:
    def __init__(self, cfg: MakerConfig, queue_model: SimpleQueueModel | None = None) -> None:
        self.cfg = cfg
        self.queue_model = queue_model or SimpleQueueModel()

    def _dynamic_spread_bps(self, volatility_bps: float, toxic_flow: float) -> float:
        spread = self.cfg.min_spread_bps + 0.5 * volatility_bps + 10.0 * toxic_flow
        return min(max(spread, self.cfg.min_spread_bps), self.cfg.max_spread_bps)

    def _inventory_skew_bps(self, inventory: float) -> float:
        inv_error = inventory - self.cfg.inventory_target
        return inv_error * self.cfg.inventory_skew_per_unit_bps

    def build_ladder(
        self,
        book: TopOfBook,
        state: MakerState,
        volatility_bps: float,
        toxic_flow: float,
        queue_ahead: float,
        trade_rate: float,
        cancel_rate: float,
    ) -> tuple[list[QuoteLevel], QueueEstimate]:
        mid = (book.bid_px + book.ask_px) / 2.0
        spread_bps = self._dynamic_spread_bps(volatility_bps, toxic_flow)
        skew_bps = self._inventory_skew_bps(state.inventory)
        step_bps = max(0.5, spread_bps / max(self.cfg.levels, 1))

        queue = self.queue_model.estimate(
            queue_ahead=queue_ahead,
            trade_rate=trade_rate,
            cancel_rate=cancel_rate,
            own_size=self.cfg.base_order_size,
            volatility_bps=volatility_bps,
            quote_age_ms=state.quote_age_ms,
        )
        fill_penalty = 1.0 - min(queue.adverse_selection_bps / 100.0, 0.5)
        size = max(0.0001, self.cfg.base_order_size * queue.fill_probability * fill_penalty)

        quotes: list[QuoteLevel] = []
        for level in range(1, self.cfg.levels + 1):
            lv_bps = step_bps * level
            bid_px = mid * (1.0 - (spread_bps + lv_bps + skew_bps) / 10_000.0)
            ask_px = mid * (1.0 + (spread_bps + lv_bps - skew_bps) / 10_000.0)
            quotes.append(QuoteLevel(side="BUY", price=bid_px, size=size, level=level))
            quotes.append(QuoteLevel(side="SELL", price=ask_px, size=size, level=level))
        return quotes, queue

    def should_fade_quotes(self, state: MakerState, toxic_flow: float, microprice_drift_bps: float) -> bool:
        return (
            state.quote_age_ms >= self.cfg.fade_after_ms
            or toxic_flow >= self.cfg.toxic_flow_threshold
            or abs(microprice_drift_bps) >= self.cfg.min_spread_bps * 2
        )

    def cancel_replace_pressure(self, state: MakerState) -> float:
        return min(1.0, state.cancel_replace_count / max(self.cfg.max_cancel_replace_per_sec, 1))

    def inventory_drift(self, state: MakerState, book: TopOfBook) -> float:
        mpx = MicrostructureFeatureBuilder.microprice(book)
        if mpx <= 0:
            return 0.0
        return ((state.inventory - self.cfg.inventory_target) * mpx)
