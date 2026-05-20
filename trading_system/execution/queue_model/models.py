from __future__ import annotations

from dataclasses import dataclass


@dataclass
class QueueEstimate:
    fill_probability: float
    expected_queue_time_ms: float
    expected_fill_size: float
    adverse_selection_bps: float
    stale_quote_decay: float


class SimpleQueueModel:
    def estimate(
        self,
        queue_ahead: float,
        trade_rate: float,
        cancel_rate: float,
        own_size: float = 1.0,
        volatility_bps: float = 5.0,
        quote_age_ms: float = 0.0,
    ) -> QueueEstimate:
        pressure = max(trade_rate + cancel_rate, 1e-6)
        t_ms = 1_000.0 * queue_ahead / pressure
        p_fill = max(0.0, min(1.0, pressure / (pressure + queue_ahead + 1e-9)))
        expected_fill = own_size * p_fill
        adverse_selection_bps = min(50.0, volatility_bps * (1.0 - p_fill / 2.0))
        stale_decay = min(1.0, quote_age_ms / max(5.0 * t_ms, 1.0))
        return QueueEstimate(
            fill_probability=p_fill,
            expected_queue_time_ms=t_ms,
            expected_fill_size=expected_fill,
            adverse_selection_bps=adverse_selection_bps,
            stale_quote_decay=stale_decay,
        )
