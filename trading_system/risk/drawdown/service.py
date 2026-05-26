from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class DrawdownTracker:
    peak_value: float = 0.0
    current_value: float = 0.0
    drawdown_pct: float = 0.0
    halt_threshold: float = 0.2
    halted: bool = False
    last_updated: datetime | None = None

    def update(self, current_value: float) -> None:
        self.current_value = current_value
        if current_value > self.peak_value:
            self.peak_value = current_value
        if self.peak_value > 0:
            self.drawdown_pct = (self.peak_value - current_value) / self.peak_value
        self.halted = self.drawdown_pct >= self.halt_threshold
        self.last_updated = datetime.now(timezone.utc)

    def reset(self) -> None:
        self.peak_value = 0.0
        self.current_value = 0.0
        self.drawdown_pct = 0.0
        self.halted = False
        self.last_updated = None


@dataclass
class DrawdownMonitor:
    trackers: dict[str, DrawdownTracker] = field(default_factory=dict)

    def get_tracker(self, portfolio_id: str) -> DrawdownTracker:
        return self.trackers.setdefault(portfolio_id, DrawdownTracker())

    def set_halt_threshold(self, portfolio_id: str, threshold: float) -> None:
        self.get_tracker(portfolio_id).halt_threshold = threshold

    def update(self, portfolio_id: str, current_value: float) -> None:
        self.get_tracker(portfolio_id).update(current_value)

    def is_halted(self, portfolio_id: str) -> bool:
        return self.get_tracker(portfolio_id).halted

    def reset_portfolio(self, portfolio_id: str) -> None:
        self.trackers.pop(portfolio_id, None)
