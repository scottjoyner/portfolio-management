from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Tearsheet:
    title: str
    metrics: dict[str, float] = field(default_factory=dict)
    charts: dict[str, list[Any]] = field(default_factory=dict)

    def add_metric(self, name: str, value: float) -> None:
        self.metrics[name] = value

    def add_chart(self, name: str, data: list[Any]) -> None:
        self.charts[name] = data
