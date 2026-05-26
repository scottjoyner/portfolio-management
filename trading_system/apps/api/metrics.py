from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MetricsCollector:
    request_count: int = 0
    error_count: int = 0
    request_duration_ms: list[float] = field(default_factory=list)
    _counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _gauges: dict[str, float] = field(default_factory=dict)

    def inc(self, name: str, labels: dict[str, str] | None = None) -> None:
        if labels:
            key = f"{name}{{{','.join(f'{k}={v}' for k, v in labels.items())}}}"
        else:
            key = name
        self._counts[key] += 1

    def gauge(self, name: str, value: float) -> None:
        self._gauges[name] = value

    def observe_request(self, duration_ms: float) -> None:
        self.request_count += 1
        self.request_duration_ms.append(duration_ms)

    def snapshot(self) -> dict[str, Any]:
        avg_ms = sum(self.request_duration_ms) / len(self.request_duration_ms) if self.request_duration_ms else 0.0
        return {
            "request_count": self.request_count,
            "error_count": self.error_count,
            "avg_duration_ms": round(avg_ms, 2),
            "counts": dict(self._counts),
            "gauges": dict(self._gauges),
        }


metrics = MetricsCollector()
