from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from typing import Any

class MetricsCollector:
    def __init__(self) -> None:
        # Use a dedicated registry per collector so multiple instances (or
        # re-instantiation under test) don't collide on the global registry.
        self._registry = CollectorRegistry()
        self.request_count = Counter('app_requests_total', 'Total number of requests', registry=self._registry)
        self.error_count = Counter('app_errors_total', 'Total number of errors', registry=self._registry)
        self.request_duration = Histogram('app_request_duration_seconds', 'Request duration in seconds', registry=self._registry)
        self._counters: dict[str, Counter] = {}
        self._gauges: dict[str, Gauge] = {}

    def inc(self, name: str, labels: dict[str, str] | None = None) -> None:
        if name == 'requests':
            self.request_count.inc()
            return
        if name == 'errors':
            self.error_count.inc()
            return
        if labels:
            key = f"{name}{{{','.join(f'{k}={v}' for k, v in labels.items())}}}"
        else:
            key = name
        if key not in self._counters:
            self._counters[key] = Counter(name, name)
        self._counters[key].inc()

    def gauge(self, name: str, value: float) -> None:
        if name not in self._gauges:
            self._gauges[name] = Gauge(name, name)
        self._gauges[name].set(value)

    def observe_request(self, duration_ms: float) -> None:
        self.request_count.inc()
        self.request_duration.observe(duration_ms / 1000.0)

    def snapshot(self) -> dict[str, Any]:
        return {
            'request_count': int(self.request_count._value.get()),
            'error_count': int(self.error_count._value.get()),
            'avg_duration_ms': 0.0,
        }

metrics = MetricsCollector()
