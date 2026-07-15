import pytest
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from trading_system.apps.api import metrics


@pytest.fixture(autouse=True)
def _isolated_registry(monkeypatch):
    # prometheus_client registers metrics globally; give each created metric its
    # own registry so repeated instantiation in tests does not raise
    # "Duplicated timeseries".
    def mk(cls):
        return lambda name, documentation: cls(name, documentation, registry=CollectorRegistry())

    monkeypatch.setattr(metrics, "Counter", mk(Counter))
    monkeypatch.setattr(metrics, "Gauge", mk(Gauge))
    monkeypatch.setattr(metrics, "Histogram", mk(Histogram))


def test_inc_requests_and_errors():
    mc = metrics.MetricsCollector()
    mc.inc("requests")
    mc.inc("errors")
    assert mc.snapshot()["request_count"] == 1
    assert mc.snapshot()["error_count"] == 1


def test_inc_custom_with_and_without_labels():
    mc = metrics.MetricsCollector()
    mc.inc("orders_total", labels={"env": "test"})
    mc.inc("orders_total")
    assert mc.snapshot()["request_count"] == 0


def test_gauge_and_observe():
    mc = metrics.MetricsCollector()
    mc.gauge("queue_depth", 12.5)
    mc.observe_request(50.0)
    snap = mc.snapshot()
    assert snap["request_count"] == 1
    assert snap["avg_duration_ms"] == 0.0


def test_inc_repeat_same_key():
    mc = metrics.MetricsCollector()
    mc.inc("orders_total", labels={"env": "test"})
    mc.inc("orders_total", labels={"env": "test"})
    mc.gauge("queue_depth", 1.0)
    mc.gauge("queue_depth", 2.0)
    assert mc.snapshot()["request_count"] == 0


def test_module_instance():
    assert isinstance(metrics.metrics, metrics.MetricsCollector)
