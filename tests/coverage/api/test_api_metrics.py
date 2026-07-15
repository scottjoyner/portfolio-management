from unittest.mock import MagicMock

from trading_system.apps.api import metrics


def test_inc_requests():
    m = metrics.MetricsCollector()
    m.inc("requests")
    m.inc("requests")  # second call hits the already-cached counter branch
    assert int(m.request_count._value.get()) == 2


def test_inc_errors():
    m = metrics.MetricsCollector()
    m.inc("errors")
    assert int(m.error_count._value.get()) == 1


def test_inc_with_labels():
    m = metrics.MetricsCollector()
    m.inc("custom", labels={"env": "test"})
    m.inc("custom", labels={"env": "test"})  # second call hits cached counter
    assert "custom{env=test}" in m._counters


def test_inc_without_labels_uses_bare_name():
    m = metrics.MetricsCollector()
    m.inc("another")
    assert "another" in m._counters


def test_gauge():
    m = metrics.MetricsCollector()
    m.gauge("queue_depth", 3.5)
    m.gauge("queue_depth", 7.5)  # second call hits cached gauge
    assert "queue_depth" in m._gauges
    assert m._gauges["queue_depth"]._value.get() == 7.5


def test_observe_request():
    m = metrics.MetricsCollector()
    m.observe_request(1000.0)
    assert int(m.request_count._value.get()) == 1


def test_snapshot():
    m = metrics.MetricsCollector()
    m.inc("requests")
    m.inc("errors")
    snap = m.snapshot()
    assert snap["request_count"] == 1
    assert snap["error_count"] == 1
    assert snap["avg_duration_ms"] == 0.0


def test_module_instance():
    assert isinstance(metrics.metrics, metrics.MetricsCollector)
