from execution.queue_model.models import SimpleQueueModel


def test_queue_model_estimate_bounds():
    est = SimpleQueueModel().estimate(queue_ahead=4, trade_rate=2, cancel_rate=1)
    assert 0 <= est.fill_probability <= 1
    assert est.expected_queue_time_ms > 0
